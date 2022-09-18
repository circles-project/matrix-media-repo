package ds_s3

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v6"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/turt2live/matrix-media-repo/common/config"
	"github.com/turt2live/matrix-media-repo/common/rcontext"
	"github.com/turt2live/matrix-media-repo/metrics"
	"github.com/turt2live/matrix-media-repo/types"
	"github.com/turt2live/matrix-media-repo/util"
	"github.com/turt2live/matrix-media-repo/util/cleanup"
)

var stores = make(map[string]*s3Datastore)

type s3Datastore struct {
	conf         config.DatastoreConfig
	dsId         string
	client       *minio.Client
	bucket       string
	region       string
	tempPath     string
	storageClass string
	prefixLength int
}

func GetOrCreateS3Datastore(dsId string, conf config.DatastoreConfig) (*s3Datastore, error) {
	if s, ok := stores[dsId]; ok {
		return s, nil
	}

	endpoint, epFound := conf.Options["endpoint"]
	bucket, bucketFound := conf.Options["bucketName"]
	accessKeyId, keyFound := conf.Options["accessKeyId"]
	accessSecret, secretFound := conf.Options["accessSecret"]
	region, regionFound := conf.Options["region"]
	tempPath, tempPathFound := conf.Options["tempPath"]
	storageClass, storageClassFound := conf.Options["storageClass"]
	if !epFound || !bucketFound || !keyFound || !secretFound {
		return nil, errors.New("invalid configuration: missing s3 options")
	}
	if !tempPathFound {
		logrus.Warn("Datastore ", dsId, " (s3) does not have a tempPath set - this could lead to excessive memory usage by the media repo")
	}
	if !storageClassFound {
		storageClass = "STANDARD"
	}

	useSsl := true
	useSslStr, sslFound := conf.Options["ssl"]
	if sslFound && useSslStr != "" {
		useSsl, _ = strconv.ParseBool(useSslStr)
	}

	prefixLength := 0
	prefixLengthStr, prefixLengthFound := conf.Options["prefixLength"]
	if prefixLengthFound && prefixLengthStr != "" {
		prefixLength, _ = strconv.Atoi(prefixLengthStr)
	}

	var s3client *minio.Client
	var err error

	if regionFound {
		s3client, err = minio.NewWithRegion(endpoint, accessKeyId, accessSecret, useSsl, region)
	} else {
		s3client, err = minio.New(endpoint, accessKeyId, accessSecret, useSsl)
	}
	if err != nil {
		return nil, err
	}

	s3ds := &s3Datastore{
		conf:         conf,
		dsId:         dsId,
		client:       s3client,
		bucket:       bucket,
		region:       region,
		tempPath:     tempPath,
		storageClass: storageClass,
		prefixLength: prefixLength,
	}
	stores[dsId] = s3ds
	return s3ds, nil
}

func GetS3URL(datastoreId string, location string) (string, error) {
	var store *s3Datastore
	var ok bool
	if store, ok = stores[datastoreId]; !ok {
		return "", errors.New("s3 datastore not found")
	}

	// HACK: Surely there's a better way...
	return fmt.Sprintf("https://%s/%s/%s", store.conf.Options["endpoint"], store.bucket, location), nil
}

func ParseS3URL(s3url string) (string, string, string, error) {
	trimmed := s3url[8:] // trim off https
	parts := strings.Split(trimmed, "/")
	if len(parts) < 3 {
		return "", "", "", errors.New("invalid url")
	}

	endpoint := parts[0]
	location := parts[len(parts)-1]
	bucket := strings.Join(parts[1:len(parts)-1], "/")

	return endpoint, bucket, location, nil
}

func (s *s3Datastore) EnsureBucketExists() error {
	found, err := s.client.BucketExists(s.bucket)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("bucket not found")
	}
	return nil
}

func (s *s3Datastore) EnsureTempPathExists() error {
	err := os.MkdirAll(s.tempPath, os.ModePerm)
	if err != os.ErrExist && err != nil {
		return err
	}
	return nil
}

func (s s3Datastore) generateObjectKey() (string, error) {
	objectKey, err := util.GenerateRandomString(512)
	if err != nil {
		return "", err
	}

	return objectKey[:s.prefixLength] + "/" + objectKey[s.prefixLength:], nil
}

func (s *s3Datastore) GetUploadURL(ctx rcontext.RequestContext) (string, string, error) {
	objectName, err := s.generateObjectKey()
	if err != nil {
		return "", "", err
	}

	expiryTime := time.Duration(ctx.Config.Features.MSC2246Async.AsyncUploadExpirySecs) * time.Second

	u, err := s.client.PresignedPutObject(s.bucket, objectName, expiryTime)
	if err != nil {
		return "", "", err
	}

	return u.String(), objectName, nil
}

func (s *s3Datastore) UploadFile(file io.ReadCloser, expectedLength int64, ctx rcontext.RequestContext) (*types.ObjectInfo, error) {
	defer cleanup.DumpAndCloseStream(file)

	objectName, err := s.generateObjectKey()
	if err != nil {
		return nil, err
	}

	var rs3 io.ReadCloser
	var ws3 io.WriteCloser
	rs3, ws3 = io.Pipe()
	tr := io.TeeReader(file, ws3)

	done := make(chan bool)
	defer close(done)

	var hash string
	var sizeBytes int64
	var hashErr error
	var uploadErr error

	go func() {
		defer ws3.Close()
		ctx.Log.Info("Calculating hash of stream...")
		hash, hashErr = util.GetSha256HashOfStream(ioutil.NopCloser(tr))
		ctx.Log.Info("Hash of file is ", hash)
		done <- true
	}()

	go func() {
		if expectedLength <= 0 {
			if s.tempPath != "" {
				ctx.Log.Info("Buffering file to temp path due to unknown file size")
				var f *os.File
				f, uploadErr = ioutil.TempFile(s.tempPath, "mr*")
				if uploadErr != nil {
					io.Copy(ioutil.Discard, rs3)
					done <- true
					return
				}
				defer os.Remove(f.Name())
				expectedLength, uploadErr = io.Copy(f, rs3)
				cleanup.DumpAndCloseStream(f)
				f, uploadErr = os.Open(f.Name())
				if uploadErr != nil {
					done <- true
					return
				}
				rs3 = f
				defer cleanup.DumpAndCloseStream(f)
			} else {
				ctx.Log.Warn("Uploading content of unknown length to s3 - this could result in high memory usage")
				expectedLength = -1
			}
		}

		timer := prometheus.NewTimer(metrics.MediaUploadDuration.WithLabelValues(ctx.Log.Data["worker_previewer"].(string)))
		defer func() {
			t := timer.ObserveDuration()
			ctx.Log.WithFields(logrus.Fields{"duration": t.Round(time.Millisecond), "upload_size": expectedLength}).Info("s3 upload complete")
		}()

		ctx.Log.Info("Uploading file...")
		sizeBytes, uploadErr = s.client.PutObjectWithContext(ctx, s.bucket, objectName, rs3, expectedLength, minio.PutObjectOptions{StorageClass: s.storageClass})
		ctx.Log.Info("Uploaded ", sizeBytes, " bytes to s3")
		metrics.MediaUploadBytes.Add(float64(sizeBytes))
		done <- true
	}()

	for c := 0; c < 2; c++ {
		<-done
	}

	obj := &types.ObjectInfo{
		Location:   objectName,
		Sha256Hash: hash,
		SizeBytes:  sizeBytes,
	}

	if hashErr != nil {
		s.DeleteObject(obj.Location)
		return nil, hashErr
	}

	if uploadErr != nil {
		return nil, uploadErr
	}

	return obj, nil
}

func (s *s3Datastore) DeleteObject(location string) error {
	logrus.Info("Deleting object from bucket ", s.bucket, ": ", location)
	return s.client.RemoveObject(s.bucket, location)
}

func (s *s3Datastore) DownloadObject(location string) (io.ReadCloser, error) {
	logrus.Info("Downloading object from bucket ", s.bucket, ": ", location)
	return s.client.GetObject(s.bucket, location, minio.GetObjectOptions{})
}

func (s *s3Datastore) GetDownloadURL(ctx rcontext.RequestContext, location string, filename string) (string, error) {
	logrus.Info("getting pre-signed download URL for object from bucket ", s.bucket, ": ", location)

	reqParams := make(url.Values)
	reqParams.Set("response-content-disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))

	expiryTime := time.Duration(ctx.Config.Features.MSC2246Async.AsyncUploadExpirySecs) * time.Second

	u, err := s.client.PresignedGetObject(s.bucket, location, expiryTime, reqParams)
	if err != nil {
		return "", err
	}

	return u.String(), nil
}

func (s *s3Datastore) GetObjectInfo(ctx context.Context, location string) (minio.ObjectInfo, error) {
	return s.client.StatObjectWithContext(ctx, s.bucket, location, minio.StatObjectOptions{})
}

func (s *s3Datastore) ObjectExists(location string) bool {
	stat, err := s.client.StatObject(s.bucket, location, minio.StatObjectOptions{})
	if err != nil {
		return false
	}
	return stat.Size > 0
}

func (s *s3Datastore) OverwriteObject(location string, stream io.ReadCloser) error {
	defer cleanup.DumpAndCloseStream(stream)
	_, err := s.client.PutObject(s.bucket, location, stream, -1, minio.PutObjectOptions{StorageClass: s.storageClass})
	return err
}
