package datastore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	"github.com/minio/minio-go/v6"
	"github.com/sirupsen/logrus"
	config2 "github.com/turt2live/matrix-media-repo/common/config"
	"github.com/turt2live/matrix-media-repo/common/rcontext"
	"github.com/turt2live/matrix-media-repo/storage/datastore/ds_file"
	"github.com/turt2live/matrix-media-repo/storage/datastore/ds_ipfs"
	"github.com/turt2live/matrix-media-repo/storage/datastore/ds_s3"
	"github.com/turt2live/matrix-media-repo/types"
	"github.com/turt2live/matrix-media-repo/util"
)

var ErrS3Required = errors.New("download URLs unsupported for non-s3 datastores")

type DatastoreRef struct {
	// TODO: Don't blindly copy properties from types.Datastore
	DatastoreId string
	Type        string
	Uri         string

	datastore *types.Datastore
	config    config2.DatastoreConfig
}

func newDatastoreRef(ds *types.Datastore, config config2.DatastoreConfig) *DatastoreRef {
	return &DatastoreRef{
		DatastoreId: ds.DatastoreId,
		Type:        ds.Type,
		Uri:         ds.Uri,
		datastore:   ds,
		config:      config,
	}
}

func (d *DatastoreRef) GetUploadURL(ctx rcontext.RequestContext) (string, string, error) {
	if d.Type != "s3" {
		logrus.Error("attempting to get an upload URL but datasource is of type ", d.Type)
		return "", "", ErrS3Required
	}

	s3, err := ds_s3.GetOrCreateS3Datastore(d.DatastoreId, d.config)
	if err != nil {
		return "", "", err
	}

	uploadURL, objectName, err := s3.GetUploadURL(ctx)
	if err != nil {
		return "", "", err
	}

	return uploadURL, objectName, nil
}

func (d *DatastoreRef) UploadFile(file io.ReadCloser, expectedLength int64, ctx rcontext.RequestContext) (*types.ObjectInfo, error) {
	ctx = ctx.LogWithFields(logrus.Fields{"datastoreId": d.DatastoreId, "datastoreUri": d.Uri})

	if d.Type == "file" {
		return ds_file.PersistFile(d.Uri, file, ctx)
	} else if d.Type == "s3" {
		s3, err := ds_s3.GetOrCreateS3Datastore(d.DatastoreId, d.config)
		if err != nil {
			return nil, err
		}
		return s3.UploadFile(file, expectedLength, ctx)
	} else if d.Type == "ipfs" {
		return ds_ipfs.UploadFile(file, ctx)
	} else {
		return nil, errors.New("unknown datastore type")
	}
}

func (d *DatastoreRef) DeleteObject(location string) error {
	if d.Type == "file" {
		return ds_file.DeletePersistedFile(d.Uri, location)
	} else if d.Type == "s3" {
		s3, err := ds_s3.GetOrCreateS3Datastore(d.DatastoreId, d.config)
		if err != nil {
			return err
		}
		return s3.DeleteObject(location)
	} else if d.Type == "ipfs" {
		// TODO: Support deleting from IPFS - will need a "delete reason" to avoid deleting duplicates
		logrus.Warn("Unsupported operation: deleting from IPFS datastore")
		return nil
	} else {
		return errors.New("unknown datastore type")
	}
}

func (d *DatastoreRef) DownloadFile(location string) (io.ReadCloser, error) {
	if d.Type == "file" {
		return os.Open(path.Join(d.Uri, location))
	} else if d.Type == "s3" {
		s3, err := ds_s3.GetOrCreateS3Datastore(d.DatastoreId, d.config)
		if err != nil {
			return nil, err
		}
		return s3.DownloadObject(location)
	} else if d.Type == "ipfs" {
		return ds_ipfs.DownloadFile(location)
	} else {
		return nil, errors.New("unknown datastore type")
	}
}

func (d *DatastoreRef) GetDownloadURL(ctx rcontext.RequestContext, location string, filename string) (string, error) {
	if d.Type != "s3" {
		logrus.Error("attempting to get an download URL but datasource is of type ", d.Type)
		return "", ErrS3Required
	}

	s3, err := ds_s3.GetOrCreateS3Datastore(d.DatastoreId, d.config)
	if err != nil {
		return "", err
	}

	publicPrefix, ok := d.config.Options["publicPrefix"]
	if ok {
		return fmt.Sprintf("%s/%s", publicPrefix, filename), nil
	} else {
		return s3.GetDownloadURL(ctx, location, filename)
	}
}

func (d *DatastoreRef) ObjectExists(location string) bool {
	if d.Type == "file" {
		ok, err := util.FileExists(path.Join(d.Uri, location))
		if err != nil {
			return false
		}
		return ok
	} else if d.Type == "s3" {
		s3, err := ds_s3.GetOrCreateS3Datastore(d.DatastoreId, d.config)
		if err != nil {
			return false
		}
		return s3.ObjectExists(location)
	} else if d.Type == "ipfs" {
		// TODO: Support checking file existence in IPFS
		logrus.Warn("Unsupported operation: existence in IPFS datastore")
		return false
	} else {
		panic("unknown datastore type")
	}
}

func (d *DatastoreRef) ObjectInfo(ctx context.Context, location string) (minio.ObjectInfo, error) {
	if d.Type != "s3" {
		return minio.ObjectInfo{}, ErrS3Required
	}

	s3, err := ds_s3.GetOrCreateS3Datastore(d.DatastoreId, d.config)
	if err != nil {
		return minio.ObjectInfo{}, err
	}
	return s3.GetObjectInfo(ctx, location)
}

func (d *DatastoreRef) OverwriteObject(location string, stream io.ReadCloser, ctx rcontext.RequestContext) error {
	if d.Type == "file" {
		_, _, err := ds_file.PersistFileAtLocation(path.Join(d.Uri, location), stream, ctx)
		return err
	} else if d.Type == "s3" {
		s3, err := ds_s3.GetOrCreateS3Datastore(d.DatastoreId, d.config)
		if err != nil {
			return err
		}
		return s3.OverwriteObject(location, stream)
	} else if d.Type == "ipfs" {
		// TODO: Support overwriting in IPFS
		logrus.Warn("Unsupported operation: overwriting file in IPFS datastore")
		return errors.New("unsupported operation")
	} else {
		return errors.New("unknown datastore type")
	}
}

func (d *DatastoreRef) ShouldRedirectDownload() bool {
	if d.Type != "s3" {
		return false
	}

	redirectDownloads, _ := strconv.ParseBool(d.config.Options["redirectDownloads"])
	return redirectDownloads
}

func (d *DatastoreRef) ShouldRedirectUpload() bool {
	if d.Type != "s3" {
		return false
	}

	redirectUploads, _ := strconv.ParseBool(d.config.Options["redirectUploads"])
	return redirectUploads
}
