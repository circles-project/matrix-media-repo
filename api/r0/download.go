package r0

import (
	"io"
	"net/http"
	"strconv"

	"github.com/getsentry/sentry-go"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/turt2live/matrix-media-repo/api"
	"github.com/turt2live/matrix-media-repo/common"
	"github.com/turt2live/matrix-media-repo/common/rcontext"
	"github.com/turt2live/matrix-media-repo/controllers/download_controller"
	"github.com/turt2live/matrix-media-repo/storage"
	"github.com/turt2live/matrix-media-repo/storage/datastore"
)

type DownloadMediaResponse struct {
	ContentType       string
	Filename          string
	SizeBytes         int64
	Data              io.ReadCloser
	TargetDisposition string
}

type Redirect struct {
	Status int
	URL    string
}

func DownloadMedia(r *http.Request, rctx rcontext.RequestContext, user api.UserInfo) interface{} {
	params := mux.Vars(r)

	server := params["server"]
	mediaId := params["mediaId"]
	filename := params["filename"]
	allowRemote := r.URL.Query().Get("allow_remote")
	allowRedirect, _ := strconv.ParseBool(r.URL.Query().Get("allow_redirect"))

	targetDisposition := r.URL.Query().Get("org.matrix.msc2702.asAttachment")
	if targetDisposition == "true" {
		targetDisposition = "attachment"
	} else if targetDisposition == "false" {
		targetDisposition = "inline"
	} else {
		targetDisposition = "infer"
	}

	downloadRemote := true
	if allowRemote != "" {
		parsedFlag, err := strconv.ParseBool(allowRemote)
		if err != nil {
			return api.InternalServerError("allow_remote flag does not appear to be a boolean")
		}
		downloadRemote = parsedFlag
	}

	var asyncWaitMs *int = nil
	if rctx.Config.Features.MSC2246Async.Enabled {
		// request default wait time if feature enabled
		var parsedInt int = -1
		maxStallMs := r.URL.Query().Get("fi.mau.msc2246.max_stall_ms")
		if maxStallMs != "" {
			var err error
			parsedInt, err = strconv.Atoi(maxStallMs)
			if err != nil {
				return api.InternalServerError("fi.mau.msc2246.max_stall_ms does not appear to be a number")
			}
		}
		asyncWaitMs = &parsedInt
	}

	rctx = rctx.LogWithFields(logrus.Fields{
		"mediaId":     mediaId,
		"server":      server,
		"filename":    filename,
		"allowRemote": downloadRemote,
	})

	db := storage.GetDatabase().GetMediaStore(rctx)
	dbMedia, err := db.Get(server, mediaId)
	if err != nil {
		return handleDownloadError(rctx, err)
	}

	if allowRedirect && datastore.ShouldRedirectDownload(rctx, dbMedia.DatastoreId) {
		media, err := download_controller.GetMediaURL(server, mediaId, filename, downloadRemote, false, asyncWaitMs, rctx)
		if err != nil {
			return handleDownloadError(rctx, err)
		}

		return &Redirect{
			Status: http.StatusTemporaryRedirect,
			URL:    media.URL,
		}
	} else {
		streamedMedia, err := download_controller.GetMedia(server, mediaId, downloadRemote, false, asyncWaitMs, rctx)
		if err != nil {
			return handleDownloadError(rctx, err)
		}

		if filename == "" {
			filename = streamedMedia.UploadName
		}

		return &DownloadMediaResponse{
			ContentType:       streamedMedia.ContentType,
			Filename:          filename,
			SizeBytes:         streamedMedia.SizeBytes,
			Data:              streamedMedia.Stream,
			TargetDisposition: targetDisposition,
		}
	}
}

func handleDownloadError(ctx rcontext.RequestContext, err error) interface{} {
	switch err {
	case common.ErrMediaNotFound, common.ErrMediaQuarantined:
		return api.NotFoundError()
	case common.ErrMediaTooLarge:
		return api.RequestTooLarge() // this does *not* seem like the right status code
	case common.ErrNotYetUploaded:
		return api.NotYetUploaded()
	default:
		ctx.Log.Warn("error looking up media for download: ", err)
		sentry.CaptureException(err)
		return api.InternalServerError("Unexpected Error")
	}
}
