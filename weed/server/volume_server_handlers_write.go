package weed_server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/operation"
	"github.com/journeymidnight/seaweedfs/weed/stats"
	"github.com/journeymidnight/seaweedfs/weed/storage/needle"
	"github.com/journeymidnight/seaweedfs/weed/topology"
)

func (vs *VolumeServer) PostHandler(w http.ResponseWriter, r *http.Request) {

	stats.VolumeServerRequestCounter.WithLabelValues("post").Inc()
	start := time.Now()
	defer func() {
		stats.VolumeServerRequestHistogram.WithLabelValues("post").Observe(time.Since(start).Seconds())
	}()

	if e := r.ParseForm(); e != nil {
		glog.V(0).Infoln("form parse error:", e)
		writeJsonError(w, r, http.StatusBadRequest, e)
		return
	}

	vid, fid, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, ve := needle.NewVolumeId(vid)
	if ve != nil {
		glog.V(0).Infoln("NewVolumeId error:", ve)
		writeJsonError(w, r, http.StatusBadRequest, ve)
		return
	}

	if !vs.maybeCheckJwtAuthorization(r, vid, fid, true) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	needle, originalSize, ne := needle.CreateNeedleFromRequest(r, vs.FixJpgOrientation)
	if ne != nil {
		writeJsonError(w, r, http.StatusBadRequest, ne)
		return
	}

	ret := operation.UploadResult{}
	writeError := topology.ReplicatedWrite(
		vs.GetMaster(), vs.store, volumeId, needle, r)
	httpStatus := http.StatusCreated
	if writeError != nil {
		httpStatus = http.StatusInternalServerError
		ret.Error = writeError.Error()
	}
	if needle.HasName() {
		ret.Name = string(needle.Name)
	}
	ret.Size = uint32(originalSize)
	ret.ETag = needle.Etag()
	setEtag(w, ret.ETag)
	writeJsonQuiet(w, r, httpStatus, ret)
}

func (vs *VolumeServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {

	stats.VolumeServerRequestCounter.WithLabelValues("delete").Inc()
	start := time.Now()
	defer func() {
		stats.VolumeServerRequestHistogram.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	}()

	n := new(needle.Needle)
	vid, fid, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, _ := needle.NewVolumeId(vid)
	n.ParsePath(fid)

	if !vs.maybeCheckJwtAuthorization(r, vid, fid, true) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	// glog.V(2).Infof("volume %s deleting %s", vid, n)

	cookie := n.Cookie

	ecVolume, hasEcVolume := vs.store.FindEcVolume(volumeId)

	if hasEcVolume {
		count, err := vs.store.DeleteEcShardNeedle(context.Background(), ecVolume, n, cookie)
		writeDeleteResult(err, count, w, r)
		return
	}

	n.LastModified = uint64(time.Now().Unix())
	if len(r.FormValue("ts")) > 0 {
		modifiedTime, err := strconv.ParseInt(r.FormValue("ts"), 10, 64)
		if err == nil {
			n.LastModified = uint64(modifiedTime)
		}
	}

	releaseSize, err := topology.ReplicatedDelete(vs.GetMaster(), vs.store, volumeId, n, r)

	writeDeleteResult(err, int64(releaseSize), w, r)
}

func writeDeleteResult(err error, count int64, w http.ResponseWriter, r *http.Request) {
	if err == nil {
		m := make(map[string]int64)
		m["size"] = count
		writeJsonQuiet(w, r, http.StatusAccepted, m)
	} else {
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Deletion Failed: %v", err))
	}
}

func setEtag(w http.ResponseWriter, etag string) {
	if etag != "" {
		if strings.HasPrefix(etag, "\"") {
			w.Header().Set("ETag", etag)
		} else {
			w.Header().Set("ETag", "\""+etag+"\"")
		}
	}
}
