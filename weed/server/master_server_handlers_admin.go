package weed_server

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/topology"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (ms *MasterServer) collectionDeleteHandler(w http.ResponseWriter, r *http.Request) {
	collection, ok := ms.Topo.FindCollection(r.FormValue("collection"))
	if !ok {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("collection %s does not exist", r.FormValue("collection")))
		return
	}
	for _, server := range collection.ListVolumeServers() {
		err := operation.WithVolumeServerClient(server.Url(), ms.grpcDialOpiton, func(client volume_server_pb.VolumeServerClient) error {
			_, deleteErr := client.DeleteCollection(context.Background(), &volume_server_pb.DeleteCollectionRequest{
				Collection: collection.Name,
			})
			return deleteErr
		})
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError, err)
			return
		}
	}
	ms.Topo.DeleteCollection(r.FormValue("collection"))
}

func (ms *MasterServer) dirStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Topology"] = ms.Topo.ToMap()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (ms *MasterServer) volumeVacuumHandler(w http.ResponseWriter, r *http.Request) {
	gcString := r.FormValue("garbageThreshold")
	gcThreshold := ms.option.GarbageThreshold
	if gcString != "" {
		var err error
		gcThreshold, err = strconv.ParseFloat(gcString, 32)
		if err != nil {
			glog.V(0).Infof("garbageThreshold %s is not a valid float number: %v", gcString, err)
			return
		}
	}
	glog.Infoln("garbageThreshold =", gcThreshold)
	ms.Topo.Vacuum(ms.grpcDialOpiton, gcThreshold, ms.preallocateSize)
	ms.dirStatusHandler(w, r)
}

func (ms *MasterServer) volumeGrowHandler(w http.ResponseWriter, r *http.Request) {
	count := 0
	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		writeJsonError(w, r, http.StatusNotAcceptable, err)
		return
	}
	if err == nil {
		if count, err = strconv.Atoi(r.FormValue("count")); err == nil {
			if ms.Topo.FreeSpace() < int64(count*option.ReplicaPlacement.GetCopyCount()) {
				err = fmt.Errorf("only %d volumes left, not enough for %d", ms.Topo.FreeSpace(), count*option.ReplicaPlacement.GetCopyCount())
			} else {
				count, err = ms.vg.GrowByCountAndType(ms.grpcDialOpiton, count, option, ms.Topo)
			}
		} else {
			err = errors.New("parameter count is not found")
		}
	}
	if err != nil {
		writeJsonError(w, r, http.StatusNotAcceptable, err)
	} else {
		writeJsonQuiet(w, r, http.StatusOK, map[string]interface{}{"count": count})
	}
}

func (ms *MasterServer) volumeStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Volumes"] = ms.Topo.ToVolumeMap()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (ms *MasterServer) redirectHandler(w http.ResponseWriter, r *http.Request) {
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, err := needle.NewVolumeId(vid)
	if err != nil {
		debug("parsing error:", err, r.URL.Path)
		return
	}
	collection := r.FormValue("collection")
	machines := ms.Topo.Lookup(collection, volumeId)
	if machines != nil && len(machines) > 0 {
		var url string
		if r.URL.RawQuery != "" {
			url = util.NormalizeUrl(machines[rand.Intn(len(machines))].PublicUrl) + r.URL.Path + "?" + r.URL.RawQuery
		} else {
			url = util.NormalizeUrl(machines[rand.Intn(len(machines))].PublicUrl) + r.URL.Path
		}
		http.Redirect(w, r, url, http.StatusMovedPermanently)
	} else {
		writeJsonError(w, r, http.StatusNotFound, fmt.Errorf("volume id %d or collection %s not found", volumeId, collection))
	}
}

func (ms *MasterServer) selfUrl(r *http.Request) string {
	if r.Host != "" {
		return r.Host
	}
	return "localhost:" + strconv.Itoa(ms.option.Port)
}
func (ms *MasterServer) submitFromMasterServerHandler(w http.ResponseWriter, r *http.Request) {
	if ms.Topo.IsLeader() {
		submitForClientHandler(w, r, ms.selfUrl(r), ms.grpcDialOpiton)
	} else {
		masterUrl, err := ms.Topo.Leader()
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError, err)
		} else {
			submitForClientHandler(w, r, masterUrl, ms.grpcDialOpiton)
		}
	}
}

func (ms *MasterServer) HasWritableVolume(option *topology.VolumeGrowOption) bool {
	vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl)
	return vl.GetActiveVolumeCount(option) > 0
}

func (ms *MasterServer) getVolumeGrowOption(r *http.Request) (*topology.VolumeGrowOption, error) {
	replicationString := r.FormValue("replication")
	if replicationString == "" {
		replicationString = ms.option.DefaultReplicaPlacement
	}
	replicaPlacement, err := storage.NewReplicaPlacementFromString(replicationString)
	if err != nil {
		return nil, err
	}
	ttl, err := needle.ReadTTL(r.FormValue("ttl"))
	if err != nil {
		return nil, err
	}
	preallocate := ms.preallocateSize
	if r.FormValue("preallocate") != "" {
		preallocate, err = strconv.ParseInt(
			r.FormValue("preallocate"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse int64 preallocate = %s: %v",
				r.FormValue("preallocate"), err)
		}
	}
	volumeGrowOption := &topology.VolumeGrowOption{
		Collection:       r.FormValue("collection"),
		ReplicaPlacement: replicaPlacement,
		Ttl:              ttl,
		Prealloacte:      preallocate,
		DataCenter:       r.FormValue("dataCenter"),
		Rack:             r.FormValue("rack"),
		DataNode:         r.FormValue("dataNode"),
	}
	return volumeGrowOption, nil
}
