package weed_server

import (
	"context"

	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/pb/volume_server_pb"
	"github.com/journeymidnight/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) DeleteCollection(ctx context.Context, req *volume_server_pb.DeleteCollectionRequest) (*volume_server_pb.DeleteCollectionResponse, error) {

	resp := &volume_server_pb.DeleteCollectionResponse{}

	err := vs.store.DeleteCollection(req.Collection)

	if err != nil {
		glog.Errorf("delete collection %s: %v", req.Collection, err)
	} else {
		glog.V(2).Infof("delete collection %v", req)
	}

	return resp, err

}

func (vs *VolumeServer) AllocateVolume(ctx context.Context, req *volume_server_pb.AllocateVolumeRequest) (*volume_server_pb.AllocateVolumeResponse, error) {

	resp := &volume_server_pb.AllocateVolumeResponse{}

	err := vs.store.AddVolume(
		needle.VolumeId(req.VolumeId),
		req.Collection,
		vs.needleMapKind,
		req.Replication,
		req.Ttl,
		req.Preallocate,
	)

	if err != nil {
		glog.Errorf("assign volume %v: %v", req, err)
	} else {
		glog.V(2).Infof("assign volume %v", req)
	}

	return resp, err

}

func (vs *VolumeServer) VolumeMount(ctx context.Context, req *volume_server_pb.VolumeMountRequest) (*volume_server_pb.VolumeMountResponse, error) {

	resp := &volume_server_pb.VolumeMountResponse{}

	err := vs.store.MountVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume mount %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume mount %v", req)
	}

	return resp, err

}

func (vs *VolumeServer) VolumeUnmount(ctx context.Context, req *volume_server_pb.VolumeUnmountRequest) (*volume_server_pb.VolumeUnmountResponse, error) {

	resp := &volume_server_pb.VolumeUnmountResponse{}

	err := vs.store.UnmountVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume unmount %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume unmount %v", req)
	}

	return resp, err

}

func (vs *VolumeServer) VolumeDelete(ctx context.Context, req *volume_server_pb.VolumeDeleteRequest) (*volume_server_pb.VolumeDeleteResponse, error) {

	resp := &volume_server_pb.VolumeDeleteResponse{}

	err := vs.store.DeleteVolume(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume delete %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume delete %v", req)
	}

	return resp, err

}

func (vs *VolumeServer) VolumeMarkReadonly(ctx context.Context, req *volume_server_pb.VolumeMarkReadonlyRequest) (*volume_server_pb.VolumeMarkReadonlyResponse, error) {

	resp := &volume_server_pb.VolumeMarkReadonlyResponse{}

	err := vs.store.MarkVolumeReadonly(needle.VolumeId(req.VolumeId))

	if err != nil {
		glog.Errorf("volume mark readonly %v: %v", req, err)
	} else {
		glog.V(2).Infof("volume mark readonly %v", req)
	}

	return resp, err

}
