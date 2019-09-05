package weed_server

import (
	"context"
	"fmt"
	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/operation"
	"github.com/journeymidnight/seaweedfs/weed/pb/volume_server_pb"
	"github.com/journeymidnight/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) VolumeTailSender(req *volume_server_pb.VolumeTailSenderRequest, stream volume_server_pb.VolumeServer_VolumeTailSenderServer) error {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumeId)
	}
	// FIXME
	panic("not implementet yet")
}

func (vs *VolumeServer) VolumeTailReceiver(ctx context.Context, req *volume_server_pb.VolumeTailReceiverRequest) (*volume_server_pb.VolumeTailReceiverResponse, error) {

	resp := &volume_server_pb.VolumeTailReceiverResponse{}

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return resp, fmt.Errorf("receiver not found volume id %d", req.VolumeId)
	}

	defer glog.V(1).Infof("receive tailing volume %d finished", v.Id)

	return resp, operation.TailVolumeFromSource(req.SourceVolumeServer, vs.grpcDialOption, v.Id, req.SinceNs, int(req.IdleTimeoutSeconds), func(n *needle.Needle) error {
		return vs.store.Write(v.Id, n, 0, 0)
	})

}
