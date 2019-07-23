package storage

import (
	"context"
	"fmt"

	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/operation"
	"github.com/journeymidnight/seaweedfs/weed/pb/volume_server_pb"
	"github.com/journeymidnight/seaweedfs/weed/storage/erasure_coding"
	"github.com/journeymidnight/seaweedfs/weed/storage/needle"
	"github.com/journeymidnight/seaweedfs/weed/storage/types"
)

func (s *Store) DeleteEcShardNeedle(ctx context.Context, ecVolume *erasure_coding.EcVolume, n *needle.Needle, cookie types.Cookie) (int64, error) {

	count, err := s.ReadEcShardNeedle(ctx, ecVolume.VolumeId, n)

	if err != nil {
		return 0, err
	}

	if cookie != n.Cookie {
		return 0, fmt.Errorf("unexpected cookie %x", cookie)
	}

	if err = s.doDeleteNeedleFromAtLeastOneRemoteEcShards(ctx, ecVolume, n.Id); err != nil {
		return 0, err
	}

	return int64(count), nil

}

func (s *Store) doDeleteNeedleFromAtLeastOneRemoteEcShards(ctx context.Context, ecVolume *erasure_coding.EcVolume, needleId types.NeedleId) error {

	_, _, intervals, err := ecVolume.LocateEcShardNeedle(needleId, ecVolume.Version)

	if len(intervals) == 0 {
		return erasure_coding.NotFoundError
	}

	shardId, _ := intervals[0].ToShardIdAndOffset(erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingSmallBlockSize)

	hasDeletionSuccess := false
	err = s.doDeleteNeedleFromRemoteEcShardServers(ctx, shardId, ecVolume, needleId)
	if err == nil {
		hasDeletionSuccess = true
	}

	for shardId = erasure_coding.DataShardsCount; shardId < erasure_coding.TotalShardsCount; shardId++ {
		if parityDeletionError := s.doDeleteNeedleFromRemoteEcShardServers(ctx, shardId, ecVolume, needleId); parityDeletionError == nil {
			hasDeletionSuccess = true
		}
	}

	if hasDeletionSuccess {
		return nil
	}

	return err

}

func (s *Store) doDeleteNeedleFromRemoteEcShardServers(ctx context.Context, shardId erasure_coding.ShardId, ecVolume *erasure_coding.EcVolume, needleId types.NeedleId) error {

	ecVolume.ShardLocationsLock.RLock()
	sourceDataNodes, hasShardLocations := ecVolume.ShardLocations[shardId]
	ecVolume.ShardLocationsLock.RUnlock()

	if !hasShardLocations {
		return fmt.Errorf("ec shard %d.%d not located", ecVolume.VolumeId, shardId)
	}

	for _, sourceDataNode := range sourceDataNodes {
		glog.V(4).Infof("delete from remote ec shard %d.%d from %s", ecVolume.VolumeId, shardId, sourceDataNode)
		err := s.doDeleteNeedleFromRemoteEcShard(ctx, sourceDataNode, ecVolume.VolumeId, ecVolume.Collection, ecVolume.Version, needleId)
		if err != nil {
			return err
		}
		glog.V(1).Infof("delete from remote ec shard %d.%d from %s: %v", ecVolume.VolumeId, shardId, sourceDataNode, err)
	}

	return nil

}

func (s *Store) doDeleteNeedleFromRemoteEcShard(ctx context.Context, sourceDataNode string, vid needle.VolumeId, collection string, version needle.Version, needleId types.NeedleId) error {

	return operation.WithVolumeServerClient(sourceDataNode, s.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// copy data slice
		_, err := client.VolumeEcBlobDelete(ctx, &volume_server_pb.VolumeEcBlobDeleteRequest{
			VolumeId:   uint32(vid),
			Collection: collection,
			FileKey:    uint64(needleId),
			Version:    uint32(version),
		})
		if err != nil {
			return fmt.Errorf("failed to delete from ec shard %d on %s: %v", vid, sourceDataNode, err)
		}
		return nil
	})

}
