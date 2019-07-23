package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/journeymidnight/seaweedfs/weed/operation"
	"github.com/journeymidnight/seaweedfs/weed/pb/volume_server_pb"
	"github.com/journeymidnight/seaweedfs/weed/storage/erasure_coding"
	"github.com/journeymidnight/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandEcRebuild{})
}

type commandEcRebuild struct {
}

func (c *commandEcRebuild) Name() string {
	return "ec.rebuild"
}

func (c *commandEcRebuild) Help() string {
	return `find and rebuild missing ec shards among volume servers

	ec.rebuild [-c EACH_COLLECTION|<collection_name>] [-force]

	Algorithm:

	For each type of volume server (different max volume count limit){
		for each collection {
			rebuildEcVolumes()
		}
	}

	func rebuildEcVolumes(){
		idealWritableVolumes = totalWritableVolumes / numVolumeServers
		for {
			sort all volume servers ordered by the number of local writable volumes
			pick the volume server A with the lowest number of writable volumes x
			pick the volume server B with the highest number of writable volumes y
			if y > idealWritableVolumes and x +1 <= idealWritableVolumes {
				if B has a writable volume id v that A does not have {
					move writable volume v from A to B
				}
			}
		}
	}

`
}

func (c *commandEcRebuild) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fixCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := fixCommand.String("collection", "EACH_COLLECTION", "collection name, or \"EACH_COLLECTION\" for each collection")
	applyChanges := fixCommand.Bool("force", false, "apply the changes")
	if err = fixCommand.Parse(args); err != nil {
		return nil
	}

	// collect all ec nodes
	allEcNodes, _, err := collectEcNodes(context.Background(), commandEnv, "")
	if err != nil {
		return err
	}

	if *collection == "EACH_COLLECTION" {
		collections, err := ListCollectionNames(commandEnv, false, true)
		if err != nil {
			return err
		}
		fmt.Printf("rebuildEcVolumes collections %+v\n", len(collections))
		for _, c := range collections {
			fmt.Printf("rebuildEcVolumes collection %+v\n", c)
			if err = rebuildEcVolumes(commandEnv, allEcNodes, c, writer, *applyChanges); err != nil {
				return err
			}
		}
	} else {
		if err = rebuildEcVolumes(commandEnv, allEcNodes, *collection, writer, *applyChanges); err != nil {
			return err
		}
	}

	return nil
}

func rebuildEcVolumes(commandEnv *CommandEnv, allEcNodes []*EcNode, collection string, writer io.Writer, applyChanges bool) error {

	ctx := context.Background()

	fmt.Printf("rebuildEcVolumes %s\n", collection)

	// collect vid => each shard locations, similar to ecShardMap in topology.go
	ecShardMap := make(EcShardMap)
	for _, ecNode := range allEcNodes {
		ecShardMap.registerEcNode(ecNode, collection)
	}

	for vid, locations := range ecShardMap {
		shardCount := locations.shardCount()
		if shardCount == erasure_coding.TotalShardsCount {
			continue
		}
		if shardCount < erasure_coding.DataShardsCount {
			return fmt.Errorf("ec volume %d is unrepairable with %d shards\n", vid, shardCount)
		}

		sortEcNodes(allEcNodes)

		if allEcNodes[0].freeEcSlot < erasure_coding.TotalShardsCount {
			return fmt.Errorf("disk space is not enough")
		}

		if err := rebuildOneEcVolume(ctx, commandEnv, allEcNodes[0], collection, vid, locations, writer, applyChanges); err != nil {
			return err
		}
	}

	return nil
}

func rebuildOneEcVolume(ctx context.Context, commandEnv *CommandEnv, rebuilder *EcNode, collection string, volumeId needle.VolumeId, locations EcShardLocations, writer io.Writer, applyChanges bool) error {

	fmt.Printf("rebuildOneEcVolume %s %d\n", collection, volumeId)

	// collect shard files to rebuilder local disk
	var generatedShardIds []uint32
	copiedShardIds, _, err := prepareDataToRecover(ctx, commandEnv, rebuilder, collection, volumeId, locations, writer, applyChanges)
	if err != nil {
		return err
	}
	defer func() {
		// clean up working files

		// ask the rebuilder to delete the copied shards
		err = sourceServerDeleteEcShards(ctx, commandEnv.option.GrpcDialOption, collection, volumeId, rebuilder.info.Id, copiedShardIds)
		if err != nil {
			fmt.Fprintf(writer, "%s delete copied ec shards %s %d.%v\n", rebuilder.info.Id, collection, volumeId, copiedShardIds)
		}

	}()

	if !applyChanges {
		return nil
	}

	// generate ec shards, and maybe ecx file
	generatedShardIds, err = generateMissingShards(ctx, commandEnv.option.GrpcDialOption, collection, volumeId, rebuilder.info.Id)
	if err != nil {
		return err
	}

	// mount the generated shards
	err = mountEcShards(ctx, commandEnv.option.GrpcDialOption, collection, volumeId, rebuilder.info.Id, generatedShardIds)
	if err != nil {
		return err
	}

	rebuilder.addEcVolumeShards(volumeId, collection, generatedShardIds)

	return nil
}

func generateMissingShards(ctx context.Context, grpcDialOption grpc.DialOption,
	collection string, volumeId needle.VolumeId, sourceLocation string) (rebuiltShardIds []uint32, err error) {

	err = operation.WithVolumeServerClient(sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, rebultErr := volumeServerClient.VolumeEcShardsRebuild(ctx, &volume_server_pb.VolumeEcShardsRebuildRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
		})
		if rebultErr == nil {
			rebuiltShardIds = resp.RebuiltShardIds
		}
		return rebultErr
	})
	return
}

func prepareDataToRecover(ctx context.Context, commandEnv *CommandEnv, rebuilder *EcNode, collection string, volumeId needle.VolumeId, locations EcShardLocations, writer io.Writer, applyBalancing bool) (copiedShardIds []uint32, localShardIds []uint32, err error) {

	needEcxFile := true
	var localShardBits erasure_coding.ShardBits
	for _, ecShardInfo := range rebuilder.info.EcShardInfos {
		if ecShardInfo.Collection == collection && needle.VolumeId(ecShardInfo.Id) == volumeId {
			needEcxFile = false
			localShardBits = erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
		}
	}

	for shardId, ecNodes := range locations {

		if len(ecNodes) == 0 {
			fmt.Fprintf(writer, "missing shard %d.%d\n", volumeId, shardId)
			continue
		}

		if localShardBits.HasShardId(erasure_coding.ShardId(shardId)) {
			localShardIds = append(localShardIds, uint32(shardId))
			fmt.Fprintf(writer, "use existing shard %d.%d\n", volumeId, shardId)
			continue
		}

		var copyErr error
		if applyBalancing {
			copyErr = operation.WithVolumeServerClient(rebuilder.info.Id, commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				_, copyErr := volumeServerClient.VolumeEcShardsCopy(ctx, &volume_server_pb.VolumeEcShardsCopyRequest{
					VolumeId:       uint32(volumeId),
					Collection:     collection,
					ShardIds:       []uint32{uint32(shardId)},
					CopyEcxFile:    needEcxFile,
					SourceDataNode: ecNodes[0].info.Id,
				})
				return copyErr
			})
			if copyErr == nil && needEcxFile {
				needEcxFile = false
			}
		}
		if copyErr != nil {
			fmt.Fprintf(writer, "%s failed to copy %d.%d from %s: %v\n", rebuilder.info.Id, volumeId, shardId, ecNodes[0].info.Id, copyErr)
		} else {
			fmt.Fprintf(writer, "%s copied %d.%d from %s\n", rebuilder.info.Id, volumeId, shardId, ecNodes[0].info.Id)
			copiedShardIds = append(copiedShardIds, uint32(shardId))
		}

	}

	if len(copiedShardIds)+len(localShardIds) >= erasure_coding.DataShardsCount {
		return copiedShardIds, localShardIds, nil
	}

	return nil, nil, fmt.Errorf("%d shards are not enough to recover volume %d", len(copiedShardIds)+len(localShardIds), volumeId)

}

type EcShardMap map[needle.VolumeId]EcShardLocations
type EcShardLocations [][]*EcNode

func (ecShardMap EcShardMap) registerEcNode(ecNode *EcNode, collection string) {
	for _, shardInfo := range ecNode.info.EcShardInfos {
		if shardInfo.Collection == collection {
			existing, found := ecShardMap[needle.VolumeId(shardInfo.Id)]
			if !found {
				existing = make([][]*EcNode, erasure_coding.TotalShardsCount)
				ecShardMap[needle.VolumeId(shardInfo.Id)] = existing
			}
			for _, shardId := range erasure_coding.ShardBits(shardInfo.EcIndexBits).ShardIds() {
				existing[shardId] = append(existing[shardId], ecNode)
			}
		}
	}
}

func (ecShardLocations EcShardLocations) shardCount() (count int) {
	for _, locations := range ecShardLocations {
		if len(locations) > 0 {
			count++
		}
	}
	return
}
