package replication

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/pb/filer_pb"
	"github.com/journeymidnight/seaweedfs/weed/replication/sink"
	"github.com/journeymidnight/seaweedfs/weed/replication/source"
	"github.com/journeymidnight/seaweedfs/weed/util"
)

type Replicator struct {
	sink   sink.ReplicationSink
	source *source.FilerSource
}

func NewReplicator(sourceConfig util.Configuration, dataSink sink.ReplicationSink) *Replicator {

	source := &source.FilerSource{}
	source.Initialize(sourceConfig)

	dataSink.SetSourceFiler(source)

	return &Replicator{
		sink:   dataSink,
		source: source,
	}
}

func (r *Replicator) Replicate(ctx context.Context, key string, message *filer_pb.EventNotification) error {
	if !strings.HasPrefix(key, r.source.Dir) {
		glog.V(4).Infof("skipping %v outside of %v", key, r.source.Dir)
		return nil
	}
	newKey := filepath.ToSlash(filepath.Join(r.sink.GetSinkToDirectory(), key[len(r.source.Dir):]))
	glog.V(3).Infof("replicate %s => %s", key, newKey)
	key = newKey
	if message.OldEntry != nil && message.NewEntry == nil {
		glog.V(4).Infof("deleting %v", key)
		return r.sink.DeleteEntry(ctx, key, message.OldEntry.IsDirectory, message.DeleteChunks)
	}
	if message.OldEntry == nil && message.NewEntry != nil {
		glog.V(4).Infof("creating %v", key)
		return r.sink.CreateEntry(ctx, key, message.NewEntry)
	}
	if message.OldEntry == nil && message.NewEntry == nil {
		glog.V(0).Infof("weird message %+v", message)
		return nil
	}

	foundExisting, err := r.sink.UpdateEntry(ctx, key, message.OldEntry, message.NewParentPath, message.NewEntry, message.DeleteChunks)
	if foundExisting {
		glog.V(4).Infof("updated %v", key)
		return err
	}

	err = r.sink.DeleteEntry(ctx, key, message.OldEntry.IsDirectory, false)
	if err != nil {
		return fmt.Errorf("delete old entry %v: %v", key, err)
	}

	glog.V(4).Infof("creating missing %v", key)
	return r.sink.CreateEntry(ctx, key, message.NewEntry)
}
