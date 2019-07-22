package storage

import (
	"fmt"
	"github.com/journeymidnight/seaweedfs/weed/util"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/pb/master_pb"
	"github.com/journeymidnight/seaweedfs/weed/stats"
	"github.com/journeymidnight/seaweedfs/weed/storage/needle"
	cannlys "github.com/thesues/cannyls-go/storage"
)

type Volume struct {
	Id         needle.VolumeId
	dir        string
	Collection string
	readOnly   bool
	store      *cannlys.Storage

	SuperBlock

	lastModifiedTsSeconds uint64 //unix time in seconds
	lastAppendAtNs        uint64 //unix time in nanoseconds

	lastCompactIndexOffset uint64
	lastCompactRevision    uint16
}

func NewVolume(dirname string, collection string, id needle.VolumeId, needleMapKind NeedleMapType,
	replicaPlacement *ReplicaPlacement, ttl *needle.TTL, preallocate int64) (v *Volume, e error) {

	// FIXME
	if replicaPlacement == nil {
		replicaPlacement = &ReplicaPlacement{0, 0, 0}
	}
	if ttl == nil {
		ttl = &needle.TTL{}
	}

	v = &Volume{
		dir:        dirname,
		Collection: collection,
		Id:         id,
		SuperBlock: SuperBlock{
			ReplicaPlacement: replicaPlacement,
			Ttl:              ttl,
			version:          needle.CurrentVersion,
		},
	}
	_, err := os.Stat(v.FileName())
	var store *cannlys.Storage
	if os.IsNotExist(err) {
		store, e = cannlys.CreateCannylsStorage(v.FileName(),
			util.VolumeSizeLimitGB<<30,
			0.1) // TODO tuning
	} else {
		store, e = cannlys.OpenCannylsStorage(v.FileName())
	}
	if e != nil {
		return nil, e
	}
	v.store = store

	return v, nil
}
func (v *Volume) String() string {
	return fmt.Sprintf("Id:%v, dir:%s, Collection:%s, dataFile:%v, readOnly:%v",
		v.Id, v.dir, v.Collection, v.FileName(), v.readOnly)
}

func VolumeFileName(dir string, collection string, id int) (fileName string) {
	idString := strconv.Itoa(id)
	if collection == "" {
		fileName = path.Join(dir, idString+".lump")
	} else {
		fileName = path.Join(dir, collection+"_"+idString+".lump")
	}
	return
}
func (v *Volume) FileName() (fileName string) {
	return VolumeFileName(v.dir, v.Collection, int(v.Id))
}

func (v *Volume) FileStat() (datSize uint64, idxSize uint64, modTime time.Time) {
	// FIXME
	usage := v.store.Usage()
	return usage.CurrentFileSize, usage.JournalCapacity, time.Now()
}

func (v *Volume) IndexFileSize() uint64 {
	return 0
}

func (v *Volume) FileCount() uint64 {
	usage := v.store.Usage()
	return usage.FileCounts
}

// Close cleanly shuts down this volume
func (v *Volume) Close() {
	v.store.Close()
	stats.VolumeServerVolumeCounter.WithLabelValues(v.Collection, "volume").Dec()
}

func (v *Volume) NeedToReplicate() bool {
	return v.ReplicaPlacement.GetCopyCount() > 1
}

func (v *Volume) ContentSize() uint64 {
	usage := v.store.Usage()
	return usage.CurrentFileSize
}

// volume is expired if modified time + volume ttl < now
// except when volume is empty
// or when the volume does not have a ttl
// or when volumeSizeLimit is 0 when server just starts
func (v *Volume) expired(volumeSizeLimit uint64) bool {
	if volumeSizeLimit == 0 {
		//skip if we don't know size limit
		return false
	}
	if v.ContentSize() == 0 {
		return false
	}
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	glog.V(1).Infof("now:%v lastModified:%v",
		time.Now().Unix(), v.lastModifiedTsSeconds)
	livedMinutes := (time.Now().Unix() - int64(v.lastModifiedTsSeconds)) / 60
	glog.V(1).Infof("ttl:%v lived:%v", v.Ttl, livedMinutes)
	if int64(v.Ttl.Minutes()) < livedMinutes {
		return true
	}
	return false
}

// wait either maxDelayMinutes or 10% of ttl minutes
func (v *Volume) expiredLongEnough(maxDelayMinutes uint32) bool {
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	removalDelay := v.Ttl.Minutes() / 10
	if removalDelay > maxDelayMinutes {
		removalDelay = maxDelayMinutes
	}

	if uint64(v.Ttl.Minutes()+removalDelay)*60+v.lastModifiedTsSeconds < uint64(time.Now().Unix()) {
		return true
	}
	return false
}

func (v *Volume) ToVolumeInformationMessage() *master_pb.VolumeInformationMessage {
	size, _, modTime := v.FileStat()
	usage := v.store.Usage()
	return &master_pb.VolumeInformationMessage{
		Id:               uint32(v.Id),
		Size:             size,
		Collection:       v.Collection,
		FileCount:        usage.FileCounts,
		DeleteCount:      0, // FIXME
		DeletedByteCount: 0, // FIXME
		ReadOnly:         v.readOnly,
		ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
		Version:          uint32(v.Version()),
		Ttl:              v.Ttl.ToUint32(),
		CompactRevision:  uint32(v.SuperBlock.CompactionRevision),
		ModifiedAtSecond: modTime.Unix(),
	}
}
