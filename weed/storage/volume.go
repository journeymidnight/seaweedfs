package storage

import (
	"fmt"
	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/pb/master_pb"
	"github.com/journeymidnight/seaweedfs/weed/stats"
	"github.com/journeymidnight/seaweedfs/weed/storage/needle"
	"github.com/journeymidnight/seaweedfs/weed/util"
	cannlys "github.com/thesues/cannyls-go/storage"
	"os"
	"path"
	"strconv"
	"time"
)

type Volume struct {
	Id           needle.VolumeId
	dir          string
	Collection   string
	readOnly     bool
	store        *cannlys.Storage
	queryChannel chan query
	closeSignal  chan interface{}

	SuperBlock

	lastModifiedTsSeconds uint64 //unix time in seconds
	lastAppendAtNs        uint64 //unix time in nanoseconds

	lastCompactIndexOffset uint64
	lastCompactRevision    uint16
}

func NewVolume(dirname string, collection string, id needle.VolumeId, needleMapKind NeedleMapType,
	replicaPlacement *ReplicaPlacement, ttl *needle.TTL, preallocate int64) (v *Volume, err error) {

	v = &Volume{
		dir:          dirname,
		Collection:   collection,
		Id:           id,
		queryChannel: make(chan query, 1024), // TODO tuning length
		closeSignal:  make(chan interface{}),
		SuperBlock: SuperBlock{
			ReplicaPlacement: replicaPlacement,
			Ttl:              ttl,
			version:          needle.CurrentVersion,
		},
	}
	_, err = os.Stat(v.FileName())
	if os.IsNotExist(err) { // create new volume
		err = v.InitializeDiskFiles()
	} else { // load from disk
		err = v.LoadDiskFiles()
	}
	if err != nil {
		return nil, err
	}
	go v.workerThread()
	return v, nil
}

func (v *Volume) InitializeDiskFiles() (err error) {
	// create data file
	store, err := cannlys.CreateCannylsStorage(v.FileName(),
		util.VolumeSizeLimitGB<<30,
		0.1) // TODO tuning ratio
	if err != nil {
		return err
	}
	v.store = store
	// create meta file
	f, err := os.Create(v.MetaFileName())
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(v.SuperBlock.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (v *Volume) LoadDiskFiles() (err error) {
	// load data file
	store, err := cannlys.OpenCannylsStorage(v.FileName())
	if err != nil {
		return err
	}
	v.store = store
	// load meta file
	f, err := os.Open(v.MetaFileName())
	if err != nil {
		return err
	}
	defer f.Close()
	superBlock, err := ReadSuperBlock(f)
	if err != nil {
		return err
	}
	v.SuperBlock = superBlock
	return nil
}

type queryType int

const (
	getQuery queryType = iota
	putQuery
	deleteQuery
)

type query struct {
	queryType queryType
	needle    *needle.Needle
	result    chan error
}

// cannlys-go only supports single-thread access
func (v *Volume) workerThread() {
	for {
		q, ok := <-v.queryChannel
		if !ok { // queue closed, which indicates store is closed
			return
		}
		v.handleQuery(q)
	}
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

func (v *Volume) MetaFileName() string {
	return v.FileName() + ".meta"
}

func (v *Volume) FileStat() (datSize uint64, idxSize uint64, modTime time.Time) {
	usage := v.store.Usage()
	fileInfo, err := os.Stat(v.MetaFileName())
	if err != nil {
		idxSize = 0
	} else {
		idxSize = uint64(fileInfo.Size())
	}
	return usage.CurrentFileSize, idxSize, time.Now()
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
	close(v.closeSignal)
	close(v.queryChannel)
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
