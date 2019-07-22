package storage

import (
	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/stats"
	"github.com/journeymidnight/seaweedfs/weed/storage/needle"
	"github.com/journeymidnight/seaweedfs/weed/util"
	"github.com/thesues/cannyls-go/lump"
	"os"
)

func (v *Volume) garbageLevel() float64 {
	return 0
}

func (v *Volume) Compact(preallocate int64, compactionBytePerSecond int64) error {
	glog.V(3).Infof("Compacting volume %d ...", v.Id)
	return nil
}

func (v *Volume) Compact2() error {
	glog.V(3).Infof("Compact2 volume %d ...", v.Id)
	return nil
}

func (v *Volume) CommitCompact() error {
	glog.V(0).Infof("Committing volume %d vacuuming...", v.Id)
	glog.V(3).Infof("Got volume %d committing lock...", v.Id)
	stats.VolumeServerVolumeCounter.WithLabelValues(v.Collection, "volume").Inc()
	glog.V(3).Infof("Loading volume %d commit file...", v.Id)
	return nil
}

func (v *Volume) cleanupCompact() error {
	glog.V(0).Infof("Cleaning up volume %d vacuuming...", v.Id)

	e1 := os.Remove(v.FileName() + ".cpd")
	e2 := os.Remove(v.FileName() + ".cpx")
	if e1 != nil {
		return e1
	}
	if e2 != nil {
		return e2
	}
	return nil
}

func fetchCompactRevisionFromDatFile(file *os.File) (compactRevision uint16, err error) {
	superBlock, err := ReadSuperBlock(file)
	if err != nil {
		return 0, err
	}
	return superBlock.CompactionRevision, nil
}

type VolumeFileScanner4Vacuum struct {
	version        needle.Version
	v              *Volume
	dst            *os.File
	nm             *NeedleMap
	newOffset      int64
	now            uint64
	writeThrottler *util.WriteThrottler
}

func (scanner *VolumeFileScanner4Vacuum) VisitSuperBlock(superBlock SuperBlock) error {
	scanner.version = superBlock.Version()
	superBlock.CompactionRevision++
	_, err := scanner.dst.Write(superBlock.Bytes())
	scanner.newOffset = int64(superBlock.BlockSize())
	return err

}
func (scanner *VolumeFileScanner4Vacuum) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4Vacuum) VisitNeedle(n *needle.Needle,
	offset int64) (err error) {

	if n.HasTtl() && scanner.now >= n.LastModified+uint64(scanner.v.Ttl.Minutes()*60) {
		return nil
	}
	lumpId := lump.FromU64(0, uint64(n.Id))
	n.Data, err = scanner.v.store.Get(lumpId)
	return err
}
