package storage

import (
	"os"

	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/storage/idx"
	"github.com/journeymidnight/seaweedfs/weed/storage/needle_map"
	. "github.com/journeymidnight/seaweedfs/weed/storage/types"
)

type NeedleMap struct {
	baseNeedleMapper
	m needle_map.NeedleValueMap
}

func NewCompactNeedleMap(file *os.File) *NeedleMap {
	nm := &NeedleMap{
		m: needle_map.NewCompactMap(),
	}
	nm.indexFile = file
	return nm
}

func NewBtreeNeedleMap(file *os.File) *NeedleMap {
	nm := &NeedleMap{
		m: needle_map.NewBtreeMap(),
	}
	nm.indexFile = file
	return nm
}

func LoadCompactNeedleMap(file *os.File) (*NeedleMap, error) {
	nm := NewCompactNeedleMap(file)
	return doLoading(file, nm)
}

func LoadBtreeNeedleMap(file *os.File) (*NeedleMap, error) {
	nm := NewBtreeNeedleMap(file)
	return doLoading(file, nm)
}

func doLoading(file *os.File, nm *NeedleMap) (*NeedleMap, error) {
	e := idx.WalkIndexFile(file, func(key NeedleId, offset Offset, size uint32) error {
		nm.MaybeSetMaxFileKey(key)
		if !offset.IsZero() && size != TombstoneFileSize {
			nm.FileCounter++
			nm.FileByteCounter = nm.FileByteCounter + uint64(size)
			oldOffset, oldSize := nm.m.Set(NeedleId(key), offset, size)
			// glog.V(3).Infoln("reading key", key, "offset", offset*NeedlePaddingSize, "size", size, "oldSize", oldSize)
			if !oldOffset.IsZero() && oldSize != TombstoneFileSize {
				nm.DeletionCounter++
				nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
			}
		} else {
			oldSize := nm.m.Delete(NeedleId(key))
			// glog.V(3).Infoln("removing key", key, "offset", offset*NeedlePaddingSize, "size", size, "oldSize", oldSize)
			nm.DeletionCounter++
			nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
		}
		return nil
	})
	glog.V(1).Infof("max file key: %d for file: %s", nm.MaxFileKey(), file.Name())
	return nm, e
}

func (nm *NeedleMap) Put(key NeedleId, offset Offset, size uint32) error {
	_, oldSize := nm.m.Set(NeedleId(key), offset, size)
	nm.logPut(key, oldSize, size)
	return nm.appendToIndexFile(key, offset, size)
}
func (nm *NeedleMap) Get(key NeedleId) (element *needle_map.NeedleValue, ok bool) {
	element, ok = nm.m.Get(NeedleId(key))
	return
}
func (nm *NeedleMap) Delete(key NeedleId, offset Offset) error {
	deletedBytes := nm.m.Delete(NeedleId(key))
	nm.logDelete(deletedBytes)
	return nm.appendToIndexFile(key, offset, TombstoneFileSize)
}
func (nm *NeedleMap) Close() {
	_ = nm.indexFile.Close()
}
func (nm *NeedleMap) Destroy() error {
	nm.Close()
	return os.Remove(nm.indexFile.Name())
}
