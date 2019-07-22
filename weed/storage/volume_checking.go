package storage

import (
	"fmt"
	"os"

	"github.com/journeymidnight/seaweedfs/weed/storage/needle"
	. "github.com/journeymidnight/seaweedfs/weed/storage/types"
	"github.com/journeymidnight/seaweedfs/weed/util"
)

func CheckVolumeDataIntegrity(v *Volume, indexFile *os.File) (lastAppendAtNs uint64, e error) {
	// FIXME
	return 0, nil
}

func verifyIndexFileIntegrity(indexFile *os.File) (indexSize int64, err error) {
	if indexSize, err = util.GetFileSize(indexFile); err == nil {
		if indexSize%NeedleMapEntrySize != 0 {
			err = fmt.Errorf("index file's size is %d bytes, maybe corrupted", indexSize)
		}
	}
	return
}

func readIndexEntryAtOffset(indexFile *os.File, offset int64) (bytes []byte, err error) {
	if offset < 0 {
		err = fmt.Errorf("offset %d for index file is invalid", offset)
		return
	}
	bytes = make([]byte, NeedleMapEntrySize)
	_, err = indexFile.ReadAt(bytes, offset)
	return
}

func verifyNeedleIntegrity(datFile *os.File, v needle.Version, offset int64, key NeedleId, size uint32) (lastAppendAtNs uint64, err error) {
	n := new(needle.Needle)
	if err = n.ReadData(datFile, offset, size, v); err != nil {
		return n.AppendAtNs, err
	}
	if n.Id != key {
		return n.AppendAtNs, fmt.Errorf("index key %#x does not match needle's Id %#x", key, n.Id)
	}
	return n.AppendAtNs, err
}
