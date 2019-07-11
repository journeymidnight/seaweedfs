package storage

import (
	"errors"
	"fmt"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	"io"
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
)

var ErrorNotFound = errors.New("not found")

// Destroy removes everything related to this volume
func (v *Volume) Destroy() (err error) {
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.FileName())
		return
	}
	v.Close()
	os.Remove(v.FileName() + ".dat")
	os.Remove(v.FileName() + ".idx")
	os.Remove(v.FileName() + ".cpd")
	os.Remove(v.FileName() + ".cpx")
	os.Remove(v.FileName() + ".ldb")
	os.Remove(v.FileName() + ".bdb")
	return
}

func (v *Volume) writeNeedle(n *needle.Needle) (offset uint64, size uint32, isUnchanged bool, err error) {
	glog.V(4).Infof("writing needle %s",
		needle.NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.FileName())
		return
	}

	if n.Ttl == needle.EMPTY_TTL && v.Ttl != needle.EMPTY_TTL {
		n.SetHasTtl()
		n.Ttl = v.Ttl
	}
	lumpId := lump.FromU64(0, uint64(n.Id))
	data := block.FromBytes(n.Data, block.Min())
	lumpData := lump.NewLumpDataWithAb(data)
	updated, err := v.store.Put(lumpId, lumpData)
	if err != nil {
		return 0, 0, false, err
	}
	isUnchanged = !updated

	n.AppendAtNs = uint64(time.Now().UnixNano())
	v.lastAppendAtNs = n.AppendAtNs
	if v.lastModifiedTsSeconds < n.LastModified {
		v.lastModifiedTsSeconds = n.LastModified
	}
	return
}

func (v *Volume) deleteNeedle(n *needle.Needle) (uint32, error) {
	glog.V(4).Infof("delete needle %s", needle.NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly {
		return 0, fmt.Errorf("%s is read-only", v.FileName())
	}
	lumpId := lump.FromU64(0, uint64(n.Id))
	_, err := v.store.Delete(lumpId)
	if err != nil {
		return n.Size, err
	}
	return 0, nil
}

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) readNeedle(n *needle.Needle) (int, error) {
	var err error
	lumpId := lump.FromU64(0, uint64(n.Id))
	n.Data, err = v.store.Get(lumpId)
	if err != nil {
		return -1, err
	}
	bytesRead := len(n.Data)
	if !n.HasTtl() {
		return bytesRead, nil
	}
	ttlMinutes := n.Ttl.Minutes()
	if ttlMinutes == 0 {
		return bytesRead, nil
	}
	if !n.HasLastModifiedDate() {
		return bytesRead, nil
	}
	if uint64(time.Now().Unix()) < n.LastModified+uint64(ttlMinutes*60) {
		return bytesRead, nil
	}
	return -1, ErrorNotFound
}

type VolumeFileScanner interface {
	VisitSuperBlock(SuperBlock) error
	ReadNeedleBody() bool
	VisitNeedle(n *needle.Needle, offset int64) error
}

func ScanVolumeFile(dirname string, collection string, id needle.VolumeId,
	needleMapKind NeedleMapType,
	volumeFileScanner VolumeFileScanner) (err error) {

	// FIXME
	return nil
}

func ScanVolumeFileNeedleFrom(version needle.Version, dataFile *os.File, offset int64, fn func(needleHeader, needleBody []byte, needleAppendAtNs uint64) error) (err error) {
	n, nh, rest, e := needle.ReadNeedleHeader(dataFile, version, offset)
	if e != nil {
		if e == io.EOF {
			return nil
		}
		return fmt.Errorf("cannot read %s at offset %d: %v", dataFile.Name(), offset, e)
	}
	for n != nil {
		var needleBody []byte
		if needleBody, err = n.ReadNeedleBody(dataFile, version, offset+NeedleHeaderSize, rest); err != nil {
			glog.V(0).Infof("cannot read needle body: %v", err)
			//err = fmt.Errorf("cannot read needle body: %v", err)
			//return
		}
		err = fn(nh, needleBody, n.AppendAtNs)
		if err != nil {
			glog.V(0).Infof("visit needle error: %v", err)
			return
		}
		offset += NeedleHeaderSize + rest
		glog.V(4).Infof("==> new entry offset %d", offset)
		if n, nh, rest, err = needle.ReadNeedleHeader(dataFile, version, offset); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("cannot read needle header at offset %d: %v", offset, err)
		}
		glog.V(4).Infof("new entry needle size:%d rest:%d", n.Size, rest)
	}
	return nil
}
