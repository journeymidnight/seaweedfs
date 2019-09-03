package storage

import (
	"errors"
	"fmt"
	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/storage/needle"
	. "github.com/journeymidnight/seaweedfs/weed/storage/types"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/storage"
	"io"
	"os"
	"time"
)

var ErrorNotFound = errors.New("not found")

// Destroy removes everything related to this volume
func (v *Volume) Destroy() (err error) {
	if v.readOnly {
		err = fmt.Errorf("%s is read-only", v.FileName())
		return
	}
	v.Close()
	os.Remove(v.FileName() + ".lusf")
	os.Remove(v.FileName() + ".idx")
	os.Remove(v.FileName() + ".cpd")
	os.Remove(v.FileName() + ".cpx")
	os.Remove(v.FileName() + ".ldb")
	os.Remove(v.FileName() + ".bdb")
	return
}

type queryType int

const (
	getQuery queryType = iota
	putQuery
	deleteQuery
	usageQuery
)

type queryResult struct {
	result interface{}
	err    error
}

type query struct {
	queryType     queryType
	needle        *needle.Needle
	resultChannel chan queryResult
	startOffset   int64
	length        uint64
}

func (v *Volume) handleQuery(q query) {
	select {
	case <-v.closeSignal:
		result := queryResult{
			err: errors.New("volume store is shutting down"),
		}
		q.resultChannel <- result
		return
	default:
	}

	var result queryResult
	switch q.queryType {
	case getQuery:
		lumpId := lump.FromU64(0, uint64(q.needle.Id))
		if q.length == 0 { // length == 0 means whole object
			q.needle.Data, result.err = v.store.Get(lumpId)
		} else {
			q.needle.Data, result.err = v.store.GetWithOffset(lumpId,
				uint32(q.startOffset), uint32(q.length))
		}
	case putQuery:
		lumpId := lump.FromU64(0, uint64(q.needle.Id))
		data := block.FromBytes(q.needle.Data, block.Min())
		lumpData := lump.NewLumpDataWithAb(data)
		_, result.err = v.store.Put(lumpId, lumpData)
	case deleteQuery:
		lumpId := lump.FromU64(0, uint64(q.needle.Id))
		_, q.needle.Size, result.err = v.store.Delete(lumpId)
	case usageQuery:
		result.result = v.store.Usage()
	default:
		panic("shouldn't reach here")
	}
	q.resultChannel <- result
}

func (v *Volume) getUsage() storage.StorageUsage {
	q := query{
		queryType:     usageQuery,
		resultChannel: make(chan queryResult),
	}
	v.queryChannel <- q
	result := <-q.resultChannel
	return result.result.(storage.StorageUsage)
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
	q := query{
		queryType:     putQuery,
		needle:        n,
		resultChannel: make(chan queryResult),
	}
	v.queryChannel <- q
	result := <-q.resultChannel
	if result.err != nil {
		return 0, 0, false, result.err
	}
	// The "update" returned by v.store.Put() means:
	// - true: update operation
	// - false: create operation
	// "isUnchanged" means:
	// - true: update operation and file checksum is same as before
	// - false: create operation or update operation with different checksum
	// so it's always false here, at least for now
	isUnchanged = false

	size = uint32(len(n.Data))

	n.AppendAtNs = uint64(time.Now().UnixNano())
	v.lastAppendAtNs = n.AppendAtNs
	if v.lastModifiedTsSeconds < n.LastModified {
		v.lastModifiedTsSeconds = n.LastModified
	}
	return
}

func (v *Volume) deleteNeedle(n *needle.Needle) (uint32, error) {
	glog.V(4).Infof("delete needle %s",
		needle.NewFileIdFromNeedle(v.Id, n).String())
	if v.readOnly {
		return 0, fmt.Errorf("%s is read-only", v.FileName())
	}
	q := query{
		queryType:     deleteQuery,
		needle:        n,
		resultChannel: make(chan queryResult),
	}
	v.queryChannel <- q
	result := <-q.resultChannel
	if result.err != nil {
		return n.Size, result.err
	}
	v.lastModifiedTsSeconds = uint64(time.Now().Unix())
	return n.Size, nil
}

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) readNeedle(n *needle.Needle,
	startOffset int64, length uint64) (int, error) {

	q := query{
		queryType:     getQuery,
		needle:        n,
		resultChannel: make(chan queryResult),
		startOffset:   startOffset,
		length:        length,
	}
	v.queryChannel <- q
	result := <-q.resultChannel
	if result.err != nil {
		return -1, result.err
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
