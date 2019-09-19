package weed_server

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"time"

	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/operation"
	"github.com/journeymidnight/seaweedfs/weed/pb/volume_server_pb"
	"github.com/journeymidnight/seaweedfs/weed/storage"
	"github.com/journeymidnight/seaweedfs/weed/storage/erasure_coding"
	"github.com/journeymidnight/seaweedfs/weed/storage/needle"
	"github.com/journeymidnight/seaweedfs/weed/util"
)

const BufferSizeLimit = 1024 * 1024 * 2

// VolumeCopy copy the .meta .lusf files, and mount the volume
func (vs *VolumeServer) VolumeCopy(ctx context.Context,
	req *volume_server_pb.VolumeCopyRequest) (*volume_server_pb.VolumeCopyResponse, error) {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v != nil {
		return nil, fmt.Errorf("volume %d already exists", req.VolumeId)
	}

	location := vs.store.FindFreeLocation()
	if location == nil {
		return nil, fmt.Errorf("no space left")
	}

	// the master will not start compaction for read-only volumes,
	// so it is safe to just copy files directly
	// copy .lusf and .meta files
	//   read .meta .lusf file size and timestamp
	//   send .meta file
	//   send .lusf file
	//   confirm size and timestamp
	var volFileInfoResp *volume_server_pb.ReadVolumeFileStatusResponse
	var volumeFileName, idxFileName, datFileName string
	err := operation.WithVolumeServerClient(req.SourceDataNode, vs.grpcDialOption,
		func(client volume_server_pb.VolumeServerClient) error {
		var err error
		volFileInfoResp, err = client.ReadVolumeFileStatus(ctx,
			&volume_server_pb.ReadVolumeFileStatusRequest{
				VolumeId: req.VolumeId,
			})
		if nil != err {
			return fmt.Errorf("read volume file status failed, %v", err)
		}

		volumeFileName = storage.VolumeFileName(location.Directory,
			volFileInfoResp.Collection, int(req.VolumeId))

		// println("source:", volFileInfoResp.String())
		// copy ecx file
		if err := vs.doCopyFile(ctx, client, false, req.Collection,
			req.VolumeId, volFileInfoResp.CompactionRevision,
			volFileInfoResp.IdxFileSize, volumeFileName,
			".meta", false); err != nil {
			return err
		}

		if err := vs.doCopyFile(ctx, client, false, req.Collection,
			req.VolumeId, volFileInfoResp.CompactionRevision,
			volFileInfoResp.DatFileSize, volumeFileName,
			"", false); err != nil {
			return err
		}

		return nil
	})

	idxFileName = volumeFileName + ".meta"
	datFileName = volumeFileName

	if err != nil && volumeFileName != "" {
		if idxFileName != "" {
			os.Remove(idxFileName)
		}
		if datFileName != "" {
			os.Remove(datFileName)
		}
		return nil, err
	}

	if err = checkCopyFiles(volFileInfoResp, idxFileName, datFileName); err != nil { // added by panyc16
		return nil, err
	}

	// mount the volume
	err = vs.store.MountVolume(needle.VolumeId(req.VolumeId))
	if err != nil {
		return nil, fmt.Errorf("failed to mount volume %d: %v", req.VolumeId, err)
	}

	return &volume_server_pb.VolumeCopyResponse{
		LastAppendAtNs: volFileInfoResp.DatFileTimestampSeconds * uint64(time.Second),
	}, err
}

func (vs *VolumeServer) doCopyFile(ctx context.Context, client volume_server_pb.VolumeServerClient, isEcVolume bool, collection string, vid uint32,
	compactRevision uint32, stopOffset uint64, baseFileName, ext string, isAppend bool) error {

	copyFileClient, err := client.CopyFile(ctx, &volume_server_pb.CopyFileRequest{
		VolumeId:           vid,
		Ext:                ext,
		CompactionRevision: compactRevision,
		StopOffset:         stopOffset,
		Collection:         collection,
		IsEcVolume:         isEcVolume,
	})
	if err != nil {
		return fmt.Errorf("failed to start copying volume %d %s file: %v", vid, ext, err)
	}

	err = writeToFile(copyFileClient, baseFileName+ext, util.NewWriteThrottler(vs.compactionBytePerSecond), isAppend)
	if err != nil {
		return fmt.Errorf("failed to copy %s file: %v", baseFileName+ext, err)
	}

	return nil

}

/**
only check the the differ of the file size
todo: maybe should check the received count and deleted count of the volume
*/
func checkCopyFiles(originFileInf *volume_server_pb.ReadVolumeFileStatusResponse, idxFileName, datFileName string) error {
	stat, err := os.Stat(idxFileName)
	if err != nil {
		return fmt.Errorf("stat idx file %s failed, %v", idxFileName, err)
	}
	if originFileInf.IdxFileSize != uint64(stat.Size()) {
		return fmt.Errorf("idx file %s size [%v] is not same as origin file size [%v]",
			idxFileName, stat.Size(), originFileInf.IdxFileSize)
	}

	stat, err = os.Stat(datFileName)
	if err != nil {
		return fmt.Errorf("get dat file info failed, %v", err)
	}
	if originFileInf.DatFileSize != uint64(stat.Size()) {
		return fmt.Errorf("the dat file size [%v] is not same as origin file size [%v]",
			stat.Size(), originFileInf.DatFileSize)
	}
	return nil
}

func writeToFile(client volume_server_pb.VolumeServer_CopyFileClient, fileName string, wt *util.WriteThrottler, isAppend bool) error {
	glog.V(4).Infof("writing to %s", fileName)
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if isAppend {
		flags = os.O_WRONLY | os.O_CREATE
	}
	dst, err := os.OpenFile(fileName, flags, 0644)
	if err != nil {
		return nil
	}
	defer dst.Close()

	for {
		resp, receiveErr := client.Recv()
		if receiveErr == io.EOF {
			break
		}
		if receiveErr != nil {
			return fmt.Errorf("receiving %s: %v", fileName, receiveErr)
		}
		dst.Write(resp.FileContent)
		wt.MaybeSlowdown(int64(len(resp.FileContent)))
	}
	return nil
}

func (vs *VolumeServer) ReadVolumeFileStatus(ctx context.Context, req *volume_server_pb.ReadVolumeFileStatusRequest) (*volume_server_pb.ReadVolumeFileStatusResponse, error) {
	resp := &volume_server_pb.ReadVolumeFileStatusResponse{}
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	resp.VolumeId = req.VolumeId
	datSize, idxSize, modTime := v.FileStat()
	resp.DatFileSize = datSize
	resp.IdxFileSize = idxSize
	resp.DatFileTimestampSeconds = uint64(modTime.Unix())
	resp.IdxFileTimestampSeconds = uint64(modTime.Unix())
	resp.FileCount = v.FileCount()
	resp.CompactionRevision = uint32(v.CompactionRevision)
	resp.Collection = v.Collection
	return resp, nil
}

// CopyFile client pulls the volume related file from the source server.
// if req.CompactionRevision != math.MaxUint32, it ensures the compact revision is as expected
// The copying still stop at req.StopOffset, but you can set it to math.MaxUint64 in order to read all data.
func (vs *VolumeServer) CopyFile(req *volume_server_pb.CopyFileRequest, stream volume_server_pb.VolumeServer_CopyFileServer) error {

	var fileName string
	if !req.IsEcVolume {
		v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
		if v == nil {
			return fmt.Errorf("not found volume id %d", req.VolumeId)
		}

		if uint32(v.CompactionRevision) != req.CompactionRevision && req.CompactionRevision != math.MaxUint32 {
			return fmt.Errorf("volume %d is compacted", req.VolumeId)
		}
		fileName = v.FileName() + req.Ext
	} else {
		baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId)) + req.Ext
		for _, location := range vs.store.Locations {
			tName := path.Join(location.Directory, baseFileName)
			if util.FileExists(tName) {
				fileName = tName
			}
		}
		if fileName == "" {
			return fmt.Errorf("CopyFile not found ec volume id %d", req.VolumeId)
		}
	}

	bytesToRead := int64(req.StopOffset)

	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := make([]byte, BufferSizeLimit)

	for bytesToRead > 0 {
		bytesread, err := file.Read(buffer)

		// println(fileName, "read", bytesread, "bytes, with target", bytesToRead)

		if err != nil {
			if err != io.EOF {
				return err
			}
			// println(fileName, "read", bytesread, "bytes, with target", bytesToRead, "err", err.Error())
			break
		}

		if int64(bytesread) > bytesToRead {
			bytesread = int(bytesToRead)
		}
		err = stream.Send(&volume_server_pb.CopyFileResponse{
			FileContent: buffer[:bytesread],
		})
		if err != nil {
			// println("sending", bytesread, "bytes err", err.Error())
			return err
		}

		bytesToRead -= int64(bytesread)

	}

	return nil
}

func (vs *VolumeServer) findVolumeOrEcVolumeLocation(volumeId needle.VolumeId) {

}
