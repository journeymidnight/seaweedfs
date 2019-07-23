package erasure_coding

import (
	"fmt"
	"io"
	"os"

	"github.com/journeymidnight/seaweedfs/weed/storage/types"
	"github.com/journeymidnight/seaweedfs/weed/util"
)

var (
	markNeedleDeleted = func(file *os.File, offset int64) error {
		b := make([]byte, types.SizeSize)
		util.Uint32toBytes(b, types.TombstoneFileSize)
		n, err := file.WriteAt(b, offset+types.NeedleIdSize+types.OffsetSize)
		if err != nil {
			return fmt.Errorf("ecx write error: %v", err)
		}
		if n != types.SizeSize {
			return fmt.Errorf("ecx written %d bytes, expecting %d", n, types.SizeSize)
		}
		return nil
	}
)

func (ev *EcVolume) DeleteNeedleFromEcx(needleId types.NeedleId) (err error) {

	_, _, err = searchNeedleFromEcx(ev.ecxFile, ev.ecxFileSize, needleId, markNeedleDeleted)

	if err != nil {
		if err == NotFoundError {
			return nil
		}
		return err
	}

	b := make([]byte, types.NeedleIdSize)
	types.NeedleIdToBytes(b, needleId)

	ev.ecjFileAccessLock.Lock()

	ev.ecjFile.Seek(0, io.SeekEnd)
	ev.ecjFile.Write(b)

	ev.ecjFileAccessLock.Unlock()

	return
}

func RebuildEcxFile(baseFileName string) error {

	if !util.FileExists(baseFileName + ".ecj") {
		return nil
	}

	ecxFile, err := os.OpenFile(baseFileName+".ecx", os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("rebuild: failed to open ecx file: %v", err)
	}
	defer ecxFile.Close()

	fstat, err := ecxFile.Stat()
	if err != nil {
		return err
	}

	ecxFileSize := fstat.Size()

	ecjFile, err := os.OpenFile(baseFileName+".ecj", os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("rebuild: failed to open ecj file: %v", err)
	}

	buf := make([]byte, types.NeedleIdSize)
	for {
		n, _ := ecjFile.Read(buf)
		if n != types.NeedleIdSize {
			break
		}

		needleId := types.BytesToNeedleId(buf)

		_, _, err = searchNeedleFromEcx(ecxFile, ecxFileSize, needleId, markNeedleDeleted)

		if err != nil && err != NotFoundError {
			ecxFile.Close()
			return err
		}

	}

	ecxFile.Close()

	os.Remove(baseFileName + ".ecj")

	return nil
}
