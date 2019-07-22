// +build windows openbsd netbsd plan9 solaris

package stats

import "github.com/journeymidnight/seaweedfs/weed/pb/volume_server_pb"

func fillInDiskStatus(status *volume_server_pb.DiskStatus) {
	return
}
