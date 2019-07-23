package sub

import (
	"github.com/journeymidnight/seaweedfs/weed/pb/filer_pb"
	"github.com/journeymidnight/seaweedfs/weed/util"
)

type NotificationInput interface {
	// GetName gets the name to locate the configuration in sync.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration) error
	ReceiveMessage() (key string, message *filer_pb.EventNotification, err error)
}

var (
	NotificationInputs []NotificationInput
)
