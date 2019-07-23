package sub

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/pb/filer_pb"
	"github.com/journeymidnight/seaweedfs/weed/util"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/awssnssqs"
	_ "gocloud.dev/pubsub/azuresb"
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/natspubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"
)

func init() {
	NotificationInputs = append(NotificationInputs, &GoCDKPubSubInput{})
}

type GoCDKPubSubInput struct {
	sub *pubsub.Subscription
}

func (k *GoCDKPubSubInput) GetName() string {
	return "gocdk_pub_sub"
}

func (k *GoCDKPubSubInput) Initialize(config util.Configuration) error {
	subURL := config.GetString("sub_url")
	glog.V(0).Infof("notification.gocdk_pub_sub.sub_url: %v", subURL)
	sub, err := pubsub.OpenSubscription(context.Background(), subURL)
	if err != nil {
		return err
	}
	k.sub = sub
	return nil
}

func (k *GoCDKPubSubInput) ReceiveMessage() (key string, message *filer_pb.EventNotification, err error) {
	msg, err := k.sub.Receive(context.Background())
	key = msg.Metadata["key"]
	message = &filer_pb.EventNotification{}
	err = proto.Unmarshal(msg.Body, message)
	if err != nil {
		return "", nil, err
	}
	return key, message, nil
}
