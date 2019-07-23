package weed_server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/journeymidnight/seaweedfs/weed/operation"
	"github.com/journeymidnight/seaweedfs/weed/pb/master_pb"
	"github.com/journeymidnight/seaweedfs/weed/stats"
	"github.com/journeymidnight/seaweedfs/weed/util"
	"google.golang.org/grpc"

	"github.com/journeymidnight/seaweedfs/weed/filer2"
	_ "github.com/journeymidnight/seaweedfs/weed/filer2/cassandra"
	_ "github.com/journeymidnight/seaweedfs/weed/filer2/leveldb"
	_ "github.com/journeymidnight/seaweedfs/weed/filer2/leveldb2"
	_ "github.com/journeymidnight/seaweedfs/weed/filer2/memdb"
	_ "github.com/journeymidnight/seaweedfs/weed/filer2/mysql"
	_ "github.com/journeymidnight/seaweedfs/weed/filer2/postgres"
	_ "github.com/journeymidnight/seaweedfs/weed/filer2/redis"
	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/notification"
	_ "github.com/journeymidnight/seaweedfs/weed/notification/aws_sqs"
	_ "github.com/journeymidnight/seaweedfs/weed/notification/gocdk_pub_sub"
	_ "github.com/journeymidnight/seaweedfs/weed/notification/google_pub_sub"
	_ "github.com/journeymidnight/seaweedfs/weed/notification/kafka"
	_ "github.com/journeymidnight/seaweedfs/weed/notification/log"
	"github.com/journeymidnight/seaweedfs/weed/security"
	"github.com/spf13/viper"
)

type FilerOption struct {
	Masters            []string
	Collection         string
	DefaultReplication string
	RedirectOnRead     bool
	DisableDirListing  bool
	MaxMB              int
	DirListingLimit    int
	DataCenter         string
	DefaultLevelDbDir  string
	DisableHttp        bool
	Port               int
}

type FilerServer struct {
	option         *FilerOption
	secret         security.SigningKey
	filer          *filer2.Filer
	grpcDialOption grpc.DialOption
}

func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, option *FilerOption) (fs *FilerServer, err error) {

	fs = &FilerServer{
		option:         option,
		grpcDialOption: security.LoadClientTLS(viper.Sub("grpc"), "filer"),
	}

	if len(option.Masters) == 0 {
		glog.Fatal("master list is required!")
	}

	fs.filer = filer2.NewFiler(option.Masters, fs.grpcDialOption)

	go fs.filer.KeepConnectedToMaster()

	v := viper.GetViper()
	if !util.LoadConfiguration("filer", false) {
		v.Set("leveldb2.enabled", true)
		v.Set("leveldb2.dir", option.DefaultLevelDbDir)
		_, err := os.Stat(option.DefaultLevelDbDir)
		if os.IsNotExist(err) {
			os.MkdirAll(option.DefaultLevelDbDir, 0755)
		}
	}
	util.LoadConfiguration("notification", false)

	fs.filer.LoadConfiguration(v)

	notification.LoadConfiguration(v.Sub("notification"))

	handleStaticResources(defaultMux)
	if !option.DisableHttp {
		defaultMux.HandleFunc("/", fs.filerHandler)
	}
	if defaultMux != readonlyMux {
		readonlyMux.HandleFunc("/", fs.readonlyFilerHandler)
	}

	maybeStartMetrics(fs, option)

	return fs, nil
}

func maybeStartMetrics(fs *FilerServer, option *FilerOption) {
	isConnected := false
	var metricsAddress string
	var metricsIntervalSec int
	var readErr error
	for !isConnected {
		metricsAddress, metricsIntervalSec, readErr = readFilerConfiguration(fs.grpcDialOption, option.Masters[0])
		if readErr == nil {
			isConnected = true
		} else {
			time.Sleep(7 * time.Second)
		}
	}
	if metricsAddress == "" && metricsIntervalSec <= 0 {
		return
	}
	go stats.LoopPushingMetric("filer", stats.SourceName(option.Port), stats.FilerGather,
		func() (addr string, intervalSeconds int) {
			return metricsAddress, metricsIntervalSec
		})
}

func readFilerConfiguration(grpcDialOption grpc.DialOption, masterGrpcAddress string) (metricsAddress string, metricsIntervalSec int, err error) {
	err = operation.WithMasterServerClient(masterGrpcAddress, grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
		resp, err := masterClient.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		if err != nil {
			return fmt.Errorf("get master %s configuration: %v", masterGrpcAddress, err)
		}
		metricsAddress, metricsIntervalSec = resp.MetricsAddress, int(resp.MetricsIntervalSeconds)
		return nil
	})
	return
}
