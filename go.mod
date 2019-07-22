module github.com/journeymidnight/seaweedfs

go 1.12

require (
	cloud.google.com/go v0.41.0
	github.com/Azure/azure-storage-blob-go v0.7.0
	github.com/Shopify/sarama v1.23.0
	github.com/aws/aws-sdk-go v1.20.16
	github.com/chrislusf/raft v0.0.0-20190225081310-10d6e2182d92
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/disintegration/imaging v1.6.0
	github.com/dustin/go-humanize v1.0.0
	github.com/gabriel-vasile/mimetype v0.3.14
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-logfmt/logfmt v0.4.0 // indirect
	github.com/go-redis/redis v6.15.2+incompatible
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gocql/gocql v0.0.0-20190708145057-55a38e15c5db
	github.com/golang/protobuf v1.3.1
	github.com/google/btree v1.0.0
	github.com/gorilla/mux v1.7.3
	github.com/jacobsa/daemonize v0.0.0-20160101105449-e460293e890f
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
	github.com/karlseguin/ccache v2.0.3+incompatible
	github.com/klauspost/cpuid v1.2.1 // indirect
	github.com/klauspost/crc32 v1.2.0
	github.com/klauspost/reedsolomon v1.9.2
	github.com/kr/pty v1.1.8 // indirect
	github.com/kurin/blazer v0.5.3
	github.com/lib/pq v1.1.1
	github.com/mattn/go-isatty v0.0.8 // indirect
	github.com/peterh/liner v1.1.0
	github.com/pierrec/lz4 v2.0.5+incompatible // indirect
	github.com/prometheus/client_golang v1.0.0
	github.com/rakyll/statik v0.1.6
	github.com/rwcarlsen/goexif v0.0.0-20190401172101-9e8deecbddbd
	github.com/satori/go.uuid v1.2.0
	github.com/seaweedfs/fuse v0.0.0-20190510212405-310228904eff
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/viper v1.4.0
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/syndtr/goleveldb v1.0.0
	github.com/thesues/cannyls-go v0.0.0-20190531073809-a72639814deb
	github.com/ugorji/go v1.1.7 // indirect
	github.com/willf/bitset v1.1.10 // indirect
	github.com/willf/bloom v2.0.3+incompatible
	gocloud.dev v0.15.0
	gocloud.dev/pubsub/natspubsub v0.15.0
	gocloud.dev/pubsub/rabbitpubsub v0.15.0
	golang.org/x/crypto v0.0.0-20190701094942-4def268fd1a4 // indirect
	golang.org/x/net v0.0.0-20190628185345-da137c7871d7
	golang.org/x/sys v0.0.0-20190626221950-04f50cda93cb // indirect
	golang.org/x/tools v0.0.0-20190708203411-c8855242db9c
	google.golang.org/api v0.7.0
	google.golang.org/grpc v1.22.0
)

replace github.com/thesues/go-judy v0.1.0 => ./go-judy

replace github.com/thesues/cannyls-go => ../cannyls-go // remove after stable
