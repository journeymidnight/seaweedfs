package gcssink

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/storage"
	"github.com/journeymidnight/seaweedfs/weed/filer2"
	"github.com/journeymidnight/seaweedfs/weed/glog"
	"github.com/journeymidnight/seaweedfs/weed/pb/filer_pb"
	"github.com/journeymidnight/seaweedfs/weed/replication/sink"
	"github.com/journeymidnight/seaweedfs/weed/replication/source"
	"github.com/journeymidnight/seaweedfs/weed/util"
	"google.golang.org/api/option"
)

type GcsSink struct {
	client      *storage.Client
	bucket      string
	dir         string
	filerSource *source.FilerSource
}

func init() {
	sink.Sinks = append(sink.Sinks, &GcsSink{})
}

func (g *GcsSink) GetName() string {
	return "google_cloud_storage"
}

func (g *GcsSink) GetSinkToDirectory() string {
	return g.dir
}

func (g *GcsSink) Initialize(configuration util.Configuration) error {
	return g.initialize(
		configuration.GetString("google_application_credentials"),
		configuration.GetString("bucket"),
		configuration.GetString("directory"),
	)
}

func (g *GcsSink) SetSourceFiler(s *source.FilerSource) {
	g.filerSource = s
}

func (g *GcsSink) initialize(google_application_credentials, bucketName, dir string) error {
	g.bucket = bucketName
	g.dir = dir

	ctx := context.Background()
	// Creates a client.
	if google_application_credentials == "" {
		var found bool
		google_application_credentials, found = os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS")
		if !found {
			glog.Fatalf("need to specific GOOGLE_APPLICATION_CREDENTIALS env variable or google_application_credentials in replication.toml")
		}
	}
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(google_application_credentials))
	if err != nil {
		glog.Fatalf("Failed to create client: %v", err)
	}

	g.client = client

	return nil
}

func (g *GcsSink) DeleteEntry(ctx context.Context, key string, isDirectory, deleteIncludeChunks bool) error {

	if isDirectory {
		key = key + "/"
	}

	if err := g.client.Bucket(g.bucket).Object(key).Delete(ctx); err != nil {
		return fmt.Errorf("gcs delete %s%s: %v", g.bucket, key, err)
	}

	return nil

}

func (g *GcsSink) CreateEntry(ctx context.Context, key string, entry *filer_pb.Entry) error {

	if entry.IsDirectory {
		return nil
	}

	totalSize := filer2.TotalSize(entry.Chunks)
	chunkViews := filer2.ViewFromChunks(entry.Chunks, 0, int(totalSize))

	wc := g.client.Bucket(g.bucket).Object(key).NewWriter(ctx)

	for _, chunk := range chunkViews {

		fileUrl, err := g.filerSource.LookupFileId(ctx, chunk.FileId)
		if err != nil {
			return err
		}

		_, err = util.ReadUrlAsStream(fileUrl, chunk.Offset, int(chunk.Size), func(data []byte) {
			wc.Write(data)
		})

		if err != nil {
			return err
		}

	}

	if err := wc.Close(); err != nil {
		return err
	}

	return nil

}

func (g *GcsSink) UpdateEntry(ctx context.Context, key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool) (foundExistingEntry bool, err error) {
	// TODO improve efficiency
	return false, nil
}
