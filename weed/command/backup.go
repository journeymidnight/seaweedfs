package command

var (
	s BackupOptions
)

type BackupOptions struct {
	master     *string
	collection *string
	dir        *string
	volumeId   *int
}

func init() {
	cmdBackup.Run = runBackup // break init cycle
	s.master = cmdBackup.Flag.String("server", "localhost:9333", "SeaweedFS master location")
	s.collection = cmdBackup.Flag.String("collection", "", "collection name")
	s.dir = cmdBackup.Flag.String("dir", ".", "directory to store volume data files")
	s.volumeId = cmdBackup.Flag.Int("volumeId", -1, "a volume id. The volume .dat and .idx files should already exist in the dir.")
}

var cmdBackup = &Command{
	UsageLine: "backup -dir=. -volumeId=234 -server=localhost:9333",
	Short:     "incrementally backup a volume to local folder",
	Long: `Incrementally backup volume data.

	It is expected that you use this inside a script, to loop through
	all possible volume ids that needs to be backup to local folder.

	The volume id does not need to exist locally or even remotely.
	This will help to backup future new volumes.

	Usually backing up is just copying the .dat (and .idx) files.
	But it's tricky to incrementally copy the differences.

	The complexity comes when there are multiple addition, deletion and compaction.
	This tool will handle them correctly and efficiently, avoiding unnecessary data transportation.
  `,
}

func runBackup(cmd *Command, args []string) bool {
	// FIXME
	panic("not implemented yet")
}
