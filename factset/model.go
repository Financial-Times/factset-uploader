package factset

import "time"

const dataFolder = "data"
const weekly = "weekly"
const daily = "daily"

type factsetResource struct {
	archive   string
	fileNames string
}

type s3Config struct {
	accKey    string
	secretKey string
	bucket    string
	domain    string
}

type sftpConfig struct {
	address  string
	port     int
	username string
	key      string
}

type zipCollection struct {
	archive      string
	filesToWrite []string
}

type PackageVersion struct {
	FeedVersion int
	Sequence    int
}

type Package struct {
	Dataset   string
	FSPackage string
	Product   string
}

type PackageMetadata struct {
	Package
	SchemaVersion     PackageVersion
	SchemaLoadedDate  time.Time
	PackageVersion    PackageVersion
	PackageLoadedDate time.Time
}
