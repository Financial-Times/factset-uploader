package factset

import "time"

type PackageVersion struct {
	FeedVersion int
	Sequence    int
}

type Package struct {
	Dataset     string
	FSPackage   string
	Product     string
	FeedVersion int
}

type PackageMetadata struct {
	Package
	SchemaVersion     PackageVersion
	SchemaLoadedDate  time.Time
	PackageVersion    PackageVersion
	PackageLoadedDate time.Time
}

type FSFile struct {
	Name    string
	Path    string
	Version PackageVersion
	IsFull  bool
}
