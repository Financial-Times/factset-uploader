package factset

import "time"

type PackageVersion struct {
	FeedVersion int
	Sequence    int
}

// ppl              ent                     Dataset
// people           entity                  FSPackage
// ppl_premium      ent_entity_advanced     Product
// v1				v1						FeedVersion

//   /datafeeds/people/ppl_premium/ppl_premium_v1_full_1234.zip
//   /datafeeds/people/ppl_premium/ppl_premium_v1_1234.zip
//   /datafeeds/entity/ent_entity_advanced/ent_entity_advanced_v1_full_1234.zip
//   /datafeeds/documents/docs_ppl
//   /datafeeds/documents/docs_ppl/ppl_v1_schema_12.zip
//   /datafeeds/edm/edm_premium/edm_premium_full_1972.zip
//   /datafeeds/edm/edm_premium/edm_premium_1973.zip

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
