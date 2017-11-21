package factset

import "time"

// PackageVersion - Factset package versioning is two parts
type PackageVersion struct {
	FeedVersion int
	Sequence    int
}

// Example file naming and package breakdown
// ppl              	ent                     Dataset
// people           	entity                  FSPackage
// ppl_premium      	ent_entity_advanced     Product
// ff_advanced_der_ap	ppl_premium				Bundle
// v1					v1						FeedVersion
// /datafeeds/people/ppl_premium/ppl_premium_v1_full_1234.zip
// /datafeeds/people/ppl_premium/ppl_premium_v1_1234.zip
// /datafeeds/entity/ent_entity_advanced/ent_entity_advanced_v1_full_1234.zip
// /datafeeds/fundamentals/ff_advanced_ap_v3/ff_advanced_der_ap_v3_full_1234.zip
// /datafeeds/documents/docs_ppl
// /datafeeds/documents/docs_ppl/ppl_v1_schema_12.zip
// /datafeeds/edm/edm_premium/edm_premium_full_1972.zip
// /datafeeds/edm/edm_premium/edm_premium_1973.zip

// Package - represents a package from Factset
type Package struct {
	Dataset     string
	FSPackage   string
	Product     string
	Bundle      string
	FeedVersion int
}

// PackageMetadata - extended package including versioning information
type PackageMetadata struct {
	Package
	SchemaVersion     PackageVersion
	SchemaLoadedDate  time.Time
	PackageVersion    PackageVersion
	PackageLoadedDate time.Time
}

// FSFile - representation of a file on the Factset server
type FSFile struct {
	Name    string
	Path    string
	Version PackageVersion
	IsFull  bool
}
