package loader

import (
	"database/sql"

	"github.com/Financial-Times/factset-uploader/factset"
	"github.com/Financial-Times/factset-uploader/rds"
)

type Service struct {
	config  config
	db      *rds.Client
	factset *factset.Service
}

func NewService(config config, db *rds.Client, factset *factset.Service) *Service {
	return &Service{
		config:  config,
		db:      db,
		factset: factset,
	}
}

func (s *Service) LoadPackages() {
	for _, v := range s.config.packages {
		s.LoadPackage(v)
	}
}

func (s *Service) LoadPackage(pkg factset.Package) error {
	// Get package metadata

	pkgMetadata, err := s.db.GetPackageMetadata(pkg)
	if err != sql.ErrNoRows {
		// If no metadata, load schema and do full load.
		s.reloadSchema(pkg)
		return s.doFullLoad(pkg)
	} else if err != nil {
		return err
	}

	schemaVersion, err := s.factset.GetSchemaInfo(pkg)
	if err != nil {
		return err
	}

	if schemaVersion.FeedVersion > pkgMetadata.SchemaVersion.FeedVersion ||
		(schemaVersion.FeedVersion == pkgMetadata.SchemaVersion.FeedVersion && schemaVersion.Sequence > pkgMetadata.SchemaVersion.Sequence) {
		// If schema out of date, reload schema and do full load.
		s.reloadSchema(pkg)
		return s.doFullLoad(pkg)
	}

	// Else do an incremental load.
	return s.doIncrementalLoad(pkg)

}

// Incremental load:
// Get all incremental files after loaded version.
// In order
//      Download and unzip
//      Load updates into table
//      Process delete files
// Update table metadata
// Clean up and update package metadata.
func (s *Service) doIncrementalLoad(pkg factset.Package) error {
	// TODO: Actually do an incremental load as described above.
	return s.doFullLoad(pkg)
}

// Full load:
// Get most recent full file.
// Download and unzip.
// For each file, load into table.
// Update metadata with new version.
// Clean up and update package metadata.
func (s *Service) doFullLoad(pkg factset.Package) error {
	return nil
}

// Reloading schema:
// Delete all tables with applicable prefix
// Download new schema and unzip
// Run new table creation script.
func (s *Service) reloadSchema(pkg factset.Package) error {

	err := s.db.DropTablesWithPrefix(pkg.Dataset)
	if err != nil {
		return err
	}
	return nil
}
