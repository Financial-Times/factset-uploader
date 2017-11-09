package loader

import (
	"archive/zip"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"strings"

	"fmt"
	"io/ioutil"

	"strconv"
	"time"

	"github.com/Financial-Times/factset-uploader/factset"
	"github.com/Financial-Times/factset-uploader/rds"
	log "github.com/sirupsen/logrus"
)

// Service - Logic for loading the data files.
type Service struct {
	config    Config
	workspace string
	db        *rds.Client
	factset   factset.Servicer
}

// NewService - Creates a new loader.Service
func NewService(config Config, db *rds.Client, factset factset.Servicer, workspace string) *Service {
	return &Service{
		config:    config,
		db:        db,
		factset:   factset,
		workspace: workspace,
	}
}

// LoadPackages - Load all packages listed in the config
func (s *Service) LoadPackages() {
	//Make sure working directory is clean prior to run
	err := refreshWorkingDirectory(s.workspace)
	if err != nil {
		log.WithError(err).Errorf("Could not clean up working directory %s prior package load", s.workspace)
		return
	}

	for _, v := range s.config.packages {
		err = s.loadPackage(v)
		if err != nil {
			log.WithFields(log.Fields{"fs_product": v.Product}).Errorf("An error occurred whilst loading product %s; moving on to next package", v.Product)
		}
	}

	//Re clean directory after final package has been loaded
	err = refreshWorkingDirectory(s.workspace)
	if err != nil {
		log.WithError(err).Errorf("Could not clean up working directory %s after loading packages", s.workspace)
		return
	}
}

func refreshWorkingDirectory(workspace string) error {
	log.WithFields(log.Fields{"workspace": workspace}).Info("Refreshing the workspace")
	d, err := os.Open(workspace)
	if err != nil {
		log.WithError(err).Fatalf("Could not open directory %s, can not run application", workspace)
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		log.WithError(err).Fatalf("Could not read directory %s, can not run application", workspace)
		return err
	}
	for _, name := range names {
		log.WithFields(log.Fields{"workspace": workspace, "file": name}).Debug("Removing file")
		if err = os.RemoveAll(filepath.Join(workspace, name)); err != nil {
			log.WithError(err).Fatalf("Could not clear down directory %s, can not run application", workspace)
			return err
		}
	}
	return nil
}

func (s *Service) loadPackage(pkg factset.Package) error {
	log.WithFields(log.Fields{"fs_product": pkg.Product}).Infof("Processing %s package", pkg.Product)
	// Get package metadata
	if err := s.db.LoadMetadataTables(); err != nil {
		return err
	}
	//TODO make custom error instead of sql error
	log.WithFields(log.Fields{"fs_product": pkg.Product}).Debugf("Querying db for current metadata for package: %s", pkg.Product)
	currentlyLoadedPkgMetadata, currentPackageMetadataErr := s.db.GetPackageMetadata(pkg)
	if currentPackageMetadataErr != nil && currentPackageMetadataErr != sql.ErrNoRows {
		return currentPackageMetadataErr
	}

	log.WithFields(log.Fields{"fs_product": pkg.Product}).Debugf("Searching factset for most recent package: %s", pkg.Product)
	schemaVersion, err := s.factset.GetSchemaInfo(pkg)
	if err != nil {
		return err
	}

	var schemaLastUpdated time.Time
	var packageLastUpdate time.Time
	var loadedVersion factset.PackageVersion

	// If schema is out of data, reload schema then do full load
	// This will need to be reworked when delta are handled
	if isSchemaOutOfDate(schemaVersion, currentlyLoadedPkgMetadata) {
		log.WithFields(log.Fields{"fs_product": pkg.Product}).Debugf("Schema is out of date")
		if err = s.reloadSchema(pkg, schemaVersion); err != nil {
			return err
		}

		schemaLastUpdated = time.Now()
		if loadedVersion, err = s.doFullLoad(pkg, currentlyLoadedPkgMetadata); err != nil {
			return err
		}

		packageLastUpdate = time.Now()
	} else {
		log.WithFields(log.Fields{"fs_product": pkg.Product}).Debugf("Schema is up to date")
		// Else do an incremental load. which actually does a full load
		if loadedVersion, err = s.doIncrementalLoad(pkg, currentlyLoadedPkgMetadata); err != nil {
			return err
		}
		schemaLastUpdated = currentlyLoadedPkgMetadata.SchemaLoadedDate
		packageLastUpdate = time.Now()
	}

	// Update existing metadata
	updatedPackageMetadata := &factset.PackageMetadata{
		Package: pkg,
		SchemaVersion: factset.PackageVersion{
			FeedVersion: schemaVersion.FeedVersion,
			Sequence:    schemaVersion.Sequence,
		},
		SchemaLoadedDate: schemaLastUpdated,
		PackageVersion: factset.PackageVersion{
			FeedVersion: loadedVersion.FeedVersion,
			Sequence:    loadedVersion.Sequence,
		},
		PackageLoadedDate: packageLastUpdate,
	}

	if err := s.db.UpdateLoadedPackageVersion(updatedPackageMetadata); err != nil {
		return err
	}
	log.WithFields(log.Fields{"fs_product": pkg.Product}).Infof("Updated product %s to data version v%d_%d", pkg.Product, updatedPackageMetadata.PackageVersion.FeedVersion, updatedPackageMetadata.PackageVersion.Sequence)
	return nil
}

func isSchemaOutOfDate(latestSchema *factset.PackageVersion, loadedSchema factset.PackageMetadata) bool {
	return latestSchema.FeedVersion > loadedSchema.SchemaVersion.FeedVersion ||
		(latestSchema.FeedVersion == loadedSchema.SchemaVersion.FeedVersion && latestSchema.Sequence > loadedSchema.SchemaVersion.Sequence)
}

// Incremental load:
// Get all incremental files after loaded version.
// In order
//      Download and unzip
//      Load updates into table
//      Process delete files
// Update table metadata
// Clean up and update package metadata.
func (s *Service) doIncrementalLoad(pkg factset.Package, currentPackageMetadata factset.PackageMetadata) (factset.PackageVersion, error) {
	// TODO: Actually do an incremental load as described above.
	return s.doFullLoad(pkg, currentPackageMetadata)
}

// Full load:
// Get most recent full file.
// Download and unzip.
// For each file, load into table.
// Update metadata with new version.
// Clean up and update package metadata.
func (s *Service) doFullLoad(pkg factset.Package, currentLoadedFileMetadata factset.PackageMetadata) (factset.PackageVersion, error) {
	var loadedVersions factset.PackageVersion

	latestDataArchive, err := s.factset.GetLatestFile(pkg, true)
	if err != nil {
		return loadedVersions, err
	}

	if currentLoadedFileMetadata.PackageVersion.FeedVersion == 0 ||
		(currentLoadedFileMetadata.PackageVersion.FeedVersion == latestDataArchive.Version.FeedVersion && currentLoadedFileMetadata.PackageVersion.Sequence < latestDataArchive.Version.Sequence) {

		//if err = s.db.DropTablesWithDataset(pkg.Dataset, pkg.Product); err != nil {
		//	return loadedVersions, err
		//}

		var localDataArchive *os.File
		localDataArchive, err = s.factset.Download(latestDataArchive, pkg.Product)
		if err != nil {
			return loadedVersions, err
		}

		var localDataFiles []string
		localDataFiles, err = s.unzipFile(localDataArchive, pkg.Product)
		if err != nil {
			return loadedVersions, err
		}

		for _, file := range localDataFiles {
			//TODO version the file name to be table_sequence
			tableName := getTableFromFilename(file)
			if err = s.db.DropDataFromTable(tableName, pkg.Product); err != nil {
				return loadedVersions, err
			}

			err = s.db.LoadTable(file, tableName)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{"fs_product": pkg.Product}).Errorf("Error whilst loading table %s with data from file %s", tableName, file)
				return loadedVersions, err
			}

			err = s.db.UpdateLoadedTableVersion(tableName, latestDataArchive.Version, pkg.Product)
			if err != nil {
				return loadedVersions, err
			}

			loadedVersions.FeedVersion = latestDataArchive.Version.FeedVersion
			loadedVersions.Sequence = latestDataArchive.Version.Sequence
			log.WithFields(log.Fields{"fs_product": pkg.Product}).Infof("Updated table %s with data version v%d_%d", tableName, latestDataArchive.Version.FeedVersion, latestDataArchive.Version.Sequence)
		}
		// Update the package metadata, has the schema changed though?
	} else {
		log.Infof("%s data is up-to-date as version v%d_%d has already been loaded into db", pkg.Product, currentLoadedFileMetadata.PackageVersion.FeedVersion, currentLoadedFileMetadata.PackageVersion.Sequence)
		loadedVersions = currentLoadedFileMetadata.PackageVersion
	}

	return loadedVersions, err
}

func getTableFromFilename(filename string) string {
	return filename[strings.LastIndex(filename, "/")+1 : strings.LastIndex(filename, ".")]
}

func (s *Service) unzipFile(file *os.File, product string) ([]string, error) {
	var filenames []string

	zipReader, err := zip.OpenReader(file.Name())
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Could not open archive: %s", file.Name())
		return []string{}, err
	}
	defer zipReader.Close()

	for _, f := range zipReader.File {
		fpath, _ := filepath.Abs(filepath.Join(s.workspace, f.Name))

		if err := copyFile(f, fpath); err != nil {
			log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Could not copy %s to %s", file.Name(), s.workspace)
			return []string{}, err
		}
		filenames = append(filenames, fpath)
	}

	log.WithFields(log.Fields{"fs_product": product}).Debugf("Unzipped archive %s into %s", file.Name(), s.workspace)
	return filenames, nil
}

func copyFile(srcFile *zip.File, dest string) error {
	rc, err := srcFile.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	f, err := os.OpenFile(
		dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer f.Close()

	if srcFile.FileInfo().IsDir() {
		err = os.MkdirAll(dest, os.ModePerm)
		if err != nil {
			return err
		}
	} else {
		var fdir string
		if lastIndex := strings.LastIndex(dest, string(os.PathSeparator)); lastIndex > -1 {
			fdir = dest[:lastIndex]
		}

		err = os.MkdirAll(fdir, os.ModePerm)
		if err != nil {
			log.Fatal(err)
			return err
		}
	}

	_, err = io.Copy(f, rc)
	return err
}

// Reloading schema:
// Delete all tables with applicable prefix
// Download new schema and unzip
// Run new table creation script - ent_v1_table_generation_statements.sql
func (s *Service) reloadSchema(pkg factset.Package, schemaVersion *factset.PackageVersion) error {
	log.WithFields(log.Fields{"fs_product": pkg.Product}).Debugf("Reloading schema for package: %s", pkg.Product)
	if err := s.db.DropTablesWithDataset(pkg.Dataset, pkg.Product); err != nil {
		return err
	}
	schemaFileDetails := s.getSchemaDetails(pkg, schemaVersion)
	schemaFileArchive, err := s.factset.Download(*schemaFileDetails, pkg.Product)
	if err != nil {
		return err
	}

	schemaFiles, err := s.unzipFile(schemaFileArchive, pkg.Product)
	if err != nil {
		return err
	}

	for _, file := range schemaFiles {
		if strings.HasSuffix(file, ".sql") {
			fileContents, err := ioutil.ReadFile(file)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{"fs_product": pkg.Product}).Errorf("Could not read file: %s", file)
				return err
			}
			err = s.db.CreateTablesFromSchema(fileContents, pkg.Product)
			if err != nil {
				return err
			}
			log.WithFields(log.Fields{"fs_product": pkg.Product}).Infof("Updated schema for product %s to version v%d_%d", pkg.Product, schemaVersion.FeedVersion, schemaVersion.Sequence)
		}
	}
	return nil
}

func (s *Service) getSchemaDetails(pkg factset.Package, schemaVersion *factset.PackageVersion) *factset.FSFile {
	fileName := fmt.Sprintf("%s_%s_schema_%s.zip", pkg.Dataset, "v"+strconv.Itoa(schemaVersion.FeedVersion), strconv.Itoa(schemaVersion.Sequence))
	log.WithFields(log.Fields{"fs_product": pkg.Product}).Infof("Most recent schema for %s is %s", pkg.Product, fileName)
	return &factset.FSFile{
		Name:    fileName,
		Path:    fmt.Sprintf("/datafeeds/documents/docs_%s/%s", pkg.Dataset, fileName),
		IsFull:  false,
		Version: *schemaVersion,
	}
}
