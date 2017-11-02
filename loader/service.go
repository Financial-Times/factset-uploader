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

type Service struct {
	config    Config
	workspace string
	db        *rds.Client
	factset   factset.Servicer
}

func NewService(config Config, db *rds.Client, factset factset.Servicer, workspace string) *Service {
	return &Service{
		config:    config,
		db:        db,
		factset:   factset,
		workspace: workspace,
	}
}

func (s *Service) LoadPackages() {
	err := refreshWorkingDirectory(s.workspace)
	if err == nil {
		for _, v := range s.config.packages {
			err := s.LoadPackage(v)
			if err != nil {
				log.WithFields(log.Fields{"fs_product": v.Product}).Errorf("An error occurred whilst loading product %s; moving on to next package", v.Product)
			}
		}
	}
	return
}

//func refreshWorkingDirectory(workspace string) error {
//	os.RemoveAll(workspace + "/*")
//	if err := os.RemoveAll(workspace); err != nil {
//		log.WithError(err).Errorf("Could not delete directory %s, can not run application", workspace)
//		return err
//	}
//	if err := os.Mkdir(workspace, 0700); err != nil {
//		log.WithError(err).Errorf("Could not create directory %s, can not run application", workspace)
//		return err
//	}
//	return nil
//}

func refreshWorkingDirectory(workspace string) error {
	d, err := os.Open(workspace)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err = os.RemoveAll(filepath.Join(workspace, name)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) LoadPackage(pkg factset.Package) error {
	// Get package metadata
	if err := s.db.LoadMetadataTables(); err != nil {
		return err
	}
	//TODO make custom error instead of sql error
	currentlyLoadedPkgMetadata, currentPackageMetadataErr := s.db.GetPackageMetadata(pkg)
	if currentPackageMetadataErr != nil && currentPackageMetadataErr != sql.ErrNoRows {
		return currentPackageMetadataErr
	}

	schemaVersion, err := s.factset.GetSchemaInfo(pkg)
	if err != nil {
		return err
	}

	var schemaLastUpdated time.Time
	var packageLastUpdate time.Time
	var loadedVersion factset.PackageVersion

	//if schema is out of data, reload schema then do full load
	//this will need to be reworked when delta are handled
	if currentPackageMetadataErr == sql.ErrNoRows || schemaVersion.FeedVersion > currentlyLoadedPkgMetadata.SchemaVersion.FeedVersion ||
		(schemaVersion.FeedVersion == currentlyLoadedPkgMetadata.SchemaVersion.FeedVersion && schemaVersion.Sequence > currentlyLoadedPkgMetadata.SchemaVersion.Sequence) {
		if err = s.reloadSchema(pkg, schemaVersion); err != nil {
			return err
		}

		schemaLastUpdated = time.Now()
		if loadedVersion, err = s.doFullLoad(pkg, currentlyLoadedPkgMetadata); err != nil {
			return err
		}

		packageLastUpdate = time.Now()
	} else {
		// Else do an incremental load. which actually does a full load
		if loadedVersion, err = s.doIncrementalLoad(pkg, currentlyLoadedPkgMetadata); err != nil {
			return err
		}

		schemaLastUpdated = currentlyLoadedPkgMetadata.SchemaLoadedDate
		packageLastUpdate = time.Now()
	}

	//Update existing metadata
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
	} else {
		log.WithFields(log.Fields{"fs_product": pkg.Product}).Infof("Updated product %s to data version v%d_%d", pkg.Product, currentlyLoadedPkgMetadata.PackageVersion.FeedVersion, currentlyLoadedPkgMetadata.PackageVersion.Sequence)
	}
	return nil
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

		localDataArchive, err := s.factset.Download(latestDataArchive, pkg.Product)
		if err != nil {
			return loadedVersions, err
		}

		localDataFiles, err := s.unzipFile(localDataArchive, pkg.Product)
		if err != nil {
			return loadedVersions, err
		}

		for _, file := range localDataFiles {
			tableName := getTableFromFilename(file)
			err = s.db.LoadTable(file, tableName)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{"fs_product": pkg.Product}).Error("Error whilst loading table %s with data from file %s", tableName, file)
				return loadedVersions, err
			}

			err = s.db.UpdateLoadedTableVersion(tableName, latestDataArchive.Version, pkg.Product)
			if err != nil {
				return loadedVersions, err
			}

			loadedVersions = latestDataArchive.Version
			log.WithFields(log.Fields{"fs_product": pkg.Product}).Infof("Updated table %s with data version v%d_%d", tableName, loadedVersions.FeedVersion, loadedVersions.Sequence)
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

//TODO look into this. is it necessary
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

		err = copyFile(f, fpath)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Could not copy %s to %s", file.Name(), s.workspace)
			return []string{}, err
		}
		filenames = append(filenames, fpath)
	}

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
		os.MkdirAll(dest, os.ModePerm)
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
	if err != nil {
		return err
	}
	return nil
}

// Reloading schema:
// Delete all tables with applicable prefix
// Download new schema and unzip
// Run new table creation script - ent_v1_table_generation_statements.sql
func (s *Service) reloadSchema(pkg factset.Package, schemaVersion *factset.PackageVersion) error {
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
				log.WithError(err).WithFields(log.Fields{"fs_product": pkg.Product}).Error("Could not read file: %s", file)
				return err
			}
			err = s.db.CreateTablesFromSchema(fileContents, pkg.Product)
			if err != nil {
				return err
			} else {
				log.WithFields(log.Fields{"fs_product": pkg.Product}).Infof("Updated schema for product %s to version v%d_%d", pkg.Product, schemaVersion.FeedVersion, schemaVersion.Sequence)
			}
		}
	}
	return nil
}

func (s *Service) getSchemaDetails(pkg factset.Package, schemaVersion *factset.PackageVersion) *factset.FSFile {
	fileName := fmt.Sprintf("%s_%s_schema_%s.zip", pkg.Dataset, "v"+strconv.Itoa(schemaVersion.FeedVersion), strconv.Itoa(schemaVersion.Sequence))
	return &factset.FSFile{
		Name:    fileName,
		Path:    fmt.Sprintf("/datafeeds/documents/docs_%s/%s", pkg.Dataset, fileName),
		IsFull:  false,
		Version: *schemaVersion,
	}
}
