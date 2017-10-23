package loader

import (
	"archive/zip"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Financial-Times/factset-uploader/factset"
	"github.com/Financial-Times/factset-uploader/rds"
	log "github.com/sirupsen/logrus"
	"fmt"
	"io/ioutil"
)

type Service struct {
	config    Config
	workspace string
	db        *rds.Client
	factset   factset.Servicer
}

func NewService(config Config, db *rds.Client, factset factset.Servicer) *Service {
	return &Service{
		config:    config,
		db:        db,
		factset:   factset,
		workspace: "../",
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

	var lazyErr error

	latestFile, err := s.factset.GetLatestFullFile(pkg)
	if err != nil {
		return err
	}

	localFile, err := s.factset.Download(latestFile)
	if err != nil {
		return err
	}

	filenames, err := s.unzipFile(localFile)
	log.Info(filenames)
	if err != nil {
		return err
	}

	for _, fn := range filenames {
		tableName := getTableFromFilename(fn)
		err = s.db.LoadTable(fn, tableName)
		if lazyErr == nil && err != nil {
			lazyErr = err
			continue
		}
		err = s.db.UpdateLoadedTableVersion(tableName, latestFile.Version)
		if lazyErr == nil && err != nil {
			lazyErr = err
		}
	}

	log.Error(lazyErr)
	return nil
}

func getTableFromFilename(filename string) string {
	return filename[strings.LastIndex(filename, "/")+1 : strings.LastIndex(filename, ".")]
}

func (s *Service) unzipFile(file *os.File) ([]string, error) {

	var filenames []string

	zipReader, err := zip.OpenReader(file.Name())
	if err != nil {
		return []string{}, err
	}
	defer zipReader.Close()

	for _, f := range zipReader.File {

		fpath, _ := filepath.Abs(filepath.Join(s.workspace, f.Name))

		log.Info(fpath)

		err = copyFile(f, fpath)
		if err != nil {
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
func (s *Service) reloadSchema(pkg factset.Package) error {

	err := s.db.DropTablesWithPrefix(pkg.Dataset)
	if err != nil {
		return err
	}

	fsfile, err := s.downloadSchema(pkg)
	if err != nil {
		return err
	}

	zipFile, err := s.factset.Download(*fsfile)

	if err != nil {
		return err
	}

	fileNames, err := s.unzipFile(zipFile)
	if err != nil {
		return err
	}

	for _, file := range fileNames {
		if strings.HasSuffix(file, ".sql") {
			fileContents, err := ioutil.ReadFile(file)
			if err != nil {
				return err
			}


		}
	}

	return nil
}

func (s *Service) downloadSchema(pkg factset.Package) (*factset.FSFile, error){
	pkgVersion, err := s.factset.GetSchemaInfo(pkg)

	if (err != nil) {
		return &factset.FSFile{}, err
	}

	fileName := fmt.Sprintf("%s_%s_schema_%s.zip", pkg.Dataset, pkgVersion.FeedVersion, pkgVersion.Sequence)

	return &factset.FSFile{
		Name: fileName,
		Path: fmt.Sprintf("/datafeeds/documents/docs_%s/%s", pkg.Dataset, fileName),
		IsFull:false,
		Version: *pkgVersion,
	}, nil

}

