package loader

import (
	"errors"
	"os"
	"testing"
	"time"

	"strings"

	"github.com/Financial-Times/factset-uploader/factset"
	"github.com/Financial-Times/factset-uploader/rds"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var standardSchema = factset.PackageVersion{
	FeedVersion: 1,
	Sequence:    1,
}

var updatedSequenceSchema = factset.PackageVersion{
	FeedVersion: 1,
	Sequence:    2,
}

var updatedFeedVersionSchema = factset.PackageVersion{
	FeedVersion: 2,
	Sequence:    2,
}

var filesInDirectory = []factset.FSFile{
	{
		Name: "ppl_test_v1_full_1234.zip",
		Version: factset.PackageVersion{
			FeedVersion: 1,
			Sequence:    1234,
		},
		Path:   "/datafeeds/people/ppl_test/ppl_singleZip/ppl_test_v1_full_1234.zip",
		IsFull: true,
	},
}

var stalePackageMetadata = factset.PackageMetadata{
	Package:           standardPkg,
	SchemaVersion:     factset.PackageVersion{FeedVersion: 1, Sequence: 1},
	PackageVersion:    factset.PackageVersion{FeedVersion: 1, Sequence: 1},
	SchemaLoadedDate:  time.Now(),
	PackageLoadedDate: time.Now(),
}
var freshPackageMetadata = factset.PackageMetadata{
	Package:           standardPkg,
	SchemaVersion:     factset.PackageVersion{FeedVersion: 1, Sequence: 1},
	PackageVersion:    factset.PackageVersion{FeedVersion: 1, Sequence: 1250},
	SchemaLoadedDate:  time.Now(),
	PackageLoadedDate: time.Now(),
}

var standardPkg = factset.Package{
	Dataset:     "ppl",
	FSPackage:   "people",
	Product:     "ppl_test",
	Bundle:      "ppl_test",
	FeedVersion: 1,
}

func Test_LoadPackage(t *testing.T) {
	dbClient := createDBClient()
	removeMetadataTables(dbClient)
	defer dbClient.DB.Close()

	testCases := []struct {
		testName                  string
		freshLoad                 bool
		mockIncrementalLoad       bool
		factsetService            factset.Servicer
		pkg                       factset.Package
		existingPackageMetadata   factset.PackageMetadata
		expectedError             error
		expectedSchemaFeedVersion int
		expectedSchemaSequence    int
		expectedPackageSequence   int
	}{
		{
			"Success loading package first time",
			true,
			false,
			getFactsetService(filesInDirectory, standardSchema, nil),
			standardPkg,
			stalePackageMetadata,
			nil,
			1,
			1,
			1234,
		},
		{
			"Success loading updated schema sequence",
			false,
			false,
			getFactsetService(filesInDirectory, updatedSequenceSchema, nil),
			standardPkg,
			stalePackageMetadata,
			nil,
			1,
			2,
			1234,
		},
		{
			"Success loading updated schema feed version",
			false,
			false,
			getFactsetService(filesInDirectory, updatedFeedVersionSchema, nil),
			standardPkg,
			stalePackageMetadata,
			nil,
			2,
			2,
			1234,
		},
		{
			//mock of incremental load as that functionality is not currently covered
			"Success loading new sequence of package with no schema change",
			false,
			true,
			getFactsetService(filesInDirectory, standardSchema, nil),
			standardPkg,
			stalePackageMetadata,
			nil,
			1,
			1,
			1234,
		},
		{
			//mock of incremental load as that functionality is not currently covered
			"Fails when factset service cannot load schema",
			false,
			true,
			getFactsetService(filesInDirectory, standardSchema, errors.New("Could not load schema")),
			standardPkg,
			stalePackageMetadata,
			errors.New("Could not load schema"),
			1,
			1,
			1234,
		},
		{
			//mock of incremental load as that functionality is not currently covered
			"Data File is not loaded when it is out of date",
			false,
			false,
			getFactsetService(filesInDirectory, standardSchema, nil),
			standardPkg,
			freshPackageMetadata,
			nil,
			1,
			1,
			1250,
		},
	}
	for _, d := range testCases {
		t.Run(d.testName, func(t *testing.T) {
			os.Mkdir("../fixtures/tmp", 0700)
			defer os.RemoveAll("../fixtures/tmp")
			defer dropTable(dbClient, "ppl_names")
			defer removeMetadataTables(dbClient)

			if !d.freshLoad {
				err := dbClient.LoadMetadataTables()
				assert.NoError(t, err, "Test %s failed, could not load metadata tables with error: ", d.testName, err)
				err = dbClient.UpdateLoadedPackageVersion(&d.existingPackageMetadata)
				assert.NoError(t, err, "Test %s failed, could not pre load package metadata table with error: ", d.testName, err)
			}

			if d.mockIncrementalLoad {
				err := createPplNamesTable(dbClient)
				assert.NoError(t, err, "Test %s failed, could not load ppl_names table with error: ", d.testName, err)
			}

			loader := NewService(Config{[]factset.Package{d.pkg}}, dbClient, d.factsetService, "../fixtures/tmp")

			err := loader.loadPackage(d.pkg)

			if d.expectedError != nil {
				assert.Errorf(t, err, "Test %s failed, should have resulted in an error", d.testName)
				assert.Contains(t, err.Error(), d.expectedError.Error(), "Test %s failed, returned unexpected error", d.testName)

			} else {
				assert.NoError(t, err)

				pm, err := dbClient.GetPackageMetadata(d.pkg)
				assert.NoError(t, err, "Test %s failed, could not retrieve metadata for package with error: ", d.testName, err)
				assert.Equal(t, d.expectedSchemaFeedVersion, pm.SchemaVersion.FeedVersion, "Test %s failed, schema feed version was not updated", d.testName)
				assert.Equal(t, d.expectedSchemaSequence, pm.SchemaVersion.Sequence, "Test %s failed, schema sequence was not updated", d.testName)
				assert.Equal(t, d.expectedPackageSequence, pm.PackageVersion.Sequence, "Test %s failed, package sequence was not updated", d.testName)
			}
		})
	}
}

func getFactsetService(fileList []factset.FSFile, packageVersion factset.PackageVersion, err error) factset.Servicer {
	return &MockFactsetService{
		fileList:   fileList,
		schemaInfo: packageVersion,
		err:        err,
	}
}

type MockFactsetService struct {
	fileList   []factset.FSFile
	schemaInfo factset.PackageVersion
	err        error
}

func (s *MockFactsetService) GetSchemaInfo(pkg factset.Package) (*factset.PackageVersion, error) {
	return &s.schemaInfo, s.err
}

func (s *MockFactsetService) GetLatestFile(pkg factset.Package, isFullLoad bool) (factset.FSFile, error) {

	var latestFile factset.FSFile

	for _, f := range s.fileList {
		latestFile = pickLatestFile(latestFile, f, pkg)
	}
	return latestFile, nil
}

func (s *MockFactsetService) Download(file factset.FSFile, product string) (*os.File, error) {
	wd, _ := os.Getwd()
	log.Info(wd)
	return os.Open("../fixtures" + file.Path)
}

func pickLatestFile(f1 factset.FSFile, f2 factset.FSFile, pkg factset.Package) factset.FSFile {
	if f1.Version.FeedVersion == pkg.FeedVersion && f2.Version.FeedVersion != pkg.FeedVersion {
		return f1
	}
	if f2.Version.FeedVersion == pkg.FeedVersion && f1.Version.FeedVersion != pkg.FeedVersion {
		return f2
	}
	if f1.Version.FeedVersion == pkg.FeedVersion && f2.Version.FeedVersion == pkg.FeedVersion {
		if f1.Version.Sequence > f2.Version.Sequence {
			return f1
		}
		return f2
	}
	return factset.FSFile{}
}

func createDBClient() *rds.Client {
	var testDSN string

	if os.Getenv("RDS_DSN") != "" {
		testDSN = os.Getenv("RDS_DSN")
	} else {
		testDSN = "root:@/test"
	}

	dbClient, _ := rds.NewClient(testDSN)
	return dbClient
}

func dropTable(dbClient *rds.Client, tables ...string) {
	if len(tables) == 0 {
		return
	}
	query := "DROP TABLE " + strings.Join(tables, ", ")
	dbClient.DB.Exec(query)
}

func removeMetadataTables(dbClient *rds.Client) {
	dbClient.DB.Exec(`DROP TABLE IF EXISTS metadata_package_version, metadata_table_version`)
}

func createPplNamesTable(dbClient *rds.Client) error {
	query := `CREATE TABLE ppl_names (
 		FACTSET_PERSON_ID CHAR(8) NOT NULL,
 		people_name_type VARCHAR(35) NOT NULL,
 		people_name_value VARCHAR(100) NOT NULL,
 		PRIMARY KEY (FACTSET_PERSON_ID, people_name_type, people_name_value));`
	_, err := dbClient.DB.Exec(query)
	return err
}
