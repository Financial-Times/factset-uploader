package loader

import (
	"os"
	"testing"

	"strings"

	"github.com/Financial-Times/factset-uploader/factset"
	"github.com/Financial-Times/factset-uploader/rds"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestServiceDoFullLoad(t *testing.T) {

	factsetService := &MockFactsetService{
		fileList: []factset.FSFile{
			{
				Name: "ppl_test_v1_full_1234.zip",
				Version: factset.PackageVersion{
					FeedVersion: 1,
					Sequence:    1234,
				},
				Path:   "../fixtures/datafeeds/people/ppl_test/ppl_singleZip",
				IsFull: true,
			},
		},
		schemaInfo: factset.PackageVersion{
			FeedVersion: 1,
			Sequence:    12,
		},
	}

	dbClient := createDBClient()
	createPeopleNamesTable(dbClient)
	defer dropTable(dbClient, "ppl_names")

	loader := NewService(Config{}, dbClient, factsetService)

	err := loader.doFullLoad(factset.Package{
		Dataset:     "ppl",
		FSPackage:   "people",
		Product:     "ppl_pickCorrectZip",
		FeedVersion: 1,
	})

	assert.NoError(t, err)

}

type MockFactsetService struct {
	fileList   []factset.FSFile
	schemaInfo factset.PackageVersion
}

func (s *MockFactsetService) GetSchemaInfo(pkg factset.Package) (*factset.PackageVersion, error) {
	return &s.schemaInfo, nil
}

func (s *MockFactsetService) GetLatestFile(pkg factset.Package, isFullLoad bool) (factset.FSFile, error) {

	var latestFile factset.FSFile

	for _, f := range s.fileList {
		latestFile = pickLatestFile(latestFile, f, pkg)
	}
	return latestFile, nil
}

func (s *MockFactsetService) Download(file factset.FSFile) (*os.File, error) {
	wd, _ := os.Getwd()
	log.Info(wd)
	return os.Open("../fixtures/datafeeds/people/ppl_test/ppl_singleZip/" + file.Name)
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
	testDSN := ""

	if os.Getenv("RDS_TEST_DSN") != "" {
		testDSN = os.Getenv("RDS_TEST_DSN")
	}
	//log.Infof("Client: %s %s %s %s", testHost, testUser, testPass, testName)
	dbClient, _ := rds.NewClient(testDSN)
	return dbClient
}

func createPeopleNamesTable(dbClient *rds.Client) {
	query := `CREATE TABLE ppl_names (
		FACTSET_PERSON_ID varchar(100) NOT NULL,
		PEOPLE_NAME_TYPE varchar(45) DEFAULT NULL,
		PEOPLE_NAME_VALUE varchar(255) DEFAULT NULL,
		PRIMARY KEY (FACTSET_PERSON_ID)
	);`

	dbClient.DB.Exec(query)
}

func dropTable(dbClient *rds.Client, tables ...string) {
	if len(tables) == 0 {
		return
	}
	query := "DROP TABLE " + strings.Join(tables, ", ")
	dbClient.DB.Exec(query)
}
