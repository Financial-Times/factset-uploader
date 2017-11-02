package rds

import (
	"os"

	_ "github.com/joho/godotenv/autoload"
	"github.com/golang/go/src/pkg/go/doc/testdata"
	"github.com/Financial-Times/factset-uploader/factset"
)

var dbClient *Client

func init() {
	var dsn string

	if os.Getenv("RDS_DSN") != "" {
		dsn = os.Getenv("RDS_DSN")
	} else {
		dsn = "root:@/test"
	}

	dbClient, _ = NewClient(dsn)
}

//func TestClientGetLoadedVersionNoTable(t *testing.T) {
//	defer dropTestTables()
//	defer removeMetadataTables()
//
//	hasMetadata, err := dbClient.VerifyMetadata()
//	assert.NoError(t, err)
//	assert.False(t, hasMetadata)
//
//	version, err := dbClient.GetLoadedVersion("testTable")
//	assert.Error(t, err)
//	assert.EqualValues(t, factset.PackageVersion{}, version, "Table version is not an empty object")
//}
//
//func TestClientUpdateAndGetLoadedVersion(t *testing.T) {
//	defer dropTestTables()
//	defer removeMetadataTables()
//	err := dbClient.LoadMetadataTables()
//	assert.NoError(t, err)
//
//	err = dbClient.UpdateLoadedTableVersion("testTable", factset.PackageVersion{1, 10}, "test")
//	assert.NoError(t, err)
//
//	version, err := dbClient.GetLoadedVersion("testTable")
//	assert.NoError(t, err)
//	assert.EqualValues(t, factset.PackageVersion{1, 10}, version)
//}

//func TestClientDropTablesWithPrefix(t *testing.T) {
//	createTestTables()
//	defer dropTestTables()
//
//	assert.Equal(t, 5, countTestTables())
//
//	err := dbClient.DropTablesWithDataset("ent")
//	assert.NoError(t, err)
//	assert.Equal(t, 2, countTestTables())
//
//	err = dbClient.DropTablesWithDataset("fake")
//	assert.NoError(t, err)
//	assert.Equal(t, 2, countTestTables())
//}

//func TestClientGetPackageMetadata(t *testing.T) {
//	dbClient.LoadMetadataTables()
//	defer removeMetadataTables()
//	_, err := dbClient.DB.Exec(`INSERT INTO metadata_package_version (product, schema_feed_version, schema_sequence, schema_date_loaded, package_feed_version, package_sequence, package_date_loaded)
//										VALUES ('ent_entity_advanced', 2, 1234, '2017-01-02 03:04:05', 2, 5678, '2017-06-07 08:09:10')`)
//	assert.NoError(t, err)
//
//	pkgMetadata, err := dbClient.GetPackageMetadata(factset.Package{
//		Dataset:   "ent",
//		FSPackage: "entity",
//		Product:   "ent_entity_advanced",
//	})
//	assert.NoError(t, err)
//	assert.EqualValues(t, factset.PackageMetadata{
//		Package: factset.Package{
//			Dataset:   "ent",
//			FSPackage: "entity",
//			Product:   "ent_entity_advanced",
//		},
//		SchemaVersion: factset.PackageVersion{
//			FeedVersion: 2,
//			Sequence:    1234,
//		},
//		PackageVersion: factset.PackageVersion{
//			FeedVersion: 2,
//			Sequence:    5678,
//		},
//		SchemaLoadedDate:  time.Date(2017, 1, 2, 3, 4, 5, 0, time.UTC),
//		PackageLoadedDate: time.Date(2017, 6, 7, 8, 9, 10, 0, time.UTC),
//	}, pkgMetadata)
//}

func verifyMetadata() (bool, error) {
	queryTemplate := `SELECT count(*)
						FROM information_schema.TABLES
						WHERE TABLE_SCHEMA = ?
						AND TABLE_NAME LIKE "metadata%"`

	stmt, err := dbClient.DB.Prepare(queryTemplate)
	if err != nil {
		return false, err
	}
	var metadataTableCount int
	err = stmt.QueryRow(dbClient.schema).Scan(&metadataTableCount)
	defer stmt.Close()
	if err != nil {
		return false, err
	}
	return metadataTableCount == MetadataTableCount, nil
}

func GetLoadedVersion(tableName string) (factset.PackageVersion, error) {
	queryTemplate := `SELECT feed_version, sequence
						FROM metadata_table_version
						WHERE tablename = ?
						`
	stmt, err := dbClient.DB.Prepare(queryTemplate)
	if err != nil {
		return factset.PackageVersion{}, err
	}
	var feedVersion int
	var sequence int
	err = stmt.QueryRow(tableName).Scan(&feedVersion, &sequence)
	defer stmt.Close()
	if err != nil {
		return factset.PackageVersion{}, err
	}
	return factset.PackageVersion{FeedVersion: feedVersion, Sequence: sequence}, nil
}

func removeMetadataTables() {
	dbClient.DB.Exec(`DROP TABLE IF EXISTS metadata_package_version, metadata_table_version`)
}

func createTestTables() {
	dbClient.DB.Exec(`CREATE TABLE ent_test1 (ID VARCHAR(10) NOT NULL)`)
	dbClient.DB.Exec(`CREATE TABLE ent_test2 (ID VARCHAR(10) NOT NULL)`)
	dbClient.DB.Exec(`CREATE TABLE ent_test3 (ID VARCHAR(10) NOT NULL)`)
	dbClient.DB.Exec(`CREATE TABLE ppl_test1 (ID VARCHAR(10) NOT NULL)`)
	dbClient.DB.Exec(`CREATE TABLE ppl_test2 (ID VARCHAR(10) NOT NULL)`)
}

func dropTestTables() {
	dbClient.DB.Exec(`DROP TABLE IF EXISTS ent_test1, ent_test2, ent_test3, ppl_test1, ppl_test2`)

}

func countTestTables() int {
	var i int
	dbClient.DB.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema=?`, dbClient.schema).Scan(&i)

	return i
}
