package rds

import (
	"os"

	"testing"
	"time"

	"github.com/Financial-Times/factset-uploader/factset"
	_ "github.com/joho/godotenv/autoload"
	"github.com/stretchr/testify/assert"
)

var dbClient *Client

func init() {
	var dsn string

	if os.Getenv("RDS_DSN") != "" {
		dsn = os.Getenv("RDS_DSN")
	} else {
		dsn = "root:@/test2"
	}

	dbClient, _ = NewClient(dsn)
}

func TestClientGetLoadedVersionNoTable(t *testing.T) {
	defer dropTestTables()
	defer removeMetadataTables()

	hasMetadata, err := verifyMetadata()
	assert.NoError(t, err)
	assert.False(t, hasMetadata)

	version, err := getLoadedVersion("testTable")
	assert.Error(t, err)
	assert.EqualValues(t, factset.PackageVersion{}, version, "Table version is not an empty object")
}

func TestClientUpdateAndGetLoadedVersion(t *testing.T) {
	defer dropTestTables()
	defer removeMetadataTables()
	err := dbClient.LoadMetadataTables()
	assert.NoError(t, err)

	err = dbClient.UpdateLoadedTableVersion("testTable", factset.PackageVersion{1, 10}, factset.Package{Product: "test", Bundle: "test"})
	assert.NoError(t, err)

	version, err := getLoadedVersion("testTable")
	assert.NoError(t, err)
	assert.EqualValues(t, factset.PackageVersion{1, 10}, version)
}

//func TestClientDropTablesWithPrefix(t *testing.T) {
//	createTestTables()
//	defer dropTestTables()
//
//	assert.Equal(t, 5, countTestTables())
//
//	err := dbClient.DropTablesWithProduct("foo", "foo_entity")
//	assert.NoError(t, err)
//	assert.Equal(t, 2, countTestTables())
//
//	err = dbClient.DropTablesWithProduct("fake", "fke_entity")
//	assert.NoError(t, err)
//	assert.Equal(t, 2, countTestTables())
//}

func TestClientGetPackageMetadata(t *testing.T) {
	dbClient.LoadMetadataTables()
	defer removeMetadataTables()
	_, err := dbClient.DB.Exec(`INSERT INTO metadata_package_version (product, bundle, schema_feed_version, schema_sequence, schema_date_loaded, package_feed_version, package_sequence, package_date_loaded)
										VALUES ('foo_fooey_advanced', 'foo_fooey_advanced', 2, 1234, '2017-01-02 03:04:05', 2, 5678, '2017-06-07 08:09:10')`)
	assert.NoError(t, err)

	pkgMetadata, err := dbClient.GetPackageMetadata(factset.Package{
		Dataset:   "foo",
		FSPackage: "fooey",
		Product:   "foo_fooey_advanced",
		Bundle:    "foo_fooey_advanced",
	})
	assert.NoError(t, err)
	assert.EqualValues(t, factset.PackageMetadata{
		Package: factset.Package{
			Dataset:   "foo",
			FSPackage: "fooey",
			Product:   "foo_fooey_advanced",
			Bundle:    "foo_fooey_advanced",
		},
		SchemaVersion: factset.PackageVersion{
			FeedVersion: 2,
			Sequence:    1234,
		},
		PackageVersion: factset.PackageVersion{
			FeedVersion: 2,
			Sequence:    5678,
		},
		SchemaLoadedDate:  time.Date(2017, 1, 2, 3, 4, 5, 0, time.UTC),
		PackageLoadedDate: time.Date(2017, 6, 7, 8, 9, 10, 0, time.UTC),
	}, pkgMetadata)
}

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

func getLoadedVersion(tableName string) (factset.PackageVersion, error) {
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
	dbClient.DB.Exec(`CREATE TABLE foo_test1 (ID VARCHAR(10) NOT NULL)`)
	dbClient.DB.Exec(`CREATE TABLE foo_test2 (ID VARCHAR(10) NOT NULL)`)
	dbClient.DB.Exec(`CREATE TABLE foo_test3 (ID VARCHAR(10) NOT NULL)`)
	dbClient.DB.Exec(`CREATE TABLE bob_test1 (ID VARCHAR(10) NOT NULL)`)
	dbClient.DB.Exec(`CREATE TABLE bob_test2 (ID VARCHAR(10) NOT NULL)`)
}

func dropTestTables() {
	dbClient.DB.Exec(`DROP TABLE IF EXISTS foo_test1, foo_test2, foo_test3, bob_test1, bob_test2`)

}

func countTestTables() int {
	var i int
	dbClient.DB.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema=?`, dbClient.schema).Scan(&i)

	return i
}
