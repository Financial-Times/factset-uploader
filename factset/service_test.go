package factset

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

type MockSftpClient struct {
	files []os.FileInfo
	err   error
}

func (m *MockSftpClient) ReadDir(dir string) ([]os.FileInfo, error) {
	return m.files, m.err
}

func (m *MockSftpClient) Download(path string, dest string) error {
	if m.err == nil {
		m.err = copyFile(path)
	}
	return m.err
}

func (m *MockSftpClient) Close() {
	os.Remove("./ppl_test_v1_full_1234.zip")
}

func copyFile(path string) error {
	srcFile, err := os.Open(path + "/ppl_test_v1_full_1234.zip")
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destFile, err := os.Create("ppl_test_v1_full_1234.zip")
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	if err != nil {
		return err
	}

	err = destFile.Sync()
	if err != nil {
		return err
	}

	return nil
}

func Test_GetSchemaInfo(t *testing.T) {
	testCases := []struct {
		testName            string
		testDirectory       string
		readDirErr          error
		dataset             string
		schemaErr           error
		expectedSequence    int
		expectedFeedVersion int
	}{
		{
			"Success when file and directory exist",
			"../fixtures/datafeeds/documents/docs_singleZip",
			nil,
			"singleZip",
			nil,
			11,
			1,
		},
		{
			"Picks correct schema",
			"../fixtures/datafeeds/documents/docs_pickCorrectZip",
			nil,
			"pickCorrectZip",
			nil,
			8,
			2,
		},
		{
			"Returns highest Feed Version when multiple",
			"../fixtures/datafeeds/documents/docs_multiFeedVersion",
			nil,
			"multiFeedVersion",
			nil,
			11,
			2,
		},
		{
			"Returns highest Sequence when multiple",
			"../fixtures/datafeeds/documents/docs_multiSequence",
			nil,
			"multiSequence",
			nil,
			12,
			1,
		},
		{
			"Error when directory does not exist",
			"../fixtures/datafeeds/documents/non_existent",
			errors.New("no such file or directory"),
			"non_existent",
			nil,
			11,
			1,
		},
		{
			"Error when directory has no files",
			"../fixtures/datafeeds/documents/docs_emptyDir",
			nil,
			"emptyDir",
			errors.New("Could not process schema"),
			11,
			1,
		},
	}
	for _, d := range testCases {
		t.Run(d.testName, func(t *testing.T) {
			files, err := ioutil.ReadDir(d.testDirectory)
			if d.dataset == "non_existent" {
				assert.Error(t, err, fmt.Sprintf("Test: %s failed, directory does not exist so should not be found", d.testName))
				assert.Contains(t, err.Error(), d.readDirErr.Error(), fmt.Sprintf("Test: %s failed, mismatched error codes", d.testName))
			} else {
				assert.NoError(t, err, fmt.Sprintf("Test: %s failed, should read file with no error", d.testName))
				fs := &Service{&MockSftpClient{files, d.readDirErr}, "", "../fixtures/datafeeds"}
				pack := Package{Dataset: d.dataset}
				pv, err := fs.GetSchemaInfo(pack)
				if d.dataset == "emptyDir" {
					assert.Error(t, err, d.schemaErr, fmt.Sprintf("Test: %s failed, directory is empty should should not read schema", d.testName))
					assert.Contains(t, err.Error(), d.schemaErr.Error(), fmt.Sprintf("Test: %s failed, mismatched error codes", d.testName))
				} else {
					assert.NoError(t, err, fmt.Sprintf("Test: %s failed, file does not exist", d.testName))
					assert.Equal(t, d.expectedFeedVersion, pv.FeedVersion, fmt.Sprintf("Test: %s failed, feed version values are not equal!", d.testName))
					assert.Equal(t, d.expectedSequence, pv.Sequence, fmt.Sprintf("Test: %s failed, sequence values are not equal!", d.testName))
				}
			}
		})
	}
}

func Test_GetLatestFile(t *testing.T) {
	testCases := []struct {
		testName            string
		testDirectory       string
		readDirErr          error
		fileSuffix          string
		isFullLoad          bool
		schemaErr           error
		expectedFileName    string
		expectedPath        string
		expectedSequence    int
		expectedFeedVersion int
	}{
		{
			"Success when file and directory exist",
			"../fixtures/datafeeds/people/ppl_test/ppl_singleZip",
			nil,
			"singleZip",
			true,
			nil,
			"ppl_test_v1_full_1234.zip",
			"../fixtures/datafeeds/people/ppl_test",
			1234,
			1,
		},
		{
			"Error on non-existent folder directory",
			"../fixtures/datafeeds/people/ppl_test/ppl_nonExistent",
			errors.New("no such file or directory"),
			"nonExistent",
			false,
			nil,
			"",
			"",
			0,
			0,
		},
		{
			"Error on empty folder directory",
			"../fixtures/datafeeds/people/ppl_test/ppl_emptyDir",
			nil,
			"emptyDir",
			false,
			errors.New("No valid files"),
			"",
			"",
			0,
			0,
		},
		{
			"Error on nested folder directory",
			"../fixtures/datafeeds/people/ppl_test/ppl_nestedDirectory",
			nil,
			"nestedDirectory",
			false,
			errors.New("Failed to extract file info from ftp server"),
			"",
			"",
			0,
			0,
		},
		{
			"Returns file with most recent sequence with all full files",
			"../fixtures/datafeeds/people/ppl_test/ppl_multiSequenceFull",
			nil,
			"multiSequenceFull",
			true,
			nil,
			"ppl_test_v1_full_5678.zip",
			"../fixtures/datafeeds/people/ppl_test",
			5678,
			1,
		},
		{
			"Returns file with most recent sequence with all delta files",
			"../fixtures/datafeeds/people/ppl_test/ppl_multiSequenceDelta",
			nil,
			"multiSequenceDelta",
			false,
			nil,
			"ppl_test_v1_5678.zip",
			"../fixtures/datafeeds/people/ppl_test",
			5678,
			1,
		},
		{
			"Picks correct file for full load with daily and weekly files",
			"../fixtures/datafeeds/people/ppl_test/ppl_pickCorrectZip",
			nil,
			"pickCorrectZip",
			true,
			nil,
			"ppl_test_v1_full_5678.zip",
			"../fixtures/datafeeds/people/ppl_test",
			5678,
			1,
		},
		{
			"Picks correct file for incremental load with daily and weekly files",
			"../fixtures/datafeeds/people/ppl_test/ppl_pickCorrectZip",
			nil,
			"pickCorrectZip",
			false,
			nil,
			"ppl_test_v1_9999.zip",
			"../fixtures/datafeeds/people/ppl_test",
			9999,
			1,
		},
	}
	for _, d := range testCases {
		t.Run(d.testName, func(t *testing.T) {
			files, err := ioutil.ReadDir(d.testDirectory)
			if d.fileSuffix == "nonExistent" {
				assert.Error(t, err, fmt.Sprintf("Test: %s failed, directory does not exist so should not be found", d.testName))
				assert.Contains(t, err.Error(), d.readDirErr.Error(), fmt.Sprintf("Test: %s failed, mismatched error codes", d.testName))
			} else {
				assert.NoError(t, err, fmt.Sprintf("Test: %s failed, should read file with no error", d.testName))
				fs := &Service{&MockSftpClient{files, d.readDirErr}, "", "../fixtures/datafeeds"}
				pack := Package{Dataset: "ppl", FSPackage: "people", Product: "ppl_test", FeedVersion: 1}
				fsFile, err := fs.GetLatestFile(pack, d.isFullLoad)
				if d.fileSuffix == "emptyDir" || d.fileSuffix == "nestedDirectory" {
					assert.Error(t, err, d.schemaErr, fmt.Sprintf("Test: %s failed, directory is empty/nested should should not read file", d.testName))
					assert.Contains(t, err.Error(), d.schemaErr.Error(), fmt.Sprintf("Test: %s failed, mismatched error codes", d.testName))
				} else {
					assert.NoError(t, err, fmt.Sprintf("Test: %s failed, should return file with no error", d.testName))
					assert.Equal(t, d.expectedFileName, fsFile.Name, fmt.Sprintf("Test: %s failed, extracted wrong file", d.testName))
					assert.Equal(t, d.expectedPath, fsFile.Path, fmt.Sprintf("Test: %s failed, path does not match", d.testName))
					assert.Equal(t, d.expectedFeedVersion, fsFile.Version.FeedVersion, fmt.Sprintf("Test: %s failed, did not extract latest feed version", d.testName))
					assert.Equal(t, d.expectedSequence, fsFile.Version.Sequence, fmt.Sprintf("Test: %s failed, did not extract latest sequence", d.testName))
				}
			}
		})
	}
}

func Test_Download1(t *testing.T) {
	ftpFile := FSFile{Name: "ppl_test_v1_full_1234.zip", Path: "../fixtures/datafeeds/people/ppl_test/ppl_singleZip", Version: PackageVersion{FeedVersion: 1, Sequence: 1234}, IsFull: true}
	fs := &Service{&MockSftpClient{}, ".", "../fixtures/datafeeds"}
	fmt.Printf("File name is %s\n", ftpFile.Name)
	fmt.Printf("File path is %s\n", ftpFile.Path)
	fsFile, err := fs.Download(ftpFile)
	assert.NotNil(t, fsFile, "Should not be nil...")
	assert.NoError(t, err, fmt.Sprintf("Test: %s failed, did not copy file to current directory", "Test_Download"))
	defer fs.client.Close()
}

func Test_Download(t *testing.T) {
	testCases := []struct {
		testName      string
		expectedError error
	}{
		{
			"Success when file is downloaded and opened",
			nil,
		},
		{
			"Error when file can not be downloaded",
			errors.New("Can not download file"),
		},
		{
			"Error when file can not be downloaded",
			errors.New("Can not open file"),
		},
	}
	for _, d := range testCases {
		t.Run(d.testName, func(t *testing.T) {
			ftpFile := FSFile{Name: "ppl_test_v1_full_1234.zip", Path: "../fixtures/datafeeds/people/ppl_test/ppl_singleZip", Version: PackageVersion{FeedVersion: 1, Sequence: 1234}, IsFull: true}
			fs := &Service{&MockSftpClient{err: d.expectedError}, ".", "../fixtures/datafeeds"}
			fsFile, err := fs.Download(ftpFile)
			if d.expectedError != nil {
				assert.Error(t, err, fmt.Sprintf("Test: %s failed, error whilst downloading/copying file to current directory", d.testName))
			} else {
				assert.NotNil(t, fsFile, fmt.Sprintf("Test: %s failed, file should exist in current directory", d.testName))
				assert.Contains(t, fsFile.Name(), "ppl_test_v1_full_1234.zip", fmt.Sprintf("Test: %s failed, file name does not match expected", d.testName))
			}
			defer fs.client.Close()
		})
	}
}
