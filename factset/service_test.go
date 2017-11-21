package factset

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var pkg = Package{
	Dataset:     "ppl",
	FSPackage:   "people",
	Product:     "ppl_test",
	Bundle:      "ppl_test",
	FeedVersion: 1,
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
			"Errors when no schema to process",
			"../fixtures/datafeeds/documents/docs_missingSchema",
			nil,
			"missingSchema",
			errors.New("no valid schema found in: "),
			-1,
			-1,
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
				pv, err := fs.GetSchemaInfo(pkg)
				if d.dataset == "emptyDir" || d.dataset == "missingSchema" {
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

func Test_GetSchemaInfo_EmptyDir(t *testing.T) {
	var directory = "../fixtures/datafeeds/documents/docs_emptyDir"
	os.Mkdir(directory, 0700)
	files, err := ioutil.ReadDir(directory)
	assert.NoError(t, err, fmt.Sprintf("Test: %s failed, should read file with no error", "Error when directory has no files"))
	fs := &Service{&MockSftpClient{files, nil}, "", "../fixtures/datafeeds"}
	_, err = fs.GetSchemaInfo(pkg)
	assert.Error(t, err, "Test failed, directory should be empty")
	assert.Contains(t, err.Error(), "no schema found in: ", "Test failed, unexpected error was returned")
	defer os.Remove(directory)
}

func Test_GetLatestFile(t *testing.T) {

	testCases := []struct {
		testName            string
		testDirectory       string
		testPackage         Package
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
			testName:            "Success when file and directory exist",
			testDirectory:       "../fixtures/datafeeds/people/ppl_test/ppl_singleZip",
			testPackage:         pkg,
			fileSuffix:          "singleZip",
			isFullLoad:          true,
			expectedFileName:    "ppl_test_v1_full_1234.zip",
			expectedPath:        "../fixtures/datafeeds/people/ppl_test/ppl_test_v1_full_1234.zip",
			expectedSequence:    1234,
			expectedFeedVersion: 1,
		},
		{
			testName:      "Error on non-existent folder directory",
			testDirectory: "../fixtures/datafeeds/people/ppl_test/ppl_nonExistent",
			testPackage:   pkg,
			readDirErr:    errors.New("no such file or directory"),
			fileSuffix:    "nonExistent",
		},
		{
			testName:            "Returns file with most recent sequence with all full files",
			testDirectory:       "../fixtures/datafeeds/people/ppl_test/ppl_multiSequenceFull",
			testPackage:         pkg,
			fileSuffix:          "multiSequenceFull",
			isFullLoad:          true,
			expectedFileName:    "ppl_test_v1_full_5678.zip",
			expectedPath:        "../fixtures/datafeeds/people/ppl_test/ppl_test_v1_full_5678.zip",
			expectedSequence:    5678,
			expectedFeedVersion: 1,
		},
		{
			testName:            "Returns file with most recent sequence with all delta files",
			testDirectory:       "../fixtures/datafeeds/people/ppl_test/ppl_multiSequenceDelta",
			testPackage:         pkg,
			fileSuffix:          "multiSequenceDelta",
			expectedFileName:    "ppl_test_v1_5678.zip",
			expectedPath:        "../fixtures/datafeeds/people/ppl_test/ppl_test_v1_5678.zip",
			expectedSequence:    5678,
			expectedFeedVersion: 1,
		},
		{
			testName:            "Picks correct file for full load with daily and weekly files",
			testDirectory:       "../fixtures/datafeeds/people/ppl_test/ppl_pickCorrectZip",
			testPackage:         pkg,
			fileSuffix:          "pickCorrectZip",
			isFullLoad:          true,
			expectedFileName:    "ppl_test_v1_full_5678.zip",
			expectedPath:        "../fixtures/datafeeds/people/ppl_test/ppl_test_v1_full_5678.zip",
			expectedSequence:    5678,
			expectedFeedVersion: 1,
		},
		{
			testName:            "Picks correct file for incremental load with daily and weekly files",
			testDirectory:       "../fixtures/datafeeds/people/ppl_test/ppl_pickCorrectZip",
			testPackage:         pkg,
			fileSuffix:          "pickCorrectZip",
			expectedFileName:    "ppl_test_v1_9999.zip",
			expectedPath:        "../fixtures/datafeeds/people/ppl_test/ppl_test_v1_9999.zip",
			expectedSequence:    9999,
			expectedFeedVersion: 1,
		},
		{
			testName:      "Picks correct file for full load with varied version",
			testDirectory: "../fixtures/datafeeds/people/ppl_test/ppl_pickCorrectZip",
			testPackage: Package{
				Dataset:     pkg.Dataset,
				FSPackage:   pkg.FSPackage,
				FeedVersion: 2,
				Bundle:      pkg.Bundle,
				Product:     pkg.Product,
			},
			fileSuffix:          "pickCorrectZip",
			isFullLoad:          true,
			expectedFileName:    "ppl_test_v2_full_5670.zip",
			expectedPath:        "../fixtures/datafeeds/people/ppl_test/ppl_test_v2_full_5670.zip",
			expectedSequence:    5670,
			expectedFeedVersion: 2,
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
				fsFile, err := fs.GetLatestFile(d.testPackage, d.isFullLoad)
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

func Test_GetLatestFile_EmptyDirectory(t *testing.T) {
	var directory = "../fixtures/datafeeds/people/ppl_test/ppl_emptyDir"
	os.Mkdir(directory, 0700)
	files, err := ioutil.ReadDir(directory)
	assert.NoError(t, err, fmt.Sprintf("Test: %s failed, should read file with no error", "Error when directory has no files"))
	fs := &Service{&MockSftpClient{files, nil}, "", "../fixtures/datafeeds"}
	_, err = fs.GetLatestFile(pkg, true)
	assert.Error(t, err, "Test failed, directory should be empty")
	assert.Contains(t, err.Error(), "no data archives found in: ../fixtures/datafeeds/people/ppl_test", "Test failed, returned unexpected error")
	defer os.Remove(directory)
}

func Test_GetLatestFile_NestedDirectory(t *testing.T) {
	var directory = "../fixtures/datafeeds/people/ppl_test/ppl_nestedDir"
	os.Mkdir(directory, 0700)
	os.Mkdir(directory+"/evenMoreNestedDirectory", 0700)
	files, err := ioutil.ReadDir(directory)
	assert.NoError(t, err, fmt.Sprintf("Test: %s failed, should read file with no error", "Error when directory has no files"))
	fs := &Service{&MockSftpClient{files, nil}, "", "../fixtures/datafeeds"}
	//Full load error
	_, err = fs.GetLatestFile(pkg, true)
	assert.Error(t, err, "Test failed, directory should be empty")
	assert.Contains(t, err.Error(), "no valid Full files found in: ../fixtures/datafeeds/people/ppl_test", "Test failed, mismatched error codes")
	//Delta load error
	_, err = fs.GetLatestFile(pkg, false)
	assert.Error(t, err, "Test failed, directory should be empty")
	assert.Contains(t, err.Error(), "no valid Delta files found in: ../fixtures/datafeeds/people/ppl_test", "Test failed, mismatched error codes")
	defer os.RemoveAll(directory)
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
			fsFile, err := fs.Download(ftpFile, "ppl_test")
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

type MockSftpClient struct {
	files []os.FileInfo
	err   error
}

func (m *MockSftpClient) ReadDir(dir string) ([]os.FileInfo, error) {
	return m.files, m.err
}

func (m *MockSftpClient) Download(path string, dest string, product string) error {
	if m.err == nil {
		m.err = copyFile(path)
	}
	return m.err
}

func (m *MockSftpClient) Close() error {
	os.Remove("./ppl_test_v1_full_1234.zip")
	return nil
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
	return err
}
