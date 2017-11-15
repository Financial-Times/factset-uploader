package factset

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"path"
	"regexp"

	log "github.com/sirupsen/logrus"
)

// Servicer - service interface to be able to mock for testing
type Servicer interface {
	GetSchemaInfo(pkg Package) (*PackageVersion, error)
	GetLatestFile(pkg Package, isFull bool) (FSFile, error)
	Download(file FSFile, product string) (*os.File, error)
}

// Service - Factset service
type Service struct {
	client           sftpClienter
	workspace        string
	ftpServerBaseDir string
}

var baseDir = "/datafeeds"
var schemaDir = "/documents"

// NewService - create a new Service(r)
func NewService(sftpUser, sftpKey, sftpAddress string, sftpPort int, workspace string) (Servicer, error) {

	sftpClient, err := newSFTPClient(sftpUser, sftpKey, sftpAddress, sftpPort)
	if err != nil {
		return nil, err
	}

	return &Service{
		client:           sftpClient,
		workspace:        workspace,
		ftpServerBaseDir: baseDir,
	}, nil
}

// GetSchemaInfo - Get the latest schema info from Factset
func (s *Service) GetSchemaInfo(pkg Package) (*PackageVersion, error) {
	schemaDirectory := s.ftpServerBaseDir + schemaDir + fmt.Sprintf("/docs_%s/", pkg.Dataset)
	files, err := s.client.ReadDir(schemaDirectory)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": pkg.Product}).Errorf("Error reading schema directory: %s", schemaDirectory)
		return nil, err
	}
	if len(files) == 0 {
		err := fmt.Errorf("no schema found in: %s", schemaDirectory)
		log.WithFields(log.Fields{"fs_product": pkg.Product}).Error(err)
		return nil, err
	}

	var latestSchema = &PackageVersion{-1, -1}

	for _, file := range files {
		name := file.Name()[:strings.LastIndex(file.Name(), ".")]

		splitName := strings.Split(name, "_")
		if strings.Compare(splitName[2], "docs") == 0 {
			continue
		}

		feedVersion, _ := strconv.Atoi(splitName[1][1:])
		sequence, _ := strconv.Atoi(splitName[3])

		if feedVersion >= latestSchema.FeedVersion || sequence > latestSchema.Sequence {
			latestSchema = &PackageVersion{
				FeedVersion: feedVersion,
				Sequence:    sequence,
			}
		}
	}

	if latestSchema == nil || latestSchema.FeedVersion == -1 || latestSchema.Sequence == -1 {
		err := fmt.Errorf("no valid schema found in: %s", schemaDirectory)
		log.WithFields(log.Fields{"fs_product": pkg.Product}).Error(err)
		return nil, err
	}
	return latestSchema, nil
}

// GetLatestFile - Get latest file for a package
func (s *Service) GetLatestFile(pkg Package, isFull bool) (FSFile, error) {
	var mostRecentDataArchive FSFile
	var mostRecentFileName string
	var fileType string
	if isFull {
		fileType = "Full"
	} else {
		fileType = "Delta"
	}

	fileDirectory := path.Join(s.ftpServerBaseDir, pkg.FSPackage, pkg.Product)
	files, err := s.client.ReadDir(fileDirectory)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": pkg.Product}).Errorf("Error reading: %s", fileDirectory)
		return mostRecentDataArchive, err
	}
	if len(files) == 0 {
		err := fmt.Errorf("no data archives found in: %s", fileDirectory)
		log.WithFields(log.Fields{"fs_product": pkg.Product}).Error(err)
		return mostRecentDataArchive, err
	}

	fsFiles := filterAndExtractFileInfo(pkg, files, isFull)
	if len(fsFiles) == 0 {
		err := fmt.Errorf("no valid %s files found in: %s", fileType, fileDirectory)
		log.WithFields(log.Fields{"fs_product": pkg.Product}).Error(err)
		return mostRecentDataArchive, err
	}

	for _, file := range fsFiles {
		if file.Version.FeedVersion == pkg.FeedVersion && file.Version.Sequence > mostRecentDataArchive.Version.Sequence {
			mostRecentDataArchive = file
			mostRecentFileName = file.Name
		}
	}
	mostRecentDataArchive.Path = fileDirectory + "/" + mostRecentFileName
	log.WithFields(log.Fields{"fs_product": pkg.Product}).Infof("Most recent %s file for %s is %s", fileType, pkg.Product, mostRecentFileName)
	return mostRecentDataArchive, nil
}

// Download - downloads the file from Factset and provides a local file object
func (s *Service) Download(file FSFile, product string) (*os.File, error) {
	err := s.client.Download(file.Path, s.workspace, product)
	if err != nil {
		return nil, err
	}
	localFile, err := os.Open(s.workspace + "/" + file.Name)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Could not open file: %s", s.workspace+"/"+file.Name)
		return nil, err
	}
	return localFile, nil
}

func removeBundleMetadata(filePath string) string {
	var versionRegex = regexp.MustCompile("_v[0-9]+_")
	return versionRegex.Split(filePath, 2)[0]
}

// Filters all files in directory into weekly/daily files based on isFull variable.
// Saves feed version and sequence for remaining files for later comparison
func filterAndExtractFileInfo(pkg Package, files []os.FileInfo, isFull bool) []FSFile {
	var outputFiles []FSFile

	for _, file := range files {
		if file.IsDir() {
			log.WithFields(log.Fields{"fs_product": pkg.Product}).Debugf("File %s is a directory, skipping", file.Name())
			continue
		}

		// filter the package to only the given bundle and version
		if removeBundleMetadata(file.Name()) != removeBundleMetadata(pkg.Bundle) || !strings.HasPrefix(file.Name(), fmt.Sprintf("%s_v%d", pkg.Bundle, pkg.FeedVersion)) {
			continue
		}

		var outFile FSFile

		// Get the filename from the path and then take off the bundle name so we've got a clean start point
		name := file.Name()[strings.LastIndex(file.Name(), "/")+1:]
		outFile.Name = name // Grab the name now before we chop it up.
		name = name[:strings.LastIndex(file.Name(), ".")]
		name = name[len(removeBundleMetadata(pkg.Bundle))+1:]

		// Split the name for our parts to iterate.
		splitName := strings.Split(name, "_")

		// There are three possible bits remaining, sequence, feedVersion and full.
		for _, v := range splitName {
			if v == "full" {
				outFile.IsFull = true
			}
			if len(v) > 0 && v[0] == 'v' {
				if i, err := strconv.Atoi(v[1:]); err == nil {
					outFile.Version.FeedVersion = i
				}
			}
			if i, err := strconv.Atoi(v); err == nil {
				outFile.Version.Sequence = i
			}
		}
		if isFull == outFile.IsFull {
			outputFiles = append(outputFiles, outFile)
		}

	}
	return outputFiles
}
