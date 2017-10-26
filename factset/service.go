package factset

import (
	"errors"
	"fmt"
	"github.com/coreos/fleet/log"
	"os"
	"strconv"
	"strings"
)

type Servicer interface {
	GetSchemaInfo(pkg Package) (*PackageVersion, error)
	GetLatestFile(pkg Package, isFull bool) (FSFile, error)
	Download(file FSFile) (*os.File, error)
}

type Service struct {
	client           sftpClienter
	workspace        string
	ftpServerBaseDir string
}

var baseDir = "/datafeeds"
var schemaDir = "/documents"

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

func (s *Service) GetSchemaInfo(pkg Package) (*PackageVersion, error) {
	files, err := s.client.ReadDir(s.ftpServerBaseDir + schemaDir + fmt.Sprintf("/docs_%s/", pkg.Dataset))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, errors.New("Directory had no files to read")
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
		return nil, errors.New("There was no schema to process")
	}
	return latestSchema, nil
}

// Get list of available files for a package
// Get files after given version for a package
func (s *Service) GetLatestFile(pkg Package, isFull bool) (FSFile, error) {
	var outFile FSFile

	pathToFiles := s.ftpServerBaseDir + fmt.Sprintf("/%s/%s", pkg.FSPackage, pkg.Product)
	files, err := s.client.ReadDir(pathToFiles)
	if err != nil {
		return outFile, err
	}
	if len(files) == 0 {
		return outFile, errors.New("Directory had no files to read")
	}

	fsFiles := transformFileInfo(pkg.Product, files, isFull)
	if len(fsFiles) == 0 {
		return outFile, errors.New("Failed to extract file info from ftp server")
	}

	for _, file := range fsFiles {
		if file.Version.FeedVersion == pkg.FeedVersion && file.Version.Sequence > outFile.Version.Sequence {
			outFile = file
		}
	}
	outFile.Path = pathToFiles
	return outFile, nil
}

func (s *Service) Download(file FSFile) (*os.File, error) {
	err := s.client.Download(file.Path, s.workspace+"/"+file.Name)
	if err != nil {
		return nil, err
	}
	localFile, err := os.Open(s.workspace + "/" + file.Name)
	if err != nil {
		return nil, err
	}

	return localFile, nil
}

// ppl              ent                     dataset
// people           entity                  package
// ppl_premium      ent_entity_advanced     product

//   /datafeeds/people/ppl_premium/ppl_premium_v1_full_1234.zip
//   /datafeeds/people/ppl_premium/ppl_premium_v1_1234.zip
//   /datafeeds/entity/ent_entity_advanced/ent_entity_advanced_v1_full_1234.zip
//   /datafeeds/documents/docs_ppl
//   /datafeeds/documents/docs_ppl/ppl_v1_schema_12.zip
//   /datafeeds/edm/edm_premium/edm_premium_full_1972.zip
//   /datafeeds/edm/edm_premium/edm_premium_1973.zip

func transformFileInfo(product string, files []os.FileInfo, isFull bool) []FSFile {
	var outputFiles []FSFile

	for _, file := range files {
		if file.IsDir() {
			log.Debugf("File %s is a directory...\n", file.Name())
			continue
		}

		var outFile FSFile

		// Get the filename from the path and then take off the product name so we've got a clean start point
		name := file.Name()[strings.LastIndex(file.Name(), "/")+1:]
		outFile.Name = name // Grab the name now before we chop it up.
		name = name[:strings.LastIndex(file.Name(), ".")]
		name = name[len(product)+1:]

		// Split the name for our parts to iterate.
		splitName := strings.Split(name, "_")

		// There are three possible bits remaining, sequence, feedVersion and full.
		for _, v := range splitName {
			if v == "full" {
				outFile.IsFull = true
			}
			if v[0] == 'v' {
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
