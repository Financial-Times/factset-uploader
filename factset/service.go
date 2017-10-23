package factset

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Servicer interface {
	GetSchemaInfo(pkg Package) (*PackageVersion, error)
	GetFileList(pkg Package, startVersion *PackageVersion) ([]FSFile, error)
	GetLatestFullFile(pkg Package) (FSFile, error)
	Download(file FSFile) (*os.File, error)
}

type Service struct {
	client    *sftpClient
	workspace string
}

func NewService(sftpUser, sftpKey, sftpAddress string, sftpPort int, workspace string) (Servicer, error) {

	sftpClient, err := newSFTPClient(sftpUser, sftpKey, sftpAddress, sftpPort)
	if err != nil {
		return nil, err
	}

	return &Service{
		client:    sftpClient,
		workspace: workspace,
	}, nil
}

func (s *Service) GetSchemaInfo(pkg Package) (*PackageVersion, error) {
	files, err := s.client.ReadDir(buildSchemaPath(pkg))
	if err != nil {
		return nil, err
	}

	var latestSchema *PackageVersion

	for _, file := range files {
		name := file.Name()[strings.LastIndex(file.Name(), "/")+1:]
		name = name[:strings.LastIndex(file.Name(), ".")]

		splitName := strings.Split(name, "_")

		if splitName[2] == "docs" {
			continue
		}

		feedVersion, _ := strconv.Atoi(splitName[1][1:])
		sequence, _ := strconv.Atoi(splitName[3])

		if latestSchema != nil && feedVersion > latestSchema.FeedVersion {
			latestSchema = &PackageVersion{
				FeedVersion: feedVersion,
				Sequence:    sequence,
			}
		}
	}
	if latestSchema == nil {
		return nil, errors.New("Could not find schema")
	}
	return latestSchema, nil
}

// Get list of available files for a package
// Get files after given version for a package

func (s *Service) GetFileList(pkg Package, startVersion *PackageVersion) ([]FSFile, error) {
	var outputFileList []FSFile

	files, err := s.client.ReadDir(buildFilePath(pkg))
	if err != nil {
		return []FSFile{}, err
	}

	fsFiles := transformFileInfo(pkg.Product, files)

	if startVersion == nil {
		// No filtering required.
		return fsFiles, nil
	}

	for _, v := range fsFiles {
		if v.Version.FeedVersion == startVersion.FeedVersion && v.Version.Sequence > startVersion.Sequence {
			outputFileList = append(outputFileList, v)
		}
	}

	return outputFileList, nil
}

func (s *Service) GetLatestFullFile(pkg Package) (FSFile, error) {
	var outFile FSFile

	files, err := s.GetFileList(pkg, nil)
	if err != nil {
		return FSFile{}, err
	}
	if len(files) == 0 {
		return FSFile{}, errors.New("no valid files")
	}
	for _, file := range files {
		if (FSFile{}) == outFile && file.Version.FeedVersion == pkg.FeedVersion && file.IsFull {
			outFile = file
		} else if file.Version.FeedVersion == pkg.FeedVersion && file.Version.Sequence > outFile.Version.Sequence {
			outFile = file
		}
	}
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

func buildSchemaPath(pkg Package) string {
	return fmt.Sprintf("/datafeeds/documents/docs_%s", pkg.Dataset)
}

func buildFilePath(pkg Package) string {
	return fmt.Sprintf("/datafeeds/%s/%s", pkg.FSPackage, pkg.Product)
}

func transformFileInfo(product string, files []os.FileInfo) []FSFile {
	var outputFiles []FSFile

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var outFile FSFile

		outFile.Path = file.Name()

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
			} else if v[0] == 'v' {
				if i, err := strconv.Atoi(v[1:]); err == nil {
					outFile.Version.FeedVersion = i
				}
			} else if i, err := strconv.Atoi(v); err == nil {
				outFile.Version.Sequence = i
			}
		}
		outputFiles = append(outputFiles, outFile)
	}
	return outputFiles
}

