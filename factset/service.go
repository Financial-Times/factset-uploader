package factset

import (
	"fmt"

	"os"
	"strconv"

	"github.com/golang/go/src/pkg/strings"
	"github.com/pkg/errors"
)

type Service struct {
	client    *SFTPClient
	workspace string
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

func (s *Service) GetFileList(pkg Package, startVersion *PackageVersion) ([]fsFile, error) {
	var outputFileList []fsFile

	files, err := s.client.ReadDir(buildFilePath(pkg))
	if err != nil {
		return []fsFile{}, err
	}

	fsFiles := transformFileInfo(pkg.Product, files)

	if startVersion == nil {
		// No filtering required.
		return fsFiles, nil
	}

	for _, v := range fsFiles {
		if v.version.FeedVersion == startVersion.FeedVersion && v.version.Sequence > startVersion.Sequence {
			outputFileList = append(outputFileList, v)
		}
	}

	return outputFileList, nil
}

func (s *Service) Download(file fsFile) error {
	return s.client.Download(file.path, s.workspace+"/"+file.name)
}

// ppl              ent                     dataset
// people           entity                  package
// ppl_premium      ent_entity_advanced     product

//   /datafeeds/people/ppl_premium/ppl_premium_v1_full_1234.zip
//   /datafeeds/people/ppl_premium/ppl_premium_v1_1234.zip
//   /datafeeds/entity/ent_entity_advanced/ent_entity_advanced_v1_full_1234.zip
//   /datafeeds/documents/docs_ppl
//   /datafeeds/edm/edm_premium/edm_premium_full_1972.zip
//   /datafeeds/edm/edm_premium/edm_premium_1973.zip

func buildSchemaPath(pkg Package) string {
	return fmt.Sprintf("/datafeeds/documents/docs_%s", pkg.Dataset)
}

func buildFilePath(pkg Package) string {
	return fmt.Sprintf("/datafeeds/%s/%s", pkg.FSPackage, pkg.Product)
}

type fsFile struct {
	name    string
	path    string
	version PackageVersion
	isFull  bool
}

func transformFileInfo(product string, files []os.FileInfo) []fsFile {
	var outputFiles []fsFile

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var outFile fsFile

		outFile.path = file.Name()

		// Get the filename from the path and then take off the product name so we've got a clean start point
		name := file.Name()[strings.LastIndex(file.Name(), "/")+1:]
		outFile.name = name // Grab the name now before we chop it up.
		name = name[:strings.LastIndex(file.Name(), ".")]
		name = name[len(product)+1:]

		// Split the name for our parts to iterate.
		splitName := strings.Split(name, "_")

		// There are three possible bits remaining, sequence, feedVersion and full.
		for _, v := range splitName {
			if v == "full" {
				outFile.isFull = true
			} else if v[0] == 'v' {
				if i, err := strconv.Atoi(v[1:]); err == nil {
					outFile.version.FeedVersion = i
				}
			} else if i, err := strconv.Atoi(v); err == nil {
				outFile.version.Sequence = i
			}
		}
		outputFiles = append(outputFiles, outFile)
	}
	return outputFiles
}
