package factset

import (
	"io"
	"os"
	"path"
	"strconv"

	"github.com/pkg/sftp"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"fmt"
)

type sftpClient struct {
	sftp *sftp.Client
}

type sftpClienter interface {
	ReadDir(dir string) ([]os.FileInfo, error)
	Download(path string, dest string, product string) error
	Close()
}

func newSFTPClient(user, key, address string, port int) (*sftpClient, error) {

	signer, err := ssh.ParsePrivateKey([]byte(key))
	if err != nil {
		log.WithError(err).Error("Could not parse ssh key!")
		return nil, err
	}

	tcpConn, err := ssh.Dial("tcp", address+":"+strconv.Itoa(port),
		&ssh.ClientConfig{
			User: user,
			Auth: []ssh.AuthMethod{
				ssh.PublicKeys(signer),
			},
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		},
	)
	if err != nil {
		log.WithError(err).Error("Could not establish tcp connection!")
		return nil, err
	}

	client, err := sftp.NewClient(tcpConn)
	if err != nil {
		log.WithError(err).Error("Could not create sftp client!")
		return nil, err
	}

	return &sftpClient{
		sftp: client,
	}, nil
}

func (s *sftpClient) ReadDir(dir string) ([]os.FileInfo, error) {
	return s.sftp.ReadDir(dir)
}

func (s *sftpClient) Download(path string, dest string, product string) error {
	fmt.Printf("Opening path to %s\n", path)
	file, err := s.sftp.Open(path)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Could not open %s on sftp server", path)
		return err
	}
	defer file.Close()
	return s.save(file, dest, product)
}

func (s *sftpClient) save(file *sftp.File, dest string, product string) error {
	fmt.Printf("Whole file name is %s\n", file.Name())
	_, fileName := path.Split(file.Name())
	fmt.Printf("Split name is %s\n", fileName)
	downFile, err := os.Create(path.Join(dest, fileName))
	fmt.Printf("Download file is %s\n", downFile.Name())
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Could not create file %s/%s", dest, fileName)
		return err
	}
	defer downFile.Close()

	fileStat, err := file.Stat()
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Could not get file stats for file %s/%s", dest, fileName)
		return err
	}
	size := fileStat.Size()

	n, err := io.Copy(downFile, io.LimitReader(file, size))
	if n != size || err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Download stopped at [%d] when copying sftp file to %s/%s", n, dest, fileName)
		return err
	}

	return nil
}

func (s *sftpClient) Close() {
	if s.sftp != nil {
		s.sftp.Close()
	}
}
