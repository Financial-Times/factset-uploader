package factset

import (
	"io"
	"os"
	"path"
	"strconv"

	"github.com/pkg/sftp"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
)

type sftpClient struct {
	sftp *sftp.Client
}

type sftpClienter interface {
	ReadDir(dir string) ([]os.FileInfo, error)
	Download(path string, dest string, product string) error
	Close() error
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
	file, err := s.sftp.Open(path)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Could not open %s on sftp server", path)
		return err
	}
	defer file.Close()
	return s.save(file, dest, product)
}

//TODO nice to have, a progress bar of download
func (s *sftpClient) save(file *sftp.File, dest string, product string) error {
	_, fileName := path.Split(file.Name())
	downFile, err := os.Create(path.Join(dest, fileName))
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

	log.WithFields(log.Fields{"fs_product": product}).Infof("Downloading %s from sftp server", fileName)
	n, err := io.Copy(downFile, io.LimitReader(file, size))
	if n != size || err != nil {
		log.WithError(err).WithFields(log.Fields{"fs_product": product}).Errorf("Download stopped at [%d] when copying sftp file to %s/%s", n, dest, fileName)
		return err
	}

	return nil
}

func (s *sftpClient) Close() error {
	if s.sftp != nil {
		if err := s.sftp.Close(); err != nil {
			return err
		}
	}
	return nil
}
