package factset

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type sftpClient struct {
	sftp *sftp.Client
}

func newSFTPClient(user, key, address string, port int) (*sftpClient, error) {

	signer, err := ssh.ParsePrivateKey([]byte(key))
	if err != nil {
		return nil, err
	}

	tcpConn, err := ssh.Dial("tcp", address+":"+strconv.Itoa(port),
		&ssh.ClientConfig{
			User: user,
			Auth: []ssh.AuthMethod{
				ssh.PublicKeys(signer),
			},
		},
	)
	if err != nil {
		return nil, err
	}

	sftpClient, err := sftp.NewClient(tcpConn)
	if err != nil {
		return nil, err
	}

	return &sftpClient{
		sftp: sftpClient,
	}, nil
}

func (s *sftpClient) ReadDir(dir string) ([]os.FileInfo, error) {
	return s.sftp.ReadDir(dir)
}

func (s *sftpClient) Download(path string, dest string) error {
	file, err := s.sftp.Open(path)
	file.Name()
	if err != nil {
		return err
	}
	defer file.Close()
	return s.save(file, dest)
}

func (s *sftpClient) save(file *sftp.File, dest string) error {
	os.Mkdir(dest, 0700)
	_, fileName := path.Split(file.Name())
	downFile, err := os.Create(path.Join(dest, fileName))
	if err != nil {
		return err
	}
	defer downFile.Close()

	fileStat, err := file.Stat()
	if err != nil {
		return err
	}
	size := fileStat.Size()

	n, err := io.Copy(downFile, io.LimitReader(file, size))
	if n != size {
		e := fmt.Sprintf("Download stopped at [%d]", n)
		return errors.New(e)
	}
	if err != nil {
		return err
	}

	return nil
}

func (s *sftpClient) Close() {
	if s.sftp != nil {
		s.sftp.Close()
	}
}
