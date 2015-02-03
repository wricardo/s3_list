package s3_list

import (
	"container/list"
	"errors"
	"github.com/mitchellh/goamz/aws"
	"github.com/wricardo/goamz/s3"
	"log"
	"strings"
	"sync"
	"time"
)

type FilesListRequest struct {
	accessKey        string
	secretKey        string
	bucketName       string
	regionName       string
	startFolder      string
	files            chan string
	concurrencyLevel int
	folder_in        chan string
	folder_out       chan string
	file_out         chan string
	stats            *statsStruct
	bucket           *s3.Bucket
}

func NewFilesListRequest(access_key, secret_key, bucket_name, region_name string, files chan string) *FilesListRequest {
	flr := new(FilesListRequest)
	flr.accessKey = access_key
	flr.secretKey = secret_key
	flr.bucketName = bucket_name
	flr.regionName = region_name
	flr.startFolder = ""
	flr.files = files
	flr.concurrencyLevel = 5
	flr.stats = new(statsStruct)
	flr.folder_in = make(chan string, 0)
	flr.folder_out = make(chan string, 0)
	flr.file_out = make(chan string, 0)
	return flr
}

type statsStruct struct {
	Files   int64
	Folders int64
	M       sync.Mutex
}

func (this *statsStruct) addFolders(n int64) {
	this.M.Lock()
	this.Folders = this.Folders + n
	this.M.Unlock()
}

func (this *statsStruct) subFolders(n int64) {
	this.M.Lock()
	this.Folders = this.Folders - n
	this.M.Unlock()
}

func (this *statsStruct) addFiles(n int64) {
	this.M.Lock()
	this.Files = this.Files + n
	this.M.Unlock()
}

func (this *statsStruct) subFiles(n int64) {
	this.M.Lock()
	this.Files = this.Files - n
	this.M.Unlock()
}

func (this *FilesListRequest) Folder(folder string) error {
	this.startFolder = strings.TrimLeft(folder, "/")
	return nil
}

func (this *FilesListRequest) Concurrency(n int) error {
	if n < 1 {
		return errors.New("Concurrency can not be less than 1")
	}
	this.concurrencyLevel = n
	return nil
}

func (this *FilesListRequest) Run() error {
	if this.accessKey == "" {
		return errors.New("Invalid AccessKey")
	}
	if this.secretKey == "" {
		return errors.New("Invalid SecretKey")
	}
	region, ok := aws.Regions[this.regionName]
	if !ok {
		return errors.New("Invalid region name")
	}

	auth := aws.Auth{
		AccessKey: this.accessKey,
		SecretKey: this.secretKey,
	}

	client := s3.New(auth, region)
	this.bucket = client.Bucket(this.bucketName)

	this.Aux()

	for i := 0; i < this.concurrencyLevel; i++ {
		go this.Lister()
	}
	this.stats.addFolders(1)
	this.folder_in <- this.startFolder
	for {
		this.stats.M.Lock()
		if this.stats.Folders == 0 && this.stats.Files == 0 {
			break
		}
		this.stats.M.Unlock()
		time.Sleep(time.Millisecond * 5)
	}
	return nil
}

func (this *FilesListRequest) Aux() {
	go func() {
		for {
			select {
			case file := <-this.file_out:
				this.files <- file
				this.stats.subFiles(1)
			}

		}
	}()
	go func() {
		l := list.New()
		for {
			select {
			case folder := <-this.folder_out:
				l.PushBack(folder)
			default:
			}
			e := l.Front()
			if e != nil {
				select {
				case this.folder_in <- e.Value.(string):
					l.Remove(e)
				default:
				}
			}
		}
	}()
}

func (this *FilesListRequest) Lister() {
	for {
		folder := <-this.folder_in
		this.listObjectsInBucket(this.bucket, folder, "")

		this.stats.subFolders(1)
	}
}

func (this *FilesListRequest) listObjectsInBucket(bucket *s3.Bucket, folder, marker string) {
	var (
		err    error  = errors.New("Initial err")
		last   string = ""
		result *s3.ListResp
	)

	for {
		for err != nil {
			result, err = bucket.List(folder, "/", last, 1000)
			if err != nil {
				log.Println("err bucket list", err)
			}
		}

		this.stats.addFolders(int64(len(result.CommonPrefixes)))
		this.stats.addFiles(int64(len(result.Contents)))

		for _, folder := range result.CommonPrefixes {
			this.folder_out <- folder
		}

		for _, file := range result.Contents {
			this.file_out <- "/" + file.Key
		}

		if result.IsTruncated == false {
			break
		}
		last = result.NextMarker
		err = errors.New("Initial err")
	}
}
