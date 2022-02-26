package s3_list

import (
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// ListRequest ...
type ListRequest struct {
	BucketName string
	Prefix     string
}

// ListResult ...
type ListResult struct {
	Err error

	// The algorithm that was used to create a checksum of the object.
	ChecksumAlgorithm []*string `type:"list" flattened:"true"`

	// The entity tag is a hash of the object. The ETag reflects changes only to
	// the contents of an object, not its metadata. The ETag may or may not be an
	// MD5 digest of the object data. Whether or not it is depends on how the object
	// was created and how it is encrypted as described below:
	//
	//    * Objects created by the PUT Object, POST Object, or Copy operation, or
	//    through the Amazon Web Services Management Console, and are encrypted
	//    by SSE-S3 or plaintext, have ETags that are an MD5 digest of their object
	//    data.
	//
	//    * Objects created by the PUT Object, POST Object, or Copy operation, or
	//    through the Amazon Web Services Management Console, and are encrypted
	//    by SSE-C or SSE-KMS, have ETags that are not an MD5 digest of their object
	//    data.
	//
	//    * If an object is created by either the Multipart Upload or Part Copy
	//    operation, the ETag is not an MD5 digest, regardless of the method of
	//    encryption.
	ETag *string `type:"string"`

	// The name that you assign to an object. You use the object key to retrieve
	// the object.
	Key *string `min:"1" type:"string"`

	// Creation date of the object.
	LastModified *time.Time `type:"timestamp"`

	// The owner of the object
	Owner *Owner `type:"structure"`

	// Size in bytes of the object
	Size *int64 `type:"integer"`

	// The class of storage used to store the object.
	StorageClass *string `type:"string" enum:"ObjectStorageClass"`
}

// Owner ...
type Owner struct {
	// Container for the display name of the owner.
	DisplayName *string `type:"string"`

	// Container for the ID of the owner.
	ID *string `type:"string"`
}

func ListFiles(sess *session.Session, lr ListRequest) chan ListResult {
	ch := make(chan ListResult)
	if sess == nil {
		go func() {
			ch <- ListResult{
				Err: errors.New("sess is nil"),
			}
			close(ch)
		}()

		return ch
	}
	if lr.BucketName == "" {
		go func() {
			ch <- ListResult{
				Err: errors.New("missing ListRequest.BucketName param"),
			}
			close(ch)
		}()

		return ch
	}
	go func() {
		s3Client := s3.New(sess)

		var nextToken *string
		for {
			bucketObjects, err := listItems(s3Client, lr.BucketName, lr.Prefix, nextToken)
			if err != nil {
				ch <- ListResult{
					Err: err,
				}
				close(ch)
				return
			}
			if bucketObjects == nil {
				ch <- ListResult{
					Err: err,
				}
				close(ch)
				return
			}

			for _, item := range bucketObjects.Contents {
				lr := ListResult{
					Err:               nil,
					ChecksumAlgorithm: item.ChecksumAlgorithm,
					ETag:              item.ETag,
					Key:               item.Key,
					LastModified:      item.LastModified,
					Size:              item.Size,
					StorageClass:      item.StorageClass,
				}
				if item.Owner != nil {
					lr.Owner = &Owner{
						DisplayName: item.Owner.DisplayName,
						ID:          item.Owner.ID,
					}
				}
				ch <- lr
			}
			if bucketObjects.NextContinuationToken == nil {
				break
			} else {
				nextToken = bucketObjects.NextContinuationToken
			}
		}
		close(ch)
		return
	}()
	return ch
}

func listItems(client *s3.S3, bucketName string, prefix string, nextToken *string) (*s3.ListObjectsV2Output, error) {
	res, err := client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:              aws.String(bucketName),
		ContinuationToken:   nextToken,
		Delimiter:           aws.String("/"),
		EncodingType:        nil,
		ExpectedBucketOwner: nil,
		FetchOwner:          nil,
		Prefix:              aws.String(prefix),
		RequestPayer:        nil,
		StartAfter:          nil,
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}
