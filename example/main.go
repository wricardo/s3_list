package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/wricardo/s3_list"
)

func main() {
	sess, err := session.NewSessionWithOptions(session.Options{})
	if err != nil {
		log.Fatal(err)
	}

	lr := s3_list.ListRequest{
		BucketName: "mytestbucketabc",
		Prefix:     "mfa/",
	}
	ch := s3_list.ListFiles(sess, lr)
	for {
		select {
		case v := <-ch:
			if v.Err == nil && v.Key == nil {
				return
			}
			if v.Err != nil {
				log.Fatal(v.Err)
			}
			fmt.Println(*v.Key)

		}
	}
}
