# s3_list

Go helper for listing S3 objects. `ListFiles(sess, ListRequest{BucketName, Prefix})` returns a channel of results (`ListResult`, with `Err` set on failure); a result with nil `Key` and nil `Err` marks the end. Uses `aws-sdk-go`.

See `example/main.go`:

```
go run ./example
```

Draft; API may change.
