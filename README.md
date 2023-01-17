# confess
Object store consistency checker

## Usage
```
NAME:
  confess - Object store consistency checker

USAGE:
  confess - HOSTS [FLAGS]

HOSTS:
  HOSTS is a comma separated list or a range of hostnames/ip-addresses

FLAGS:
  --access-key value        specify access key [$CONFESS_ACCESS_KEY]
  --secret-key value        specify secret key [$CONFESS_SECRET_KEY]
  --insecure                disable TLS certificate verification [$CONFESS_INSECURE]
  --region value            specify a custom region [$CONFESS_REGION]
  --bucket value            Bucket to use for confess tests [$CONFESS_BUCKET]
  --output value, -o value  specify output path for confess log
  --help, -h                show help
  --version, -v             print the version
  
EXAMPLES:
  1. Run consistency across 4 MinIO Servers (http://minio1:9000 to http://minio4:9000) on bucket "mybucket"
     $ confess --access-key minio --secret-key minio123 --bucket mybucket http://minio{1...4}:9000 
```

## License
Confess is licensed under the GNU AGPLv3 license that can be found in the [LICENSE](https://github.com/minio/confess/blob/master/LICENSE) file.
