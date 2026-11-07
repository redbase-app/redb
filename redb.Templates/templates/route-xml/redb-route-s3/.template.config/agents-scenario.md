S3 (`redb.Route.S3`), `routes/objects.route.xml`: the bean `s3` (`S3ConnectionFactory`: ServiceUrl, Region,
ForcePathStyle, AccessKey, SecretKey) in `context.xml`; every URI says `connectionFactory=s3`. A timer writes
`orders/order-N.json` into the bucket `{{s3.inbox:incoming}}` (`keyName` takes expressions); a poll consumer reads
the new `*.json` objects under `orders/` and, when the exchange succeeded, moves them to `{{s3.archive:archive}}`.

Facts to keep: `moveAfterRead=true` needs `deleteAfterRead=false` written out (delete is the default and the two
are refused together at start); an object arrives as bytes, keep `<convertBody type="System.String"/>` before text
work; headers `redbS3.BucketName`, `redbS3.Key`, `redbS3.ContentLength`, `redbS3.ETag`; `s3.accessKey` and
`s3.secretKey` are required keys, the secret from the environment. For AWS: `s3.serviceUrl` empty, a region,
`s3.forcePathStyle` false. `deploy/docker-compose.s3.yml` runs RustFS, an S3-compatible store.

Usually changed: buckets and prefixes, the include pattern, delete instead of move, what happens to an object.

`routes/PriceListRoute.cs` writes and reads CSV price lists in C# under `prices/` of the inbox bucket (own timer
`prices.period`, `deleteAfterRead=true`); the key comes from the header `S3Headers.Key`. Keep a C# reader off
`orders/`: the XML reader owns that prefix.
