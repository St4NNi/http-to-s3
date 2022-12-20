
[![Rust](https://img.shields.io/badge/built_with-Rust-dca282.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/License-MIT-brightgreen.svg)](https://github.com/ArunaStorage/ArunaServer/blob/main/LICENSE)
[![Dependency status](https://deps.rs/repo/github/ArunaStorage/ArunaServer/status.svg)](https://deps.rs/repo/github/ArunaStorage/ArunaServer)

# http-to-s3
A small rust library that tries to efficiently move files from HTTP endpoints to S3.


## Usage

Set the needed environment variables:
- `S3_ENDPOINT_HOST` = The S3 endpoint that should be used
- `AWS_ACCESS_KEY_ID` = S3 Access Key
- `AWS_SECRET_ACCESS_KEY` = S3 Secret Key

```rust
use http-to-s3::upload_file;
upload_file(
   "https://speed.hetzner.de/100MB.bin".to_string(), // Url
   "test_bucket".to_string(), // Will be created if not exists
   "test_key".to_string(), // The key in the s3 repo
   None
 )
.await
.unwrap();
```


## License

The API is licensed under the MIT license. See the [License](LICENSE.md) file for more information