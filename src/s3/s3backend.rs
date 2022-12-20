use std::env;

use async_channel::Receiver;
use aws_sdk_s3::{
    model::{CompletedMultipartUpload, CompletedPart},
    types::ByteStream,
    Client, Endpoint, Region,
};

const S3_ENDPOINT_HOST_ENV_VAR: &str = "S3_ENDPOINT_HOST";

#[derive(Debug, Clone)]
pub struct S3Backend {
    pub s3_client: Client,
}

impl S3Backend {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint = env::var(S3_ENDPOINT_HOST_ENV_VAR)
            .unwrap_or_else(|_| "http://localhost:9000".to_string());

        let config = aws_config::load_from_env().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .region(Region::new("RegionOne"))
            .endpoint_resolver(Endpoint::immutable(endpoint.as_str())?)
            .build();

        let s3_client = aws_sdk_s3::Client::from_conf(s3_config);

        let handler = S3Backend { s3_client };
        Ok(handler)
    }

    // Uploads a single object in chunks
    // Objects are uploaded in chunks that come from a channel to allow modification in the data middleware
    // The receiver can directly will be wrapped and will then be directly passed into the s3 client
    pub async fn upload_object(
        &self,
        recv: Receiver<Result<bytes::Bytes, reqwest::Error>>,
        bucket: String,
        key: String,
        content_len: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.check_and_create_bucket(bucket.to_string()).await?;
        let hyper_body = hyper::Body::wrap_stream(recv);
        let bytestream = ByteStream::from(hyper_body);

        match self
            .s3_client
            .put_object()
            .set_bucket(Some(bucket))
            .set_key(Some(key))
            .set_content_length(Some(content_len))
            .body(bytestream)
            .send()
            .await
        {
            Ok(_) => {}
            Err(err) => {
                log::error!("{}", err);
                return Err(Box::new(err));
            }
        }

        Ok(())
    }

    // Initiates a multipart upload in s3 and returns the associated upload id.
    pub async fn init_multipart_upload(
        &self,
        bucket: String,
        key: String,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.check_and_create_bucket(bucket.to_string()).await?;
        let multipart = self
            .s3_client
            .create_multipart_upload()
            .set_bucket(Some(bucket))
            .set_key(Some(key))
            .send()
            .await?;

        return Ok(multipart.upload_id().unwrap().to_string());
    }

    pub async fn upload_multi_object(
        &self,
        recv: Receiver<Result<bytes::Bytes, reqwest::Error>>,
        bucket: String,
        key: String,
        upload_id: String,
        content_len: i64,
        part_number: i32,
    ) -> Result<(i32, String), Box<dyn std::error::Error + Send + Sync + 'static>> {
        log::info!("Submitted content-length was: {:#?}", content_len);
        let hyper_body = hyper::Body::wrap_stream(recv);
        let bytestream = ByteStream::from(hyper_body);

        let upload = self
            .s3_client
            .upload_part()
            .set_bucket(Some(bucket))
            .set_key(Some(key))
            .set_part_number(Some(part_number))
            .set_content_length(Some(content_len))
            .set_upload_id(Some(upload_id))
            .body(bytestream)
            .send()
            .await?;
        return Ok((part_number, upload.e_tag().unwrap().to_string()));
    }

    pub async fn finish_multipart_upload(
        &self,
        bucket: String,
        key: String,
        parts: Vec<CompletedPart>,
        upload_id: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.check_and_create_bucket(bucket.to_string()).await?;

        log::info!("{:?}", parts);

        self.s3_client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await?;

        return Ok(());
    }

    async fn check_and_create_bucket(
        &self,
        bucket: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        match self
            .s3_client
            .get_bucket_location()
            .bucket(bucket.clone())
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => match self.s3_client.create_bucket().bucket(bucket).send().await {
                Ok(_) => Ok(()),
                Err(err) => {
                    log::error!("{}", err);
                    Err(Box::new(err))
                }
            },
        }
    }
}
