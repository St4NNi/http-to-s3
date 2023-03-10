use std::sync::Arc;

use async_channel::{Receiver, Sender};
use aws_sdk_s3::model::CompletedPart;
use futures::Stream;
use futures_util::StreamExt;
use s3::s3backend::S3Backend;
use tokio::try_join;

mod s3;

pub const UPLOAD_CHUNK_SIZE: u64 = 104_857_600;

pub async fn upload_file(
    url: String,
    bucket: String,
    key: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    // Spawn an ARC to the S3 Backend
    let s3backend = Arc::new(
        S3Backend::new()
            .await
            .expect("Error in initializing s3backend"),
    );

    // Make the GetRequest to URL resource
    let resp = reqwest::get(url).await?;

    // Determine the Content_length via headers
    let cont_length = resp
        .content_length()
        .clone()
        .expect("ContentLength is needed!");

    // Create a stream for the data
    let mut data_stream = resp.bytes_stream();

    // Check if upload is multi-part
    if cont_length > UPLOAD_CHUNK_SIZE {
        // Determine number of parts
        let number_of_parts = cont_length / UPLOAD_CHUNK_SIZE;
        // Get size of last_part
        let last_part = cont_length % UPLOAD_CHUNK_SIZE;

        // Initialize multi-part upload
        let upload_id = s3backend
            .init_multipart_upload(bucket.to_string(), key.to_string())
            .await?;

        // Current size of "multi-part chunk"
        let mut accumulator: usize = 0;
        // Multi-partnumber
        let mut part_number: i32 = 1;

        // Create channel
        let (mut chan_send, mut chan_recv) = async_channel::bounded(30);

        // Create S3 upload queue
        let mut queue = Vec::new();

        // Spawn the first queue worker
        queue.push(spawn_multi_upload(
            s3backend.clone(),
            bucket.to_string(),
            key.to_string(),
            upload_id.to_string(),
            chan_recv.clone(),
            part_number,
            UPLOAD_CHUNK_SIZE as i64,
        ));

        // Create "next" bytes when chunk from stream does not fit into UPLOAD chunk size
        let mut next_bytes;
        // Iterate through chunks / of streamed request body
        while let Some(chunk) = data_stream.next().await {
            // Get Bytes object from chunk
            let mut ch = chunk?;
            // Determine size of chunk
            let length = ch.len();

            // If the next chunk will make the accumulator / buffer larger than the chunk size
            if accumulator + length > UPLOAD_CHUNK_SIZE as usize {
                // Determine the point where the buffer should be splitted
                let max_size = length + accumulator - UPLOAD_CHUNK_SIZE as usize;
                // Split the bytes buffer (0..max_size -> old buffer, max_size.. -> next_bytes)
                next_bytes = ch.split_to(max_size as usize);
                // Send the "old buffer" to the channel
                chan_send.send(Ok(ch)).await?;

                // Create new channel
                (chan_send, chan_recv) = async_channel::bounded(30);

                part_number += 1;
                // Determine the size (only needed for the "last" part)
                let size = if part_number == number_of_parts as i32 + 1 {
                    last_part
                } else {
                    UPLOAD_CHUNK_SIZE
                };
                // Create new s3 uploader with new channel
                queue.push(spawn_multi_upload(
                    s3backend.clone(),
                    bucket.to_string(),
                    key.to_string(),
                    upload_id.to_string(),
                    chan_recv.clone(),
                    part_number,
                    size as i64,
                ));
                // Update accumulator
                accumulator = next_bytes.len();
                // Send the missing bytes from before to the new uploader
                chan_send.send(Ok(next_bytes)).await?;
            } else {
                // Otherwise send the whole block
                chan_send.send(Ok(ch)).await?;
                accumulator += length;
            }
        }

        let mut completed_parts = Vec::new();
        for x in queue {
            let waited_for = x.await??;
            println!("{:#?}", waited_for);

            completed_parts.push(
                CompletedPart::builder()
                    .e_tag(waited_for.1)
                    .part_number(waited_for.0)
                    .build(),
            );
        }

        s3backend
            .finish_multipart_upload(
                bucket.to_string(),
                key.to_string(),
                completed_parts,
                upload_id,
            )
            .await?;
    } else {
        let (chan_send, chan_recv) = async_channel::bounded(30);
        let single_uploader = s3backend.upload_object(chan_recv, bucket, key, cont_length as i64);
        let pro_chunks = process_chunks(data_stream, chan_send);
        if let Err(err) = try_join!(single_uploader, pro_chunks) {
            log::error!("{}", err);
            return Ok(());
        }
    }
    Ok(())
}

async fn process_chunks(
    mut data_stream: impl Stream<Item = Result<bytes::Bytes, reqwest::Error>> + std::marker::Unpin,
    chan_send: Sender<Result<bytes::Bytes, reqwest::Error>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    while let Some(chunk) = data_stream.next().await {
        chan_send.send(chunk).await?;
    }
    Ok(())
}

fn spawn_multi_upload(
    backend: Arc<S3Backend>,
    bucket: String,
    key: String,
    upload_id: String,
    recv_chan: Receiver<Result<bytes::Bytes, reqwest::Error>>,
    part_number: i32,
    content_len: i64,
) -> tokio::task::JoinHandle<
    Result<(i32, String), Box<dyn std::error::Error + Sync + std::marker::Send>>,
> {
    tokio::spawn(async move {
        backend
            .upload_multi_object(recv_chan, bucket, key, upload_id, content_len, part_number)
            .await
    })
}
