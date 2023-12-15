use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use clap::Parser;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::task;
use tracing::trace;

#[derive(Debug, Parser, Clone)]
struct Opt {
    #[structopt(long)]
    bucket: String,
    #[structopt(long)]
    object: String,
    #[structopt(long)]
    destination: PathBuf,
    #[structopt(long)]
    multipart: bool,
}

// Use normal API to download a file and write to disk
async fn get_object(client: Client, opt: Opt) -> Result<usize, anyhow::Error> {
    trace!("bucket:      {}", opt.bucket);
    trace!("object:      {}", opt.object);
    trace!("destination: {}", opt.destination.display());

    let mut file = File::create(opt.destination.clone()).await?;

    let mut object = client
        .get_object()
        .bucket(opt.bucket)
        .key(opt.object)
        .send()
        .await?;

    let mut byte_count = 0_usize;
    while let Some(bytes) = object.body.try_next().await? {
        let bytes_len = bytes.len();
        file.write_all(&bytes).await?;
        trace!("Intermediate write of {bytes_len}");
        byte_count += bytes_len;
    }

    Ok(byte_count)
}

async fn get_object_multipart(client: Client, opt: Opt) -> Result<usize, anyhow::Error> {
    // Lets use 10 MegaByte chunks
    let chunk_size = 10 * 1024 * 1024;

    trace!("bucket:      {}", opt.bucket);
    trace!("object:      {}", opt.object);
    trace!("destination: {}", opt.destination.display());

    let file = File::create(opt.destination.clone()).await?;
    let mut writer = BufWriter::new(file);

    // Get the object metadat so we can get the size
    let object_metadata = client
        .head_object()
        .bucket(&opt.bucket)
        .key(&opt.object)
        .send()
        .await?;

    let object_size = object_metadata.content_length.unwrap() as u64;

    // Create a list of ranges to download
    let mut tasks = Vec::new();
    let mut start: u64 = 0;
    let mut end: u64 = chunk_size;
    while start < object_size as u64 {
        tasks.push(task::spawn(download_part(
            client.clone(),
            opt.clone(),
            start,
            end,
        )));
        start = end + 1;
        end += chunk_size;
        if end > object_size {
            end = object_size;
        }
    }

    let mut byte_count = 0;
    for task in tasks {
        let bytes = task.await??;
        writer.write_all(&bytes).await?;
        byte_count += bytes.len();
    }

    Ok(byte_count)
}

async fn download_part(
    client: Client,
    opt: Opt,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, anyhow::Error> {
    let mut object = client
        .get_object()
        .bucket(&opt.bucket)
        .key(&opt.object)
        .range(format!("bytes={}-{}", start, end))
        .send()
        .await?;

    let mut out_buf = Vec::with_capacity((end - start) as usize);

    while let Some(bytes) = object.body.try_next().await? {
        out_buf.extend_from_slice(&bytes);
    }

    Ok(out_buf)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    tracing_subscriber::fmt::init();

    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&config);

    let opts = Opt::parse();

    if opts.multipart {
        match get_object_multipart(client, opts).await {
            Ok(bytes) => {
                println!("Wrote {bytes}");
            }
            Err(err) => {
                eprintln!("Error: {}", err);
                std::process::exit(1);
            }
        }
    } else {
        match get_object(client, opts).await {
            Ok(bytes) => {
                println!("Wrote {bytes}");
            }
            Err(err) => {
                eprintln!("Error: {}", err);
                std::process::exit(1);
            }
        }
    }
}
