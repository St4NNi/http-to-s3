use s3move::upload_file;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    upload_file(
        "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/GCA_000001405.15_GRCh38_genomic.fna.gz".to_string(),
        "testb".to_string(),
        "hg381".to_string(),
    )
    .await
    .unwrap();
    Ok(())
}
