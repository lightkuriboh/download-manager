use std::{io::SeekFrom, str::FromStr};

use ring::digest::{Context, SHA256};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use crate::DownloadError;

#[derive(Clone, Debug)]
pub struct Sha256([u8; 32]);

impl FromStr for Sha256 {
    type Err = DownloadError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(DownloadError::InvalidDigest);
        }

        let mut bytes = [0; 32];
        for (slot, hex) in bytes.iter_mut().zip(value.as_bytes().as_chunks::<2>().0) {
            let high = (hex[0] as char)
                .to_digit(16)
                .ok_or(DownloadError::InvalidDigest)?;
            let low = (hex[1] as char)
                .to_digit(16)
                .ok_or(DownloadError::InvalidDigest)?;
            *slot = ((high << 4) | low) as u8;
        }
        Ok(Self(bytes))
    }
}

impl Sha256 {
    pub async fn verify(&self, file: &mut File) -> Result<(), DownloadError> {
        file.seek(SeekFrom::Start(0)).await?;
        let mut digest = Context::new(&SHA256);
        let mut buffer = vec![0; 64 * 1024];
        loop {
            let count = file.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
        if digest.finish().as_ref() != self.0 {
            return Err(DownloadError::DigestMismatch);
        }
        Ok(())
    }
}
