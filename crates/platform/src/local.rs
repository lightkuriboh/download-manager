use std::{
    fs,
    io::{self, ErrorKind},
    os::unix::fs::{FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
    time::Duration,
};

use Stream as UnixStream;
use rustix::process::geteuid;
use tokio::net::UnixListener;
pub use tokio::net::UnixStream as Stream;

const SOCKET_NAME: &str = "control.sock";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub fn validate_directory(directory: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(directory)?;
    if !metadata.is_dir() || metadata.uid() != geteuid().as_raw() || metadata.mode() & 0o077 != 0 {
        return Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "service directory must be a real directory owned by the current user with mode 0700 or stricter",
        ));
    }
    Ok(())
}

fn socket_metadata(path: &Path) -> io::Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket() || metadata.uid() != geteuid().as_raw() {
        return Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "unexpected file type or owner at the service socket path",
        ));
    }
    Ok(metadata)
}

fn file_identity(metadata: &fs::Metadata) -> (u64, u64) {
    (metadata.dev(), metadata.ino())
}

async fn dial(path: &Path) -> io::Result<UnixStream> {
    tokio::time::timeout(CONNECT_TIMEOUT, UnixStream::connect(path))
        .await
        .map_err(|_| io::Error::new(ErrorKind::TimedOut, "local service connection timed out"))?
}

fn check_peer(stream: &UnixStream) -> io::Result<()> {
    if stream.peer_cred()?.uid() != geteuid().as_raw() {
        return Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "local service peer belongs to a different user",
        ));
    }
    Ok(())
}

pub async fn connect(directory: &Path) -> io::Result<UnixStream> {
    validate_directory(directory)?;
    let path = directory.join(SOCKET_NAME);
    socket_metadata(&path)?;
    let stream = dial(&path).await?;
    check_peer(&stream)?;
    Ok(stream)
}

pub struct Listener {
    socket: UnixListener,
    path: PathBuf,
    identity: (u64, u64),
}

impl Listener {
    /// The service must hold its state-directory instance lock before binding.
    pub async fn bind(directory: &Path) -> io::Result<Self> {
        validate_directory(directory)?;
        let path = directory.canonicalize()?.join(SOCKET_NAME);
        match socket_metadata(&path) {
            Ok(stale) => match dial(&path).await {
                Err(error) if error.kind() == ErrorKind::ConnectionRefused => {
                    if file_identity(&socket_metadata(&path)?) != file_identity(&stale) {
                        return Err(io::Error::new(
                            ErrorKind::AddrInUse,
                            "service socket changed",
                        ));
                    }
                    fs::remove_file(&path)?;
                }
                Err(error) => return Err(error),
                Ok(_) => {
                    return Err(io::Error::new(
                        ErrorKind::AddrInUse,
                        "service socket is live",
                    ));
                }
            },
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let socket = UnixListener::bind(&path)?;
        let identity = file_identity(&socket_metadata(&path)?);
        Ok(Self {
            socket,
            path,
            identity,
        })
    }

    pub async fn accept(&self) -> io::Result<UnixStream> {
        loop {
            let (stream, _) = self.socket.accept().await?;
            if check_peer(&stream).is_ok() {
                return Ok(stream);
            }
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        if socket_metadata(&self.path).is_ok_and(|info| file_identity(&info) == self.identity) {
            let _ = fs::remove_file(&self.path);
        }
    }
}
