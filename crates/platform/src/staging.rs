//! Retained file and directory handles for resumable files and no-replace publication.

use std::{
    ffi::{OsStr, OsString},
    fs::File,
    io::{self, ErrorKind},
    os::unix::fs::MetadataExt,
    path::{Component, Path},
};

use rustix::{
    fs::{AtFlags, Mode, OFlags, Stat, linkat, mkdirat, openat, statat, unlinkat},
    io::Errno,
    process::geteuid,
};

pub const IDENTITY_BYTES: usize = 48;
const DIRECTORY: OFlags = OFlags::DIRECTORY
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
const PAYLOAD: &str = "payload";

pub struct Stage {
    parent: File,
    directory: File,
    payload: File,
    dir_name: String,
    destination: OsString,
    path: std::path::PathBuf,
}

impl Stage {
    pub fn create(destination: &Path, nonce: &str) -> io::Result<Self> {
        let path = destination.to_owned();
        let (parent, destination) = open_parent(destination)?;
        let dir_name = stage_name(nonce)?;
        mkdirat(
            &parent,
            dir_name.as_str(),
            Mode::RUSR | Mode::WUSR | Mode::XUSR,
        )?;
        let result = (|| {
            let directory = open_directory(&parent, dir_name.as_ref())?;
            check_private(&directory)?;
            let payload = File::from(openat(
                &directory,
                PAYLOAD,
                OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )?);
            // Make both directory entries durable before storage records the identity.
            directory.sync_all()?;
            parent.sync_all()?;
            Ok((directory, payload))
        })();
        let (directory, payload) = match result {
            Ok(files) => files,
            Err(error) => {
                if let Ok(directory) = open_directory(&parent, dir_name.as_ref()) {
                    let _ = unlinkat(&directory, PAYLOAD, AtFlags::empty());
                }
                let _ = unlinkat(&parent, dir_name.as_str(), AtFlags::REMOVEDIR);
                return Err(error);
            }
        };
        Ok(Self {
            parent,
            directory,
            payload,
            dir_name,
            destination,
            path,
        })
    }

    pub fn open(
        destination: &Path,
        nonce: &str,
        expected: &[u8; IDENTITY_BYTES],
    ) -> io::Result<Self> {
        let path = destination.to_owned();
        let (parent, destination) = open_parent(destination)?;
        let dir_name = stage_name(nonce)?;
        let directory = open_directory(&parent, dir_name.as_ref())?;
        check_private(&directory)?;
        let payload = File::from(openat(
            &directory,
            PAYLOAD,
            OFlags::RDWR | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        let stage = Self {
            parent,
            directory,
            payload,
            dir_name,
            destination,
            path,
        };
        if stage.identity()? != *expected || !stage.payload.metadata()?.is_file() {
            return Err(identity_error());
        }
        Ok(stage)
    }

    pub fn identity(&self) -> io::Result<[u8; IDENTITY_BYTES]> {
        let mut bytes = [0; IDENTITY_BYTES];
        for (file, slot) in [&self.parent, &self.directory, &self.payload]
            .into_iter()
            .zip(bytes.as_chunks_mut::<16>().0)
        {
            slot.copy_from_slice(&identity(&file.metadata()?));
        }
        Ok(bytes)
    }

    pub fn payload(&self) -> io::Result<File> {
        self.payload.try_clone()
    }

    pub fn publish(&self, length: u64) -> io::Result<()> {
        let payload = self.payload.metadata()?;
        let directory = statat(
            &self.parent,
            self.dir_name.as_str(),
            AtFlags::SYMLINK_NOFOLLOW,
        )?;
        if !matches_file(&self.directory.metadata()?, &directory) {
            return Err(identity_error());
        }
        let source = statat(&self.directory, PAYLOAD, AtFlags::SYMLINK_NOFOLLOW)?;
        if !matches_file(&payload, &source) || payload.len() != length {
            return Err(identity_error());
        }
        let (parent_now, _) = open_parent(&self.path)?;
        if identity(&parent_now.metadata()?) != identity(&self.parent.metadata()?) {
            return Err(identity_error());
        }
        match linkat(
            &self.directory,
            PAYLOAD,
            &self.parent,
            &self.destination,
            AtFlags::empty(),
        ) {
            Ok(()) | Err(Errno::EXIST) => {}
            Err(error) => return Err(error.into()),
        }
        let target = statat(&self.parent, &self.destination, AtFlags::SYMLINK_NOFOLLOW)?;
        if !matches_file(&payload, &target) {
            return Err(io::Error::new(
                ErrorKind::AlreadyExists,
                "destination already exists",
            ));
        }
        self.payload.sync_all()?;
        self.parent.sync_all()
    }

    pub fn cleanup_completed(
        destination: &Path,
        nonce: &str,
        expected: &[u8; IDENTITY_BYTES],
    ) -> io::Result<()> {
        let (parent, _) = open_parent(destination)?;
        if identity(&parent.metadata()?) != expected[..16] {
            return Err(identity_error());
        }
        let dir_name = stage_name(nonce)?;
        let directory = match open_directory(&parent, dir_name.as_ref()) {
            Ok(directory) => directory,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        check_private(&directory)?;
        if identity(&directory.metadata()?) != expected[16..32] {
            return Err(identity_error());
        }
        match statat(&directory, PAYLOAD, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(payload) => {
                if stat_identity(&payload) != expected[32..] {
                    return Err(identity_error());
                }
                unlinkat(&directory, PAYLOAD, AtFlags::empty())?;
            }
            Err(Errno::NOENT) => {}
            Err(error) => return Err(error.into()),
        }
        if !matches_file(
            &directory.metadata()?,
            &statat(&parent, dir_name.as_str(), AtFlags::SYMLINK_NOFOLLOW)?,
        ) {
            return Err(identity_error());
        }
        unlinkat(&parent, dir_name.as_str(), AtFlags::REMOVEDIR)?;
        parent.sync_all()
    }
}

fn open_parent(path: &Path) -> io::Result<(File, OsString)> {
    let mut components = path.components();
    if components.next() != Some(Component::RootDir) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "destination path must be absolute",
        ));
    }
    let mut directory = File::open("/")?;
    let Some(Component::Normal(name)) = components.next_back() else {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "expected a destination filename",
        ));
    };
    for component in components {
        let Component::Normal(part) = component else {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "invalid destination path component",
            ));
        };
        directory = open_directory(&directory, part)?;
    }
    Ok((directory, name.to_owned()))
}

fn open_directory(parent: &File, name: &OsStr) -> io::Result<File> {
    Ok(File::from(openat(
        parent,
        name,
        OFlags::RDONLY | DIRECTORY,
        Mode::empty(),
    )?))
}

fn check_private(directory: &File) -> io::Result<()> {
    let metadata = directory.metadata()?;
    if metadata.uid() != geteuid().as_raw() || metadata.mode() & 0o077 != 0 {
        return Err(identity_error());
    }
    Ok(())
}

fn stage_name(nonce: &str) -> io::Result<String> {
    if nonce.len() != 32 || !nonce.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "invalid staging file identifier",
        ));
    }
    Ok(format!(".download-manager-{nonce}"))
}

fn identity(metadata: &std::fs::Metadata) -> [u8; 16] {
    encode_identity(metadata.dev(), metadata.ino())
}

fn encode_identity(device: u64, inode: u64) -> [u8; 16] {
    let mut encoded = [0; 16];
    encoded[..8].copy_from_slice(&device.to_le_bytes());
    encoded[8..].copy_from_slice(&inode.to_le_bytes());
    encoded
}

// Device and inode integer types vary across Unix platforms.
#[allow(clippy::unnecessary_cast)]
fn stat_identity(stat: &Stat) -> [u8; 16] {
    encode_identity(stat.st_dev as u64, stat.st_ino as u64)
}

fn matches_file(file: &std::fs::Metadata, stat: &Stat) -> bool {
    identity(file) == stat_identity(stat)
}

fn identity_error() -> io::Error {
    io::Error::new(
        ErrorKind::InvalidData,
        "staged file or destination directory changed",
    )
}
