use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use base64::Engine;
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use libc::{c_int, EACCES, EEXIST, EINVAL, EIO, EISDIR, ENOENT, ENOTDIR};
use meshfs_types::{
    DeleteRequest, ListDirectoryResponse, MetaResponse, MkdirRequest, NodeKind, RenameRequest,
    UploadCommitRequest, UploadInitRequest, UploadInitResponse, UploadPartRequest,
};
use reqwest::{blocking::Client, StatusCode};

#[derive(Debug, Clone, Copy)]
pub struct FuseMountOptions {
    pub allow_other: bool,
    pub auto_unmount: bool,
    pub read_only: bool,
}

pub fn run_fuse_mount(
    server: &str,
    token: &str,
    target: &Path,
    options: FuseMountOptions,
) -> anyhow::Result<()> {
    std::fs::create_dir_all(target)
        .with_context(|| format!("failed to create mount target {}", target.display()))?;

    let api = MeshfsBlockingApi::new(server.to_string(), token.to_string())?;
    let root = api.meta("/")?;
    if root.is_none() {
        return Err(anyhow::anyhow!("remote root path not found"));
    }

    let mut mount_options = vec![
        MountOption::FSName("meshfs".to_string()),
        MountOption::DefaultPermissions,
    ];
    if options.allow_other {
        mount_options.push(MountOption::AllowOther);
    }
    if options.auto_unmount {
        mount_options.push(MountOption::AutoUnmount);
    }
    if options.read_only {
        mount_options.push(MountOption::RO);
    }

    println!(
        "fuse mount started: server={} target={} read_only={}",
        server,
        target.display(),
        options.read_only
    );

    let fs = MeshfsFuse::new(api);
    fuser::mount2(fs, target, &mount_options).context("fuse mount failed")
}

struct MeshfsFuse {
    api: MeshfsBlockingApi,
    uid: u32,
    gid: u32,
    ttl: Duration,
    state: Mutex<MeshfsFuseState>,
}

#[derive(Default)]
struct MeshfsFuseState {
    next_inode: u64,
    next_handle: u64,
    inode_to_path: HashMap<u64, String>,
    path_to_inode: HashMap<String, u64>,
    open_files: HashMap<u64, OpenFile>,
}

struct OpenFile {
    ino: u64,
    path: String,
    data: Vec<u8>,
    loaded: bool,
    dirty: bool,
}

impl MeshfsFuse {
    fn new(api: MeshfsBlockingApi) -> Self {
        let mut state = MeshfsFuseState {
            next_inode: 2,
            next_handle: 1,
            ..Default::default()
        };
        state.inode_to_path.insert(1, "/".to_string());
        state.path_to_inode.insert("/".to_string(), 1);

        Self {
            api,
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            ttl: Duration::from_secs(1),
            state: Mutex::new(state),
        }
    }

    fn path_for_inode(&self, ino: u64) -> Result<String, c_int> {
        let guard = self.state.lock().map_err(|_| EIO)?;
        guard.inode_to_path.get(&ino).cloned().ok_or(ENOENT)
    }

    fn ensure_inode_for_path(&self, path: &str) -> u64 {
        let mut guard = self.state.lock().expect("meshfs fuse state poisoned");
        ensure_inode_for_path_locked(&mut guard, path)
    }

    fn child_path(&self, parent_ino: u64, name: &OsStr) -> Result<String, c_int> {
        let name = name.to_str().ok_or(EINVAL)?;
        if name.is_empty() {
            return Err(EINVAL);
        }
        let parent_path = self.path_for_inode(parent_ino)?;
        let raw = if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        };
        normalize_remote_path(&raw).map_err(|_| EINVAL)
    }

    fn attr_from_meta(&self, ino: u64, meta: &MetaResponse) -> FileAttr {
        let size = meta.head_version.as_ref().map(|v| v.size).unwrap_or(0);
        let kind = node_kind_to_file_type(&meta.node.kind);
        let perm = match kind {
            FileType::Directory => 0o755,
            _ => 0o644,
        };
        let nlink = if matches!(kind, FileType::Directory) {
            2
        } else {
            1
        };
        let ts = meta
            .head_version
            .as_ref()
            .map(|v| chrono_to_system_time(v.committed_at))
            .unwrap_or_else(SystemTime::now);

        file_attr(FileAttrInput {
            ino,
            size,
            kind,
            perm,
            nlink,
            uid: self.uid,
            gid: self.gid,
            ts,
        })
    }

    fn remove_cached_path(&self, path: &str) {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(_) => return,
        };

        let affected: Vec<String> = guard
            .path_to_inode
            .keys()
            .filter(|candidate| {
                *candidate == path || candidate.starts_with(&(path.to_string() + "/"))
            })
            .cloned()
            .collect();

        for item in affected {
            if let Some(ino) = guard.path_to_inode.remove(&item) {
                guard.inode_to_path.remove(&ino);
                guard.open_files.retain(|_, open| open.ino != ino);
            }
        }

        guard.path_to_inode.insert("/".to_string(), 1);
        guard.inode_to_path.insert(1, "/".to_string());
    }

    fn rename_cached_path(&self, from: &str, to: &str) {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(_) => return,
        };

        let mut updates: Vec<(String, String)> = Vec::new();
        for candidate in guard.path_to_inode.keys() {
            if candidate == from || candidate.starts_with(&(from.to_string() + "/")) {
                let suffix = candidate.trim_start_matches(from);
                updates.push((candidate.clone(), format!("{to}{suffix}")));
            }
        }

        for (old_path, new_path) in updates {
            if let Some(ino) = guard.path_to_inode.remove(&old_path) {
                guard.path_to_inode.insert(new_path.clone(), ino);
                guard.inode_to_path.insert(ino, new_path.clone());
                for open in guard.open_files.values_mut().filter(|open| open.ino == ino) {
                    open.path = new_path.clone();
                }
            }
        }
    }

    fn allocate_handle(&self, ino: u64, path: String) -> Result<u64, c_int> {
        let mut guard = self.state.lock().map_err(|_| EIO)?;
        let fh = guard.next_handle;
        guard.next_handle += 1;
        guard.open_files.insert(
            fh,
            OpenFile {
                ino,
                path,
                data: Vec::new(),
                loaded: false,
                dirty: false,
            },
        );
        Ok(fh)
    }

    fn ensure_open_file_loaded(&self, fh: u64) -> Result<(), c_int> {
        let should_load = {
            let guard = self.state.lock().map_err(|_| EIO)?;
            let open = guard.open_files.get(&fh).ok_or(ENOENT)?;
            !open.loaded
        };

        if !should_load {
            return Ok(());
        }

        let path = {
            let guard = self.state.lock().map_err(|_| EIO)?;
            guard
                .open_files
                .get(&fh)
                .map(|o| o.path.clone())
                .ok_or(ENOENT)?
        };

        let loaded = match self.api.download(&path) {
            Ok(bytes) => bytes,
            Err(ApiError::NotFound) => Vec::new(),
            Err(err) => return Err(api_error_to_errno(&err)),
        };

        let mut guard = self.state.lock().map_err(|_| EIO)?;
        let open = guard.open_files.get_mut(&fh).ok_or(ENOENT)?;
        if !open.loaded {
            open.data = loaded;
            open.loaded = true;
        }
        Ok(())
    }

    fn flush_handle(&self, fh: u64) -> Result<(), c_int> {
        let maybe_upload = {
            let guard = self.state.lock().map_err(|_| EIO)?;
            let open = guard.open_files.get(&fh).ok_or(ENOENT)?;
            if open.dirty {
                Some((open.path.clone(), open.data.clone()))
            } else {
                None
            }
        };

        if let Some((path, data)) = maybe_upload {
            self.api
                .upload_bytes(&path, &data)
                .map_err(|err| api_error_to_errno(&err))?;
            let mut guard = self.state.lock().map_err(|_| EIO)?;
            if let Some(open) = guard.open_files.get_mut(&fh) {
                open.dirty = false;
            }
        }

        Ok(())
    }
}

impl Filesystem for MeshfsFuse {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        let meta = match self.api.meta(&path) {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(err) => {
                reply.error(api_error_to_errno(&err));
                return;
            }
        };

        let ino = self.ensure_inode_for_path(&meta.node.path);
        let attr = self.attr_from_meta(ino, &meta);
        reply.entry(&self.ttl, &attr, 0);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let path = match self.path_for_inode(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        let meta = match self.api.meta(&path) {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(err) => {
                reply.error(api_error_to_errno(&err));
                return;
            }
        };

        let resolved_ino = self.ensure_inode_for_path(&meta.node.path);
        let attr = self.attr_from_meta(resolved_ino, &meta);
        reply.attr(&self.ttl, &attr);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let path = match self.path_for_inode(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        let listed = match self.api.list_directory(&path) {
            Ok(listed) => listed,
            Err(ApiError::NotFound) => {
                reply.error(ENOENT);
                return;
            }
            Err(ApiError::Conflict) => {
                reply.error(ENOTDIR);
                return;
            }
            Err(err) => {
                reply.error(api_error_to_errno(&err));
                return;
            }
        };

        let parent_path = parent_remote_path(&path);
        let parent_ino = self.ensure_inode_for_path(&parent_path);

        let mut entries: Vec<(u64, FileType, String)> =
            Vec::with_capacity(listed.entries.len() + 2);
        entries.push((ino, FileType::Directory, ".".to_string()));
        entries.push((parent_ino, FileType::Directory, "..".to_string()));

        for entry in listed.entries {
            let child_ino = self.ensure_inode_for_path(&entry.node.path);
            let file_type = node_kind_to_file_type(&entry.node.kind);
            entries.push((child_ino, file_type, entry.node.name));
        }

        let start = if offset <= 0 { 0 } else { offset as usize };
        for (idx, (entry_ino, file_type, name)) in entries.into_iter().enumerate().skip(start) {
            if reply.add(entry_ino, (idx + 1) as i64, file_type, name) {
                break;
            }
        }

        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let path = match self.path_for_inode(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        let meta = match self.api.meta(&path) {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(err) => {
                reply.error(api_error_to_errno(&err));
                return;
            }
        };

        if meta.node.kind != NodeKind::File {
            reply.error(EISDIR);
            return;
        }

        let fh = match self.allocate_handle(ino, path) {
            Ok(fh) => fh,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        reply.opened(fh, flags as u32);
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        if let Err(err) = self.api.upload_bytes(&path, &[]) {
            reply.error(api_error_to_errno(&err));
            return;
        }

        let meta = match self.api.meta(&path) {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(err) => {
                reply.error(api_error_to_errno(&err));
                return;
            }
        };

        let ino = self.ensure_inode_for_path(&path);
        let attr = self.attr_from_meta(ino, &meta);
        let fh = match self.allocate_handle(ino, path) {
            Ok(fh) => fh,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        reply.created(&self.ttl, &attr, 0, fh, flags as u32);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }

        let has_handle = {
            let guard = match self.state.lock() {
                Ok(g) => g,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };
            guard.open_files.contains_key(&fh)
        };

        let bytes = if has_handle {
            if let Err(errno) = self.ensure_open_file_loaded(fh) {
                reply.error(errno);
                return;
            }
            let guard = match self.state.lock() {
                Ok(g) => g,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };
            match guard.open_files.get(&fh) {
                Some(open) => open.data.clone(),
                None => {
                    reply.error(ENOENT);
                    return;
                }
            }
        } else {
            let path = match self.path_for_inode(ino) {
                Ok(path) => path,
                Err(errno) => {
                    reply.error(errno);
                    return;
                }
            };
            match self.api.download(&path) {
                Ok(bytes) => bytes,
                Err(err) => {
                    reply.error(api_error_to_errno(&err));
                    return;
                }
            }
        };

        let start = offset as usize;
        if start >= bytes.len() {
            reply.data(&[]);
            return;
        }

        let end = start.saturating_add(size as usize).min(bytes.len());
        reply.data(&bytes[start..end]);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }

        if let Err(errno) = self.ensure_open_file_loaded(fh) {
            reply.error(errno);
            return;
        }

        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };

        let Some(open) = guard.open_files.get_mut(&fh) else {
            reply.error(ENOENT);
            return;
        };

        let start = offset as usize;
        if open.data.len() < start {
            open.data.resize(start, 0);
        }
        let end = start.saturating_add(data.len());
        if open.data.len() < end {
            open.data.resize(end, 0);
        }
        open.data[start..end].copy_from_slice(data);
        open.dirty = true;

        reply.written(data.len() as u32);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        match self.flush_handle(fh) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if let Err(errno) = self.flush_handle(fh) {
            reply.error(errno);
            return;
        }

        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };
        guard.open_files.remove(&fh);
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        match self.flush_handle(fh) {
            Ok(()) => reply.ok(),
            Err(errno) => reply.error(errno),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let path = match self.path_for_inode(ino) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        if let Some(target_size) = size {
            let mut bytes = match self.api.download(&path) {
                Ok(bytes) => bytes,
                Err(ApiError::NotFound) => Vec::new(),
                Err(err) => {
                    reply.error(api_error_to_errno(&err));
                    return;
                }
            };

            let Ok(target_len) = usize::try_from(target_size) else {
                reply.error(EINVAL);
                return;
            };

            bytes.resize(target_len, 0);
            if let Err(err) = self.api.upload_bytes(&path, &bytes) {
                reply.error(api_error_to_errno(&err));
                return;
            }

            if let Ok(mut guard) = self.state.lock() {
                for open in guard.open_files.values_mut().filter(|open| open.ino == ino) {
                    open.data = bytes.clone();
                    open.loaded = true;
                    open.dirty = false;
                }
            }
        }

        let meta = match self.api.meta(&path) {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(err) => {
                reply.error(api_error_to_errno(&err));
                return;
            }
        };
        let attr = self.attr_from_meta(ino, &meta);
        reply.attr(&self.ttl, &attr);
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        if let Err(err) = self.api.mkdir(&path) {
            reply.error(api_error_to_errno(&err));
            return;
        }

        let meta = match self.api.meta(&path) {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                reply.error(ENOENT);
                return;
            }
            Err(err) => {
                reply.error(api_error_to_errno(&err));
                return;
            }
        };

        let ino = self.ensure_inode_for_path(&path);
        let attr = self.attr_from_meta(ino, &meta);
        reply.entry(&self.ttl, &attr, 0);
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        match self.api.delete(&path) {
            Ok(()) => {
                self.remove_cached_path(&path);
                reply.ok();
            }
            Err(err) => reply.error(api_error_to_errno(&err)),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.unlink(_req, parent, name, reply)
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let from_path = match self.child_path(parent, name) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let to_path = match self.child_path(newparent, newname) {
            Ok(path) => path,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        match self.api.rename(&from_path, &to_path) {
            Ok(()) => {
                self.rename_cached_path(&from_path, &to_path);
                reply.ok();
            }
            Err(err) => reply.error(api_error_to_errno(&err)),
        }
    }
}

#[derive(Debug)]
enum ApiError {
    NotFound,
    Conflict,
    Forbidden,
    InvalidRequest,
    Transport(anyhow::Error),
}

fn api_error_to_errno(err: &ApiError) -> c_int {
    match err {
        ApiError::NotFound => ENOENT,
        ApiError::Conflict => EEXIST,
        ApiError::Forbidden => EACCES,
        ApiError::InvalidRequest => EINVAL,
        ApiError::Transport(_) => EIO,
    }
}

fn status_to_api_error(status: StatusCode) -> ApiError {
    match status {
        StatusCode::NOT_FOUND => ApiError::NotFound,
        StatusCode::CONFLICT => ApiError::Conflict,
        StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => ApiError::Forbidden,
        StatusCode::BAD_REQUEST => ApiError::InvalidRequest,
        _ => ApiError::Transport(anyhow::anyhow!("unexpected http status: {status}")),
    }
}

struct MeshfsBlockingApi {
    client: Client,
    server: String,
    token: String,
}

impl MeshfsBlockingApi {
    fn new(server: String, token: String) -> anyhow::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .context("failed to build blocking http client")?;

        Ok(Self {
            client,
            server,
            token,
        })
    }

    fn meta(&self, path: &str) -> Result<Option<MetaResponse>, ApiError> {
        let resp = self.send_with_retry(
            || {
                self.client
                    .get(format!("{}/files/meta", self.server))
                    .bearer_auth(&self.token)
                    .query(&[("path", path)])
            },
            "files meta",
        )?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(status_to_api_error(resp.status()));
        }

        resp.json::<MetaResponse>()
            .map(Some)
            .map_err(|err| ApiError::Transport(anyhow::Error::new(err)))
    }

    fn list_directory(&self, path: &str) -> Result<ListDirectoryResponse, ApiError> {
        let resp = self.send_with_retry(
            || {
                self.client
                    .get(format!("{}/files/list", self.server))
                    .bearer_auth(&self.token)
                    .query(&[("path", path)])
            },
            "files list",
        )?;

        if !resp.status().is_success() {
            return Err(status_to_api_error(resp.status()));
        }

        resp.json::<ListDirectoryResponse>()
            .map_err(|err| ApiError::Transport(anyhow::Error::new(err)))
    }

    fn download(&self, path: &str) -> Result<Vec<u8>, ApiError> {
        let resp = self.send_with_retry(
            || {
                self.client
                    .get(format!("{}/files/download", self.server))
                    .bearer_auth(&self.token)
                    .query(&[("path", path)])
            },
            "files download",
        )?;

        if !resp.status().is_success() {
            return Err(status_to_api_error(resp.status()));
        }

        resp.bytes()
            .map(|b| b.to_vec())
            .map_err(|err| ApiError::Transport(anyhow::Error::new(err)))
    }

    fn mkdir(&self, path: &str) -> Result<(), ApiError> {
        let resp = self.send_with_retry(
            || {
                self.client
                    .post(format!("{}/files/mkdir", self.server))
                    .bearer_auth(&self.token)
                    .json(&MkdirRequest {
                        path: path.to_string(),
                    })
            },
            "files mkdir",
        )?;

        if !resp.status().is_success() {
            return Err(status_to_api_error(resp.status()));
        }

        Ok(())
    }

    fn delete(&self, path: &str) -> Result<(), ApiError> {
        let resp = self.send_with_retry(
            || {
                self.client
                    .delete(format!("{}/files", self.server))
                    .bearer_auth(&self.token)
                    .json(&DeleteRequest {
                        path: path.to_string(),
                    })
            },
            "files delete",
        )?;

        if !resp.status().is_success() {
            return Err(status_to_api_error(resp.status()));
        }

        Ok(())
    }

    fn rename(&self, from_path: &str, to_path: &str) -> Result<(), ApiError> {
        let resp = self.send_with_retry(
            || {
                self.client
                    .post(format!("{}/files/rename", self.server))
                    .bearer_auth(&self.token)
                    .json(&RenameRequest {
                        from_path: from_path.to_string(),
                        to_path: to_path.to_string(),
                    })
            },
            "files rename",
        )?;

        if !resp.status().is_success() {
            return Err(status_to_api_error(resp.status()));
        }

        Ok(())
    }

    fn upload_bytes(&self, path: &str, data: &[u8]) -> Result<(), ApiError> {
        let upload_init = self.send_with_retry(
            || {
                self.client
                    .post(format!("{}/files/upload/init", self.server))
                    .bearer_auth(&self.token)
                    .json(&UploadInitRequest {
                        path: path.to_string(),
                        size_hint: Some(data.len() as u64),
                        content_hash: None,
                        writer_device_id: Some("fuse".to_string()),
                    })
            },
            "files upload init",
        )?;

        if !upload_init.status().is_success() {
            return Err(status_to_api_error(upload_init.status()));
        }

        let init_response = upload_init
            .json::<UploadInitResponse>()
            .map_err(|err| ApiError::Transport(anyhow::Error::new(err)))?;

        const PART_SIZE: usize = 1024 * 1024;
        let chunks: Vec<&[u8]> = if data.is_empty() {
            vec![&[]]
        } else {
            data.chunks(PART_SIZE).collect()
        };

        for (idx, chunk) in chunks.into_iter().enumerate() {
            let data_base64 = base64::engine::general_purpose::STANDARD.encode(chunk);
            let part_response = self.send_with_retry(
                || {
                    self.client
                        .put(format!("{}/files/upload/part", self.server))
                        .bearer_auth(&self.token)
                        .json(&UploadPartRequest {
                            upload_id: init_response.upload_id.clone(),
                            part_number: (idx as u32) + 1,
                            data_base64: data_base64.clone(),
                        })
                },
                "files upload part",
            )?;

            if !part_response.status().is_success() {
                return Err(status_to_api_error(part_response.status()));
            }
        }

        let commit_response = self.send_with_retry(
            || {
                self.client
                    .post(format!("{}/files/upload/commit", self.server))
                    .bearer_auth(&self.token)
                    .json(&UploadCommitRequest {
                        upload_id: init_response.upload_id.clone(),
                    })
            },
            "files upload commit",
        )?;

        if !commit_response.status().is_success() {
            return Err(status_to_api_error(commit_response.status()));
        }

        Ok(())
    }

    fn send_with_retry<F>(
        &self,
        mut make_request: F,
        operation: &str,
    ) -> Result<reqwest::blocking::Response, ApiError>
    where
        F: FnMut() -> reqwest::blocking::RequestBuilder,
    {
        let max_attempts = 3u64;
        let mut attempt = 0u64;

        loop {
            attempt += 1;
            match make_request().send() {
                Ok(resp) => {
                    if is_retryable_status(resp.status()) && attempt < max_attempts {
                        thread::sleep(Duration::from_millis(200 * attempt));
                        continue;
                    }
                    return Ok(resp);
                }
                Err(err) => {
                    if is_retryable_error(&err) && attempt < max_attempts {
                        thread::sleep(Duration::from_millis(200 * attempt));
                        continue;
                    }
                    return Err(ApiError::Transport(
                        anyhow::Error::new(err).context(format!("{operation} request failed")),
                    ));
                }
            }
        }
    }
}

fn ensure_inode_for_path_locked(state: &mut MeshfsFuseState, path: &str) -> u64 {
    if let Some(existing) = state.path_to_inode.get(path).copied() {
        return existing;
    }

    let ino = state.next_inode;
    state.next_inode += 1;
    state.path_to_inode.insert(path.to_string(), ino);
    state.inode_to_path.insert(ino, path.to_string());
    ino
}

fn node_kind_to_file_type(kind: &NodeKind) -> FileType {
    match kind {
        NodeKind::Dir => FileType::Directory,
        NodeKind::File => FileType::RegularFile,
    }
}

fn parent_remote_path(path: &str) -> String {
    if path == "/" {
        return "/".to_string();
    }

    match path.rfind('/') {
        Some(0) => "/".to_string(),
        Some(idx) => path[..idx].to_string(),
        None => "/".to_string(),
    }
}

struct FileAttrInput {
    ino: u64,
    size: u64,
    kind: FileType,
    perm: u16,
    nlink: u32,
    uid: u32,
    gid: u32,
    ts: SystemTime,
}

#[cfg(target_os = "macos")]
fn file_attr(input: FileAttrInput) -> FileAttr {
    FileAttr {
        ino: input.ino,
        size: input.size,
        blocks: input.size.div_ceil(512),
        atime: input.ts,
        mtime: input.ts,
        ctime: input.ts,
        crtime: input.ts,
        kind: input.kind,
        perm: input.perm,
        nlink: input.nlink,
        uid: input.uid,
        gid: input.gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

#[cfg(not(target_os = "macos"))]
fn file_attr(input: FileAttrInput) -> FileAttr {
    FileAttr {
        ino: input.ino,
        size: input.size,
        blocks: input.size.div_ceil(512),
        atime: input.ts,
        mtime: input.ts,
        ctime: input.ts,
        kind: input.kind,
        perm: input.perm,
        nlink: input.nlink,
        uid: input.uid,
        gid: input.gid,
        rdev: 0,
        blksize: 4096,
    }
}

fn chrono_to_system_time(ts: chrono::DateTime<chrono::Utc>) -> SystemTime {
    let sec = ts.timestamp();
    let nanos = ts.timestamp_subsec_nanos();
    if sec >= 0 {
        UNIX_EPOCH + Duration::new(sec as u64, nanos)
    } else {
        UNIX_EPOCH
            .checked_sub(Duration::new((-sec) as u64, nanos))
            .unwrap_or(UNIX_EPOCH)
    }
}

fn normalize_remote_path(raw: &str) -> anyhow::Result<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(anyhow::anyhow!("remote path cannot be empty"));
    }

    let mut parts = Vec::new();
    for part in raw.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return Err(anyhow::anyhow!("path traversal not allowed"));
        }
        parts.push(part);
    }

    if parts.is_empty() {
        return Ok("/".to_string());
    }

    Ok(format!("/{}", parts.join("/")))
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn is_retryable_error(err: &reqwest::Error) -> bool {
    err.is_connect() || err.is_timeout()
}

#[cfg(test)]
mod tests {
    use super::{normalize_remote_path, parent_remote_path};

    #[test]
    fn normalize_remote_path_blocks_traversal() {
        assert_eq!(normalize_remote_path("/docs/a.txt").unwrap(), "/docs/a.txt");
        assert!(normalize_remote_path("../etc/passwd").is_err());
    }

    #[test]
    fn parent_path_for_root_and_child() {
        assert_eq!(parent_remote_path("/"), "/");
        assert_eq!(parent_remote_path("/docs"), "/");
        assert_eq!(parent_remote_path("/docs/a.txt"), "/docs");
    }
}
