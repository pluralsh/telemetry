// Copyright 2026 Plural, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use std::pin::Pin;

use futures::{Stream, StreamExt};
use prost::Message;
use proto::pseudofs::v1::pseudo_fs_server::{PseudoFs as PseudoFsApi, PseudoFsServer};
use proto::pseudofs::v1::{
    BoolResponse, BytesResponse, DirectoryEntry, Empty, ErrorDetail, FileChunk, FileHandle,
    FileKind, GlobRequest, GrepLine, GrepMatch, GrepRequest, MkdirRequest, MutationPathRequest,
    OpenRequest, PathRequest, PathResponse, RenameRequest, StatResponse, StreamFileRequest,
    TextResponse, WriteBytesRequest, WriteFileRequest, WriteResponse, WriteTextRequest,
    write_file_request,
};
use pseudofs::{Durability, Error, GlobOptions, GrepOptions, PseudoFs};
use tonic::{Code, Request, Response, Status, Streaming};

use crate::config::SearchLimits;

#[derive(Clone)]
pub struct Service {
    fs: PseudoFs,
    max_unary_file_size_bytes: u64,
    search: SearchLimits,
}

impl Service {
    pub fn new(fs: PseudoFs, max_unary_file_size_bytes: u64, search: SearchLimits) -> Self {
        Self {
            fs,
            max_unary_file_size_bytes,
            search,
        }
    }

    #[allow(clippy::result_large_err)]
    fn ensure_unary_write(&self, size: usize) -> Result<(), Status> {
        if size as u64 > self.max_unary_file_size_bytes {
            return Err(status(Error::ResourceExhausted(format!(
                "payload size {size} exceeds unary limit {}; use WriteFile",
                self.max_unary_file_size_bytes
            ))));
        }
        Ok(())
    }

    pub fn into_server(
        self,
        max_decoding_message_bytes: usize,
        max_encoding_message_bytes: usize,
    ) -> PseudoFsServer<Self> {
        PseudoFsServer::new(self)
            .max_decoding_message_size(max_decoding_message_bytes)
            .max_encoding_message_size(max_encoding_message_bytes)
    }
}

type FileStream = Pin<Box<dyn Stream<Item = Result<FileChunk, Status>> + Send + 'static>>;
type DirectoryStream = Pin<Box<dyn Stream<Item = Result<DirectoryEntry, Status>> + Send + 'static>>;
type GrepStream = Pin<Box<dyn Stream<Item = Result<GrepMatch, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl PseudoFsApi for Service {
    type StreamFileStream = FileStream;
    type IterdirStream = DirectoryStream;
    type GlobStream = DirectoryStream;
    type GrepStream = GrepStream;

    async fn exists(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        let request = request.into_inner();
        let value = self
            .fs
            .exists(&request.tenant, &request.path)
            .await
            .map_err(status)?;
        Ok(Response::new(BoolResponse { value }))
    }

    async fn is_file(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        let request = request.into_inner();
        let value = self
            .fs
            .is_file(&request.tenant, &request.path)
            .await
            .map_err(status)?;
        Ok(Response::new(BoolResponse { value }))
    }

    async fn is_dir(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        let request = request.into_inner();
        let value = self
            .fs
            .is_dir(&request.tenant, &request.path)
            .await
            .map_err(status)?;
        Ok(Response::new(BoolResponse { value }))
    }

    async fn is_symlink(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<BoolResponse>, Status> {
        let request = request.into_inner();
        let value = self
            .fs
            .is_symlink(&request.tenant, &request.path)
            .await
            .map_err(status)?;
        Ok(Response::new(BoolResponse { value }))
    }

    async fn read_text(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<TextResponse>, Status> {
        let request = request.into_inner();
        let value = self
            .fs
            .read_text_within(
                &request.tenant,
                &request.path,
                self.max_unary_file_size_bytes,
            )
            .await
            .map_err(status)?;
        Ok(Response::new(TextResponse { value }))
    }

    async fn read_bytes(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<BytesResponse>, Status> {
        let request = request.into_inner();
        let value = self
            .fs
            .read_bytes_within(
                &request.tenant,
                &request.path,
                self.max_unary_file_size_bytes,
            )
            .await
            .map_err(status)?;
        Ok(Response::new(BytesResponse { value }))
    }

    async fn write_text(
        &self,
        request: Request<WriteTextRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let request = request.into_inner();
        self.ensure_unary_write(request.value.len())?;
        let size = self
            .fs
            .write_text(
                &request.tenant,
                &request.path,
                request.value,
                durability(request.durability),
            )
            .await
            .map_err(status)?;
        Ok(Response::new(WriteResponse { size }))
    }

    async fn write_bytes(
        &self,
        request: Request<WriteBytesRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let request = request.into_inner();
        self.ensure_unary_write(request.value.len())?;
        let size = self
            .fs
            .write_bytes(
                &request.tenant,
                &request.path,
                request.value,
                durability(request.durability),
            )
            .await
            .map_err(status)?;
        Ok(Response::new(WriteResponse { size }))
    }

    async fn append_text(
        &self,
        request: Request<WriteTextRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let request = request.into_inner();
        self.ensure_unary_write(request.value.len())?;
        let size = self
            .fs
            .append_text(
                &request.tenant,
                &request.path,
                request.value,
                durability(request.durability),
            )
            .await
            .map_err(status)?;
        Ok(Response::new(WriteResponse { size }))
    }

    async fn append_bytes(
        &self,
        request: Request<WriteBytesRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let request = request.into_inner();
        self.ensure_unary_write(request.value.len())?;
        let size = self
            .fs
            .append_bytes(
                &request.tenant,
                &request.path,
                request.value,
                durability(request.durability),
            )
            .await
            .map_err(status)?;
        Ok(Response::new(WriteResponse { size }))
    }

    async fn open(&self, request: Request<OpenRequest>) -> Result<Response<FileHandle>, Status> {
        let request = request.into_inner();
        let handle = self
            .fs
            .open_file(
                &request.tenant,
                &request.path,
                &request.mode,
                durability(request.durability),
            )
            .await
            .map_err(status)?;
        Ok(Response::new(FileHandle {
            path: handle.path,
            mode: handle.mode,
            position: handle.position,
            tenant: handle.tenant,
        }))
    }

    async fn mkdir(&self, request: Request<MkdirRequest>) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        self.fs
            .mkdir(
                &request.tenant,
                &request.path,
                request.parents,
                request.exist_ok,
            )
            .await
            .map_err(status)?;
        Ok(Response::new(Empty {}))
    }

    async fn unlink(
        &self,
        request: Request<MutationPathRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        self.fs
            .unlink(
                &request.tenant,
                &request.path,
                durability(request.durability),
            )
            .await
            .map_err(status)?;
        Ok(Response::new(Empty {}))
    }

    async fn rmdir(
        &self,
        request: Request<MutationPathRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        self.fs
            .rmdir(
                &request.tenant,
                &request.path,
                durability(request.durability),
            )
            .await
            .map_err(status)?;
        Ok(Response::new(Empty {}))
    }

    #[allow(clippy::result_large_err)]
    async fn iterdir(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<Self::IterdirStream>, Status> {
        let request = request.into_inner();
        let entries = self
            .fs
            .stream_dir(&request.tenant, &request.path)
            .await
            .map_err(status)?;
        Ok(Response::new(directory_stream(entries)))
    }

    #[allow(clippy::result_large_err)]
    async fn glob(
        &self,
        request: Request<GlobRequest>,
    ) -> Result<Response<Self::GlobStream>, Status> {
        let request = request.into_inner();
        let max_results = limit(
            request.max_results.into(),
            self.search.default_glob_results.into(),
            self.search.max_glob_results.into(),
        );
        let options = GlobOptions {
            max_results: max_results as usize,
            max_scanned_entries: self.search.max_scanned_entries as usize,
        };
        let entries = self
            .fs
            .glob(&request.tenant, &request.pattern, options)
            .await
            .map_err(status)?;
        Ok(Response::new(directory_stream(entries)))
    }

    #[allow(clippy::result_large_err)]
    async fn grep(
        &self,
        request: Request<GrepRequest>,
    ) -> Result<Response<Self::GrepStream>, Status> {
        let request = request.into_inner();
        let search = &self.search;
        let context = search.max_grep_context_lines;
        let options = GrepOptions {
            pattern: request.pattern,
            fixed_string: request.fixed_string,
            case_insensitive: request.case_insensitive,
            before_context: request.before_context.min(context) as usize,
            after_context: request.after_context.min(context) as usize,
            max_matches: limit(
                request.max_matches.into(),
                search.default_grep_matches.into(),
                search.max_grep_matches.into(),
            ) as usize,
            max_file_size_bytes: limit(
                request.max_file_size_bytes,
                search.default_grep_file_size_bytes,
                search.max_grep_file_size_bytes,
            ),
            concurrency: search.grep_concurrency as usize,
            max_scanned_entries: search.max_scanned_entries as usize,
        };
        let matches = self
            .fs
            .grep(&request.tenant, &request.path, options)
            .await
            .map_err(status)?;
        let stream = tokio_stream::wrappers::ReceiverStream::new(matches).map(|result| {
            result
                .map(|found| GrepMatch {
                    path: found.path,
                    line_number: found.line_number,
                    line: found.line,
                    before: found.before.into_iter().map(grep_line).collect(),
                    after: found.after.into_iter().map(grep_line).collect(),
                })
                .map_err(status)
        });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn stat(&self, request: Request<PathRequest>) -> Result<Response<StatResponse>, Status> {
        let request = request.into_inner();
        let stat = self
            .fs
            .stat(&request.tenant, &request.path)
            .await
            .map_err(status)?;
        Ok(Response::new(StatResponse {
            kind: file_kind(stat.kind),
            mode: stat.mode,
            size: stat.size,
            atime: stat.atime,
            mtime: stat.mtime,
            ctime: stat.ctime,
        }))
    }

    async fn rename(
        &self,
        request: Request<RenameRequest>,
    ) -> Result<Response<PathResponse>, Status> {
        let request = request.into_inner();
        let path = self
            .fs
            .rename(
                &request.tenant,
                &request.source,
                &request.target,
                durability(request.durability),
            )
            .await
            .map_err(status)?;
        Ok(Response::new(PathResponse { path }))
    }

    async fn resolve(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<PathResponse>, Status> {
        let request = request.into_inner();
        let path = self
            .fs
            .resolve_path(&request.tenant, &request.path)
            .map_err(status)?;
        Ok(Response::new(PathResponse { path }))
    }

    async fn absolute(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<PathResponse>, Status> {
        let request = request.into_inner();
        let path = self
            .fs
            .absolute(&request.tenant, &request.path)
            .map_err(status)?;
        Ok(Response::new(PathResponse { path }))
    }

    #[allow(clippy::result_large_err)]
    async fn stream_file(
        &self,
        request: Request<StreamFileRequest>,
    ) -> Result<Response<Self::StreamFileStream>, Status> {
        let request = request.into_inner();
        let chunks = self
            .fs
            .stream_range(
                &request.tenant,
                &request.path,
                request.offset,
                request.length,
            )
            .await
            .map_err(status)?;
        let stream = tokio_stream::wrappers::ReceiverStream::new(chunks)
            .map(|result| result.map(|data| FileChunk { data }).map_err(status));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn write_file(
        &self,
        request: Request<Streaming<WriteFileRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("write stream is empty"))?;
        let start = match first.payload {
            Some(write_file_request::Payload::Start(start)) => start,
            _ => {
                return Err(Status::invalid_argument(
                    "first write stream message must contain start",
                ));
            }
        };
        let durability = durability(start.durability);
        let mut upload = self
            .fs
            .start_upload(&start.tenant, &start.path, false)
            .await
            .map_err(status)?;
        loop {
            let message = match stream.message().await {
                Ok(message) => message,
                Err(error) => {
                    let _ = upload.abort().await;
                    return Err(error);
                }
            };
            let Some(message) = message else {
                break;
            };
            match message.payload {
                Some(write_file_request::Payload::Chunk(chunk)) => {
                    if let Err(error) = upload.push(chunk).await {
                        let _ = upload.abort().await;
                        return Err(status(error));
                    }
                }
                Some(write_file_request::Payload::Start(_)) => {
                    let _ = upload.abort().await;
                    return Err(Status::invalid_argument(
                        "write stream contains more than one start message",
                    ));
                }
                None => {}
            }
        }
        let size = upload.finish(durability).await.map_err(status)?;
        Ok(Response::new(WriteResponse { size }))
    }
}

fn durability(value: i32) -> Durability {
    use proto::pseudofs::v1::Durability as ApiDurability;
    match ApiDurability::try_from(value).unwrap_or(ApiDurability::AckUnspecified) {
        ApiDurability::AckUnspecified | ApiDurability::Written => Durability::Written,
        ApiDurability::Applied => Durability::Applied,
        ApiDurability::Durable => Durability::Durable,
    }
}

/// Applies a request limit where zero means "use the default".
fn limit(requested: u64, default: u64, max: u64) -> u64 {
    if requested == 0 {
        default
    } else {
        requested.min(max)
    }
}

#[allow(clippy::result_large_err)]
fn directory_stream(
    entries: tokio::sync::mpsc::Receiver<pseudofs::Result<pseudofs::DirectoryEntry>>,
) -> DirectoryStream {
    let stream = tokio_stream::wrappers::ReceiverStream::new(entries).map(|result| {
        result
            .map(|entry| DirectoryEntry {
                path: entry.path,
                name: entry.name,
                kind: file_kind(entry.kind),
            })
            .map_err(status)
    });
    Box::pin(stream)
}

fn grep_line(line: pseudofs::GrepLine) -> GrepLine {
    GrepLine {
        line_number: line.line_number,
        text: line.text,
    }
}

fn file_kind(kind: pseudofs::FileKind) -> i32 {
    match kind {
        pseudofs::FileKind::File => FileKind::File as i32,
        pseudofs::FileKind::Directory => FileKind::Directory as i32,
    }
}

fn status(error: Error) -> Status {
    let (code, kind, path) = match &error {
        Error::NotFound(path) => (Code::NotFound, "not_found", path.as_str()),
        Error::AlreadyExists(path) => (Code::AlreadyExists, "already_exists", path.as_str()),
        Error::NotDirectory(path) => (Code::FailedPrecondition, "not_directory", path.as_str()),
        Error::IsDirectory(path) => (Code::FailedPrecondition, "is_directory", path.as_str()),
        Error::DirectoryNotEmpty(path) => (
            Code::FailedPrecondition,
            "directory_not_empty",
            path.as_str(),
        ),
        Error::InvalidPath(path) => (Code::InvalidArgument, "invalid_path", path.as_str()),
        Error::InvalidPattern(_) => (Code::InvalidArgument, "invalid_pattern", ""),
        Error::InvalidMode(mode) => (Code::InvalidArgument, "invalid_mode", mode.as_str()),
        Error::ResourceExhausted(_) => (Code::ResourceExhausted, "resource_exhausted", ""),
        Error::Blob(_) => (Code::Unavailable, "blob", ""),
        Error::InvalidUtf8(path) => (Code::DataLoss, "invalid_utf8", path.as_str()),
        Error::Conflict(path) => (Code::Aborted, "conflict", path.as_str()),
        Error::RootOperation => (Code::PermissionDenied, "root_operation", "/"),
        Error::Corrupt(_) => (Code::DataLoss, "corrupt", ""),
        Error::Storage(_) => (Code::Internal, "storage", ""),
    };
    let detail = ErrorDetail {
        kind: kind.to_owned(),
        path: path.to_owned(),
    };
    Status::with_details(code, error.to_string(), detail.encode_to_vec().into())
}

#[cfg(test)]
mod tests {
    use common::storage::config::StorageConfig;
    use proto::pseudofs::v1::pseudo_fs_client::PseudoFsClient;
    use proto::pseudofs::v1::{
        GlobRequest, GrepRequest, PathRequest, StreamFileRequest, WriteBytesRequest,
        WriteFileRequest, WriteFileStart, WriteTextRequest, write_file_request,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;

    struct TestServer {
        endpoint: String,
        handle: tokio::task::JoinHandle<()>,
        _dir: tempfile::TempDir,
    }

    async fn test_server(max_unary_file_size_bytes: u64) -> TestServer {
        let dir = tempfile::tempdir().unwrap();
        let fs = PseudoFs::open(pseudofs::Config {
            storage: StorageConfig::InMemory,
            blobs: pseudofs::BlobConfig {
                local_path: dir.path().display().to_string(),
                ..pseudofs::BlobConfig::default()
            },
            chunk_size_bytes: 3,
            inline_threshold_bytes: 4,
            max_file_size_bytes: 1024,
            max_append_generations: 4,
            ..pseudofs::Config::default()
        })
        .await
        .unwrap();
        let search = SearchLimits {
            default_glob_results: 2,
            max_glob_results: 3,
            ..SearchLimits::default()
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<PseudoFsServer<Service>>()
            .await;
        let server = Server::builder()
            .add_service(health_service)
            .add_service(
                Service::new(fs, max_unary_file_size_bytes, search).into_server(1024, 1024),
            )
            .serve_with_incoming(TcpListenerStream::new(listener));
        let handle = tokio::spawn(async move {
            server.await.unwrap();
        });
        TestServer {
            endpoint: format!("http://{address}"),
            handle,
            _dir: dir,
        }
    }

    #[allow(clippy::result_large_err)]
    async fn read_stream(
        client: &mut PseudoFsClient<tonic::transport::Channel>,
        offset: u64,
        length: Option<u64>,
    ) -> Result<Vec<u8>, Status> {
        let mut stream = client
            .stream_file(StreamFileRequest {
                path: "/download.bin".to_owned(),
                tenant: "tenant-a".to_owned(),
                offset,
                length,
            })
            .await?
            .into_inner();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.message().await? {
            bytes.extend_from_slice(&chunk.data);
        }
        Ok(bytes)
    }

    #[tokio::test]
    async fn streams_writes_and_reads_over_grpc() {
        const TENANT: &str = "tenant-a";
        let TestServer {
            endpoint,
            handle: server,
            _dir,
        } = test_server(4).await;
        let mut client = PseudoFsClient::connect(endpoint.clone()).await.unwrap();
        let messages = vec![
            WriteFileRequest {
                payload: Some(write_file_request::Payload::Start(WriteFileStart {
                    path: "/download.bin".to_owned(),
                    durability: proto::pseudofs::v1::Durability::Applied as i32,
                    tenant: TENANT.to_owned(),
                })),
            },
            WriteFileRequest {
                payload: Some(write_file_request::Payload::Chunk(
                    bytes::Bytes::from_static(b"abc"),
                )),
            },
            WriteFileRequest {
                payload: Some(write_file_request::Payload::Chunk(
                    bytes::Bytes::from_static(b"def"),
                )),
            },
        ];
        let result = client
            .write_file(tokio_stream::iter(messages))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.size, 6);

        assert_eq!(read_stream(&mut client, 0, None).await.unwrap(), b"abcdef");
        assert_eq!(read_stream(&mut client, 2, Some(3)).await.unwrap(), b"cde");
        assert_eq!(read_stream(&mut client, 4, None).await.unwrap(), b"ef");
        assert!(read_stream(&mut client, 10, None).await.unwrap().is_empty());

        let error = client
            .read_bytes(PathRequest {
                path: "/download.bin".to_owned(),
                tenant: TENANT.to_owned(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);

        let error = client
            .write_bytes(WriteBytesRequest {
                path: "/too-large".to_owned(),
                value: bytes::Bytes::from_static(b"abcde"),
                durability: proto::pseudofs::v1::Durability::Applied as i32,
                tenant: TENANT.to_owned(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);

        for path in ["/first", "/second"] {
            client
                .write_bytes(WriteBytesRequest {
                    path: path.to_owned(),
                    value: bytes::Bytes::from_static(b"x"),
                    durability: proto::pseudofs::v1::Durability::Applied as i32,
                    tenant: TENANT.to_owned(),
                })
                .await
                .unwrap();
        }
        let mut entries = client
            .iterdir(PathRequest {
                path: "/".to_owned(),
                tenant: TENANT.to_owned(),
            })
            .await
            .unwrap()
            .into_inner();
        let mut names = Vec::new();
        while let Some(entry) = entries.message().await.unwrap() {
            names.push(entry.name);
        }
        names.sort();
        assert_eq!(names, vec!["download.bin", "first", "second"]);

        client
            .write_bytes(WriteBytesRequest {
                path: "/isolated".to_owned(),
                value: bytes::Bytes::from_static(b"b"),
                durability: proto::pseudofs::v1::Durability::Applied as i32,
                tenant: "tenant-b".to_owned(),
            })
            .await
            .unwrap();
        let error = client
            .read_bytes(PathRequest {
                path: "/isolated".to_owned(),
                tenant: TENANT.to_owned(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::NotFound);

        let error = client
            .exists(PathRequest {
                path: "/../first".to_owned(),
                tenant: TENANT.to_owned(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        let error = client
            .exists(PathRequest {
                path: "/first".to_owned(),
                tenant: String::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);

        let error = client
            .read_bytes(PathRequest {
                path: "/missing".to_owned(),
                tenant: TENANT.to_owned(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::NotFound);
        let detail = ErrorDetail::decode(error.details()).unwrap();
        assert_eq!(detail.kind, "not_found");

        let channel = tonic::transport::Channel::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut health = tonic_health::pb::health_client::HealthClient::new(channel);
        let response = health
            .check(tonic_health::pb::HealthCheckRequest {
                service: String::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response.status,
            tonic_health::pb::health_check_response::ServingStatus::Serving as i32
        );
        server.abort();
    }

    #[tokio::test]
    async fn globs_and_greps_over_grpc() {
        const TENANT: &str = "tenant-a";
        let TestServer {
            endpoint,
            handle: server,
            _dir,
        } = test_server(512).await;
        let mut client = PseudoFsClient::connect(endpoint).await.unwrap();
        for (path, value) in [
            ("/dir/a/subfolder/one.txt", "alpha\nneedle one\nomega\n"),
            ("/dir/b/c/subfolder/two.txt", "NEEDLE two\n"),
            ("/dir/d/subfolder/three.txt", "none\n"),
            ("/dir/e/subfolder/four.txt", "needle four\n"),
            ("/dir/other/five.txt", "needle five\n"),
        ] {
            client
                .write_text(WriteTextRequest {
                    path: path.to_owned(),
                    value: value.to_owned(),
                    durability: proto::pseudofs::v1::Durability::Applied as i32,
                    tenant: TENANT.to_owned(),
                })
                .await
                .unwrap();
        }

        let glob = |max_results| GlobRequest {
            tenant: TENANT.to_owned(),
            pattern: "/dir/**/subfolder".to_owned(),
            max_results,
        };
        let mut paths = Vec::new();
        let mut entries = client.glob(glob(0)).await.unwrap().into_inner();
        while let Some(entry) = entries.message().await.unwrap() {
            assert_eq!(entry.kind, FileKind::Directory as i32);
            paths.push(entry.path);
        }
        assert_eq!(paths, vec!["/dir/a/subfolder", "/dir/b/c/subfolder"]);
        let mut count = 0;
        let mut entries = client.glob(glob(100)).await.unwrap().into_inner();
        while entries.message().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 3);

        let mut matches = client
            .grep(GrepRequest {
                tenant: TENANT.to_owned(),
                path: "/dir/**/subfolder/*.txt".to_owned(),
                pattern: "needle".to_owned(),
                case_insensitive: true,
                before_context: 1,
                after_context: 1,
                ..GrepRequest::default()
            })
            .await
            .unwrap()
            .into_inner();
        let mut found = Vec::new();
        while let Some(found_match) = matches.message().await.unwrap() {
            found.push(found_match);
        }
        let summary: Vec<_> = found
            .iter()
            .map(|found| (found.path.as_str(), found.line_number, found.line.as_str()))
            .collect();
        assert_eq!(
            summary,
            vec![
                ("/dir/a/subfolder/one.txt", 2, "needle one"),
                ("/dir/b/c/subfolder/two.txt", 1, "NEEDLE two"),
                ("/dir/e/subfolder/four.txt", 1, "needle four"),
            ]
        );
        assert_eq!(found[0].before[0].text, "alpha");
        assert_eq!(found[0].after[0].text, "omega");

        let error = client
            .grep(GrepRequest {
                tenant: TENANT.to_owned(),
                path: "/dir".to_owned(),
                pattern: "(".to_owned(),
                ..GrepRequest::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(
            ErrorDetail::decode(error.details()).unwrap().kind,
            "invalid_pattern"
        );
        server.abort();
    }
}
