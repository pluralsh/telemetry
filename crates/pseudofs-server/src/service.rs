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
    FileKind, MkdirRequest, MutationPathRequest, OpenRequest, PathRequest, PathResponse,
    RenameRequest, StatResponse, TextResponse, WriteBytesRequest, WriteFileRequest, WriteResponse,
    WriteTextRequest, write_file_request,
};
use pseudofs::{Durability, Error, PseudoFs};
use tonic::{Code, Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct Service {
    fs: PseudoFs,
    max_unary_file_size_bytes: u64,
}

impl Service {
    pub fn new(fs: PseudoFs, max_unary_file_size_bytes: u64) -> Self {
        Self {
            fs,
            max_unary_file_size_bytes,
        }
    }

    #[allow(clippy::result_large_err)]
    async fn ensure_unary_read(&self, tenant: &str, path: &str) -> Result<(), Status> {
        let size = self.fs.stat(tenant, path).await.map_err(status)?.size;
        if size > self.max_unary_file_size_bytes {
            return Err(Status::resource_exhausted(format!(
                "file size {size} exceeds unary limit {}; use StreamFile",
                self.max_unary_file_size_bytes
            )));
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn ensure_unary_write(&self, size: usize) -> Result<(), Status> {
        if size as u64 > self.max_unary_file_size_bytes {
            return Err(Status::resource_exhausted(format!(
                "payload size {size} exceeds unary limit {}; use WriteFile",
                self.max_unary_file_size_bytes
            )));
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

#[tonic::async_trait]
impl PseudoFsApi for Service {
    type StreamFileStream = FileStream;
    type IterdirStream = DirectoryStream;

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
        self.ensure_unary_read(&request.get_ref().tenant, &request.get_ref().path)
            .await?;
        let request = request.into_inner();
        let value = self
            .fs
            .read_text(&request.tenant, &request.path)
            .await
            .map_err(status)?;
        Ok(Response::new(TextResponse { value }))
    }

    async fn read_bytes(
        &self,
        request: Request<PathRequest>,
    ) -> Result<Response<BytesResponse>, Status> {
        self.ensure_unary_read(&request.get_ref().tenant, &request.get_ref().path)
            .await?;
        let request = request.into_inner();
        let value = self
            .fs
            .read_bytes(&request.tenant, &request.path)
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
        let stream = tokio_stream::wrappers::ReceiverStream::new(entries).map(|result| {
            result
                .map(|entry| DirectoryEntry {
                    path: entry.path,
                    name: entry.name,
                    kind: file_kind(entry.kind),
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
        request: Request<PathRequest>,
    ) -> Result<Response<Self::StreamFileStream>, Status> {
        let request = request.into_inner();
        let chunks = self
            .fs
            .stream_chunks(&request.tenant, &request.path)
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
        Error::InvalidMode(mode) => (Code::InvalidArgument, "invalid_mode", mode.as_str()),
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
        PathRequest, WriteBytesRequest, WriteFileRequest, WriteFileStart, write_file_request,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;

    async fn test_server() -> (String, tokio::task::JoinHandle<()>) {
        let fs = PseudoFs::open(pseudofs::Config {
            storage: StorageConfig::InMemory,
            chunk_size_bytes: 3,
            max_file_size_bytes: 1024,
            max_append_generations: 4,
        })
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<PseudoFsServer<Service>>()
            .await;
        let server = Server::builder()
            .add_service(health_service)
            .add_service(Service::new(fs, 4).into_server(1024, 1024))
            .serve_with_incoming(TcpListenerStream::new(listener));
        let handle = tokio::spawn(async move {
            server.await.unwrap();
        });
        (format!("http://{address}"), handle)
    }

    #[tokio::test]
    async fn streams_writes_and_reads_over_grpc() {
        const TENANT: &str = "tenant-a";
        let (endpoint, server) = test_server().await;
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

        let mut stream = client
            .stream_file(PathRequest {
                path: "/download.bin".to_owned(),
                tenant: TENANT.to_owned(),
            })
            .await
            .unwrap()
            .into_inner();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.message().await.unwrap() {
            bytes.extend_from_slice(&chunk.data);
        }
        assert_eq!(bytes, b"abcdef");

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
}
