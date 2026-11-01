//! HTTP/1.1 JSON-RPC inspection for RPC listeners.
//!
//! An L7 listener parses exactly one HTTP/1.1 request per connection, applies
//! the method policy, and only then connects to the fixed upstream. A request
//! that passes every check is rebuilt from scratch: the client's headers are
//! not copied, hop-by-hop headers are dropped, and `Connection: close` is set.
//! The upstream response is streamed back unchanged.
//!
//! Inspection is intentionally strict and fails closed:
//!
//! - Only `GET` and `POST` are accepted. `CONNECT`, `Upgrade`, compressed
//!   bodies, and transfer codings other than `chunked` are rejected.
//! - Headers and bodies have separate size and receive-time limits.
//! - JSON is parsed with a bounded visitor, so nesting depth, array and object
//!   size, duplicate keys, and batch length are all limited.
//! - Method names are compared exactly against an allow list. No substring
//!   search is used, so escapes and lookalike names cannot smuggle a method.
//! - Expensive methods consume a per-method rate token and a concurrency
//!   permit before the request is sent, and the permit is held until the
//!   response is fully relayed.
//!
//! WebSocket upgrade is not supported: a listener configured for L7 rejects
//! `Upgrade` instead of tunneling it. See `docs/l7.md`.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use serde::de;
use serde::de::DeserializeSeed;
use serde::de::Deserializer;
use serde::de::MapAccess;
use serde::de::SeqAccess;
use serde::de::Visitor;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::time::timeout;

use crate::config;
use crate::config::Endpoint;
use crate::policy::RuntimePolicy;
use crate::policy::TokenBucket;
use crate::relay::BandwidthLimiter;
use crate::relay::ConnectError;
use crate::relay::connect_endpoint;

/// Bytes read from the upstream before writing them to the client.
const COPY_BUFFER_SIZE: usize = 8 * 1024;

/// Largest method name accepted from a client.
const MAX_METHOD_LEN: usize = 128;

/// Largest chunk-size line accepted while decoding a chunked body.
const MAX_CHUNK_LINE: usize = 32;

/// Extra raw bytes allowed for chunk framing on top of the decoded body limit.
const CHUNK_OVERHEAD: usize = 64 * 1024;

/// Largest number of chunks accepted in one body.
const MAX_CHUNKS: usize = 65_536;

/// Why an L7 request was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reject {
    /// The request line, headers, or framing were malformed.
    Malformed,
    /// The request head exceeded its byte or count limit.
    Header,
    /// The body exceeded its size limit.
    Body,
    /// Receiving the head or body exceeded its time limit.
    Timeout,
    /// The HTTP method is not `GET` or `POST`.
    Method,
    /// A compressed body or unsupported transfer coding was used.
    Encoding,
    /// The client attempted an HTTP upgrade.
    Upgrade,
    /// The body was not a valid JSON-RPC request, or exceeded a JSON limit.
    Json,
    /// A method was not allowed, or the emergency block is active.
    Policy,
    /// A per-method rate or concurrency limit rejected the request.
    Quota,
}

impl Reject {
    /// Returns the stable label used in logs and metrics.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Malformed => "malformed",
            Self::Header => "header",
            Self::Body => "body",
            Self::Timeout => "timeout",
            Self::Method => "method",
            Self::Encoding => "encoding",
            Self::Upgrade => "upgrade",
            Self::Json => "json",
            Self::Policy => "policy",
            Self::Quota => "quota",
        }
    }

    /// Returns the status line sent to the client.
    const fn status(self) -> &'static str {
        match self {
            Self::Malformed | Self::Upgrade | Self::Json => "400 Bad Request",
            Self::Header => "431 Request Header Fields Too Large",
            Self::Body => "413 Payload Too Large",
            Self::Timeout => "408 Request Timeout",
            Self::Method => "405 Method Not Allowed",
            Self::Encoding => "415 Unsupported Media Type",
            Self::Policy => "403 Forbidden",
            Self::Quota => "429 Too Many Requests",
        }
    }
}

/// Result of inspecting and, when allowed, relaying one request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Outcome {
    /// Whether a request was forwarded to the upstream.
    pub forwarded: bool,
    /// Why the request was rejected, when it was.
    pub reject: Option<Reject>,
    /// Bytes written to the upstream.
    pub to_upstream: u64,
    /// Bytes written back to the client.
    pub to_client: u64,
}

impl Outcome {
    const fn rejected(reject: Reject) -> Self {
        Self {
            forwarded: false,
            reject: Some(reject),
            to_upstream: 0,
            to_client: 0,
        }
    }
}

/// Failure while relaying an accepted request.
#[derive(Debug)]
pub enum Error {
    /// The upstream connection could not be established.
    Connect(ConnectError),
    /// Reading or writing a socket failed.
    Io(io::Error),
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Per-listener L7 state built from configuration.
#[derive(Debug)]
pub struct L7Context {
    settings: Settings,
    allow: HashSet<String>,
    deny: HashSet<String>,
    expensive: BTreeMap<String, MethodState>,
}

/// Bounded parser settings shared by every request on one listener.
#[derive(Debug, Clone, Copy)]
struct Settings {
    max_header_bytes: usize,
    max_header_count: usize,
    header_timeout: Duration,
    max_body_bytes: usize,
    body_timeout: Duration,
    json: JsonLimits,
    max_batch: usize,
}

/// Limits applied by the JSON visitor.
#[derive(Debug, Clone, Copy)]
struct JsonLimits {
    max_depth: usize,
    max_array: usize,
}

/// Rate and concurrency state for one expensive method.
#[derive(Debug)]
struct MethodState {
    rate: TokenBucket,
    rate_per_second: u32,
    burst: u32,
    concurrent: Arc<Semaphore>,
}

impl L7Context {
    /// Builds the per-listener state from validated configuration.
    #[must_use]
    pub fn new(config: &config::L7) -> Self {
        let expensive = config
            .expensive_methods
            .iter()
            .map(|(method, limit)| {
                (
                    method.clone(),
                    MethodState {
                        rate: TokenBucket::new(),
                        rate_per_second: limit.rate_per_second,
                        burst: limit.rate_per_second.max(1),
                        concurrent: Arc::new(Semaphore::new(limit.concurrent)),
                    },
                )
            })
            .collect();
        Self {
            settings: Settings {
                max_header_bytes: config.max_header_bytes,
                max_header_count: config.max_header_count,
                header_timeout: Duration::from_millis(config.header_timeout_ms),
                max_body_bytes: config.max_body_bytes,
                body_timeout: Duration::from_millis(config.body_timeout_ms),
                json: JsonLimits {
                    max_depth: config.max_json_depth,
                    max_array: config.max_json_array,
                },
                max_batch: config.max_batch,
            },
            allow: config.allow_methods.iter().cloned().collect(),
            deny: config.deny_methods.iter().cloned().collect(),
            expensive,
        }
    }

    /// Checks every method and reserves its rate and concurrency budget.
    ///
    /// Nothing is sent upstream until this returns. A batch reserves one token
    /// and one permit per expensive item, so a partially admitted batch is
    /// released before the request is rejected.
    fn admit(
        &self,
        methods: &[String],
        policy: &RuntimePolicy,
    ) -> Result<Vec<OwnedSemaphorePermit>, Reject> {
        if policy.emergency_block() {
            return Err(Reject::Policy);
        }
        for method in methods {
            if self.deny.contains(method) || !self.allow.contains(method) {
                return Err(Reject::Policy);
            }
        }
        let mut permits = Vec::new();
        for method in methods {
            let Some(state) = self.expensive.get(method) else {
                continue;
            };
            // Reserve concurrency first so a rejected request does not consume
            // a rate token that a later request could use.
            let permit = Arc::clone(&state.concurrent)
                .try_acquire_owned()
                .map_err(|_| Reject::Quota)?;
            if !state.rate.try_acquire(state.rate_per_second, state.burst) {
                return Err(Reject::Quota);
            }
            permits.push(permit);
        }
        Ok(permits)
    }
}

/// Inspects one HTTP/1.1 request and relays it when the policy allows.
///
/// Rejections are answered with a small HTTP error response and returned as
/// [`Outcome::reject`]; they are not errors. Transport failures are returned as
/// [`Error`].
///
/// # Errors
///
/// Returns [`Error::Connect`] when the upstream connection fails and
/// [`Error::Io`] when reading the client or writing either socket fails.
pub async fn serve<S>(
    stream: &mut S,
    upstream: &Endpoint,
    connect_timeout: Duration,
    state: &L7Context,
    policy: &RuntimePolicy,
    bandwidth: Option<&BandwidthLimiter>,
) -> Result<Outcome, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let settings = &state.settings;
    let (head, leftover) = match read_head(stream, settings).await {
        Ok(parsed) => parsed,
        Err(HeadError::Reject(reject)) => {
            send_error(stream, reject).await;
            return Ok(Outcome::rejected(reject));
        }
        Err(HeadError::Closed) => return Ok(Outcome::default()),
        Err(HeadError::Io(error)) => return Err(Error::Io(error)),
    };

    let plan = match check_head(&head, settings) {
        Ok(plan) => plan,
        Err(reject) => {
            send_error(stream, reject).await;
            return Ok(Outcome::rejected(reject));
        }
    };
    if plan.expect_continue {
        if let Err(error) = stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await {
            return Err(Error::Io(error));
        }
    }

    let body = match timeout(
        settings.body_timeout,
        read_body(stream, leftover, &plan, settings.max_body_bytes),
    )
    .await
    {
        Ok(Ok(body)) => body,
        Ok(Err(BodyError::Reject(reject))) => {
            send_error(stream, reject).await;
            return Ok(Outcome::rejected(reject));
        }
        Ok(Err(BodyError::Io(error))) => return Err(Error::Io(error)),
        Err(_) => {
            send_error(stream, Reject::Timeout).await;
            return Ok(Outcome::rejected(Reject::Timeout));
        }
    };

    let methods = match inspect_methods(&plan, &body, settings) {
        Ok(methods) => methods,
        Err(reject) => {
            send_error(stream, reject).await;
            return Ok(Outcome::rejected(reject));
        }
    };
    let _permits = match state.admit(&methods, policy) {
        Ok(permits) => permits,
        Err(reject) => {
            send_error(stream, reject).await;
            return Ok(Outcome::rejected(reject));
        }
    };

    let request = build_request(&plan, &body);
    let mut upstream = connect_endpoint(upstream, connect_timeout)
        .await
        .map_err(Error::Connect)?;
    write_request(&mut upstream, &request, bandwidth).await?;
    let to_client = copy_response(&mut upstream, stream, bandwidth).await?;
    Ok(Outcome {
        forwarded: true,
        reject: None,
        to_upstream: request.len() as u64,
        to_client,
    })
}

/// A parsed request head with headers lowercased.
#[derive(Debug)]
struct Head {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
}

/// The checked request, ready to be rebuilt for the upstream.
#[derive(Debug)]
struct Plan {
    method: HttpMethod,
    path: String,
    host: String,
    accept: Option<String>,
    body: BodyKind,
    expect_continue: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HttpMethod {
    Get,
    Post,
}

impl HttpMethod {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum BodyKind {
    None,
    Length(usize),
    Chunked,
}

enum HeadError {
    Reject(Reject),
    Closed,
    Io(io::Error),
}

enum BodyError {
    Reject(Reject),
    Io(io::Error),
}

impl From<Reject> for BodyError {
    fn from(reject: Reject) -> Self {
        Self::Reject(reject)
    }
}

/// Reads the request head under the byte, count, and time limits.
async fn read_head<S>(stream: &mut S, settings: &Settings) -> Result<(Head, Vec<u8>), HeadError>
where
    S: AsyncRead + Unpin,
{
    match timeout(settings.header_timeout, read_head_inner(stream, settings)).await {
        Ok(result) => result,
        Err(_) => Err(HeadError::Reject(Reject::Timeout)),
    }
}

async fn read_head_inner<S>(
    stream: &mut S,
    settings: &Settings,
) -> Result<(Head, Vec<u8>), HeadError>
where
    S: AsyncRead + Unpin,
{
    let mut buffer = Vec::with_capacity(settings.max_header_bytes.min(4096));
    let mut chunk = [0_u8; 4096];
    let mut scanned: usize = 0;
    loop {
        let remaining = settings.max_header_bytes.saturating_sub(buffer.len());
        if remaining == 0 {
            return Err(HeadError::Reject(Reject::Header));
        }
        let take = remaining.min(chunk.len());
        let read = stream
            .read(&mut chunk[..take])
            .await
            .map_err(HeadError::Io)?;
        if read == 0 {
            return if buffer.is_empty() {
                Err(HeadError::Closed)
            } else {
                Err(HeadError::Reject(Reject::Malformed))
            };
        }
        buffer.extend_from_slice(&chunk[..read]);
        // Reparse only once a complete head terminator is present. Without this
        // check a byte-at-a-time client would make the parser rescan the whole
        // buffer for every byte.
        let search_from = scanned.saturating_sub(3);
        scanned = buffer.len();
        if !buffer[search_from..]
            .windows(4)
            .any(|window| window == b"\r\n\r\n")
        {
            continue;
        }
        let mut headers = vec![httparse::EMPTY_HEADER; settings.max_header_count];
        let mut request = httparse::Request::new(&mut headers);
        let (head, length) = match request.parse(&buffer) {
            Ok(httparse::Status::Complete(length)) => (
                Head::from_request(&request, length).map_err(HeadError::Reject)?,
                length,
            ),
            // The terminator is already present, so a partial parse means the
            // request line or a header line is malformed. Waiting for more
            // bytes would only make the client rescan the same buffer.
            Ok(httparse::Status::Partial) => {
                return Err(HeadError::Reject(Reject::Malformed));
            }
            Err(httparse::Error::TooManyHeaders) => {
                return Err(HeadError::Reject(Reject::Header));
            }
            Err(_) => return Err(HeadError::Reject(Reject::Malformed)),
        };
        let leftover = buffer[length..].to_vec();
        return Ok((head, leftover));
    }
}

impl Head {
    fn from_request(request: &httparse::Request<'_, '_>, length: usize) -> Result<Self, Reject> {
        let method = request.method.ok_or(Reject::Malformed)?.to_owned();
        let path = request.path.ok_or(Reject::Malformed)?.to_owned();
        request.version.ok_or(Reject::Malformed)?;
        let mut headers = Vec::with_capacity(request.headers.len());
        for header in request.headers.iter() {
            let value = std::str::from_utf8(header.value).map_err(|_| Reject::Malformed)?;
            headers.push((header.name.to_ascii_lowercase(), value.to_owned()));
        }
        if length == 0 {
            return Err(Reject::Malformed);
        }
        Ok(Self {
            method,
            path,
            headers,
        })
    }

    fn values<'a>(&'a self, name: &str) -> impl Iterator<Item = &'a str> {
        self.headers
            .iter()
            .filter(move |(header, _)| header == name)
            .map(|(_, value)| value.as_str())
    }

    fn single(&self, name: &str) -> Result<Option<&str>, Reject> {
        let mut values = self.values(name);
        let first = values.next();
        if values.next().is_some() {
            return Err(Reject::Malformed);
        }
        Ok(first)
    }

    fn has(&self, name: &str) -> bool {
        self.values(name).next().is_some()
    }
}

/// Applies the header policy and derives how the body will be read.
fn check_head(head: &Head, settings: &Settings) -> Result<Plan, Reject> {
    let method = match head.method.as_str() {
        "GET" => HttpMethod::Get,
        "POST" => HttpMethod::Post,
        _ => return Err(Reject::Method),
    };
    let host = head.single("host")?.ok_or(Reject::Malformed)?;
    if !is_valid_host(host) {
        return Err(Reject::Malformed);
    }
    if head.has("upgrade") || connection_has_token(head, "upgrade") {
        return Err(Reject::Upgrade);
    }
    if head.has("content-encoding") {
        return Err(Reject::Encoding);
    }
    let expect_continue = match head.single("expect")? {
        Some(value) if value.eq_ignore_ascii_case("100-continue") => true,
        Some(_) => return Err(Reject::Malformed),
        None => false,
    };
    let body = body_kind(head, settings)?;
    if method == HttpMethod::Get {
        match body {
            BodyKind::None | BodyKind::Length(0) => {}
            BodyKind::Length(_) | BodyKind::Chunked => return Err(Reject::Malformed),
        }
    }
    validate_target(method, &head.path)?;
    let accept = head.single("accept")?.map(str::to_owned);
    if let Some(accept) = &accept {
        if !accept.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(Reject::Malformed);
        }
    }
    if method == HttpMethod::Post {
        if let Some(content_type) = head.single("content-type")? {
            if !is_json_content_type(content_type) {
                return Err(Reject::Encoding);
            }
        }
    }
    Ok(Plan {
        method,
        path: head.path.clone(),
        host: host.to_owned(),
        accept,
        body,
        expect_continue,
    })
}

fn body_kind(head: &Head, settings: &Settings) -> Result<BodyKind, Reject> {
    let transfer = head.single("transfer-encoding")?;
    let length = head.single("content-length")?;
    match (transfer, length) {
        (Some(_), Some(_)) => {
            // Both framings at once is the classic request-smuggling shape.
            Err(Reject::Malformed)
        }
        (Some(transfer), None) => {
            if !transfer.eq_ignore_ascii_case("chunked") {
                return Err(Reject::Encoding);
            }
            Ok(BodyKind::Chunked)
        }
        (None, Some(length)) => {
            if length.contains(',') {
                return Err(Reject::Malformed);
            }
            let length = length.parse::<u64>().map_err(|_| Reject::Malformed)?;
            let length = usize::try_from(length).map_err(|_| Reject::Body)?;
            if length > settings.max_body_bytes {
                return Err(Reject::Body);
            }
            Ok(BodyKind::Length(length))
        }
        (None, None) => Ok(BodyKind::None),
    }
}

fn connection_has_token(head: &Head, token: &str) -> bool {
    head.values("connection").any(|value| {
        value
            .split(',')
            .any(|part| part.trim().eq_ignore_ascii_case(token))
    })
}

fn is_valid_host(host: &str) -> bool {
    !host.is_empty()
        && host.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b':' | b'[' | b']')
        })
}

fn is_json_content_type(value: &str) -> bool {
    let value = value.split(';').next().unwrap_or("").trim();
    value.eq_ignore_ascii_case("application/json")
        || value.eq_ignore_ascii_case("application/json-rpc")
        || value.eq_ignore_ascii_case("application/jsonrequest")
}

/// Rejects ambiguous URI shapes before they reach the upstream.
fn validate_target(method: HttpMethod, target: &str) -> Result<(), Reject> {
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (target, None),
    };
    if !path.starts_with('/') || path.contains("//") || path.contains("..") {
        return Err(Reject::Malformed);
    }
    let segment = &path[1..];
    if !segment.bytes().all(is_path_character) {
        return Err(Reject::Malformed);
    }
    if method == HttpMethod::Get && segment.is_empty() {
        return Err(Reject::Method);
    }
    if let Some(query) = query {
        if !query
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'#')
        {
            return Err(Reject::Malformed);
        }
    }
    Ok(())
}

const fn is_path_character(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// Reads the body according to the framing chosen by [`check_head`].
async fn read_body<S>(
    stream: &mut S,
    leftover: Vec<u8>,
    plan: &Plan,
    max_body_bytes: usize,
) -> Result<Vec<u8>, BodyError>
where
    S: AsyncRead + Unpin,
{
    match plan.body {
        BodyKind::None => {
            if leftover.is_empty() {
                Ok(Vec::new())
            } else {
                Err(Reject::Malformed.into())
            }
        }
        BodyKind::Length(length) => read_length_body(stream, leftover, length).await,
        BodyKind::Chunked => read_chunked_body(stream, leftover, max_body_bytes).await,
    }
}

async fn read_length_body<S>(
    stream: &mut S,
    leftover: Vec<u8>,
    length: usize,
) -> Result<Vec<u8>, BodyError>
where
    S: AsyncRead + Unpin,
{
    let mut body = vec![0_u8; length];
    let copied = leftover.len().min(length);
    body[..copied].copy_from_slice(&leftover[..copied]);
    if leftover.len() > length {
        return Err(Reject::Malformed.into());
    }
    let mut filled = copied;
    while filled < length {
        let read = stream
            .read(&mut body[filled..])
            .await
            .map_err(BodyError::Io)?;
        if read == 0 {
            return Err(Reject::Malformed.into());
        }
        filled += read;
    }
    Ok(body)
}

async fn read_chunked_body<S>(
    stream: &mut S,
    leftover: Vec<u8>,
    max_body_bytes: usize,
) -> Result<Vec<u8>, BodyError>
where
    S: AsyncRead + Unpin,
{
    let mut decoder = ChunkedDecoder::new(max_body_bytes);
    let mut pending = leftover;
    let mut index = 0;
    let mut raw_total = pending.len();
    let mut buffer = vec![0_u8; COPY_BUFFER_SIZE];
    loop {
        while index < pending.len() {
            decoder.feed(pending[index])?;
            index += 1;
            if decoder.is_done() {
                if index != pending.len() {
                    return Err(Reject::Malformed.into());
                }
                return Ok(decoder.into_body());
            }
        }
        let read = stream.read(&mut buffer).await.map_err(BodyError::Io)?;
        if read == 0 {
            return Err(Reject::Malformed.into());
        }
        raw_total += read;
        if raw_total > max_body_bytes.saturating_add(CHUNK_OVERHEAD) {
            return Err(Reject::Body.into());
        }
        pending.clear();
        pending.extend_from_slice(&buffer[..read]);
        index = 0;
    }
}

/// Streaming decoder for the `chunked` transfer coding.
struct ChunkedDecoder {
    max_body_bytes: usize,
    body: Vec<u8>,
    state: ChunkState,
    size: usize,
    line: String,
    chunks: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkState {
    Size,
    SizeLineEnd,
    Data,
    DataEnd,
    DataLineEnd,
    TrailerEnd,
    TrailerLineEnd,
    Done,
}

impl ChunkedDecoder {
    fn new(max_body_bytes: usize) -> Self {
        Self {
            max_body_bytes,
            body: Vec::new(),
            state: ChunkState::Size,
            size: 0,
            line: String::new(),
            chunks: 0,
        }
    }

    const fn is_done(&self) -> bool {
        matches!(self.state, ChunkState::Done)
    }

    fn into_body(self) -> Vec<u8> {
        self.body
    }

    fn feed(&mut self, byte: u8) -> Result<(), Reject> {
        match self.state {
            ChunkState::Size => {
                if byte == b'\r' {
                    if self.line.is_empty() {
                        return Err(Reject::Malformed);
                    }
                    self.size =
                        usize::from_str_radix(&self.line, 16).map_err(|_| Reject::Malformed)?;
                    self.line.clear();
                    self.state = ChunkState::SizeLineEnd;
                } else if byte == b';' {
                    // Chunk extensions are legal but not needed here.
                    return Err(Reject::Encoding);
                } else if byte.is_ascii_hexdigit() {
                    if self.line.len() >= MAX_CHUNK_LINE {
                        return Err(Reject::Malformed);
                    }
                    self.line.push(char::from(byte));
                } else {
                    return Err(Reject::Malformed);
                }
            }
            ChunkState::SizeLineEnd => {
                if byte != b'\n' {
                    return Err(Reject::Malformed);
                }
                if self.size == 0 {
                    self.state = ChunkState::TrailerEnd;
                } else {
                    self.chunks += 1;
                    if self.chunks > MAX_CHUNKS {
                        return Err(Reject::Body);
                    }
                    if self.body.len().saturating_add(self.size) > self.max_body_bytes {
                        return Err(Reject::Body);
                    }
                    self.state = ChunkState::Data;
                }
            }
            ChunkState::Data => {
                self.body.push(byte);
                self.size -= 1;
                if self.size == 0 {
                    self.state = ChunkState::DataEnd;
                }
            }
            ChunkState::DataEnd => {
                if byte != b'\r' {
                    return Err(Reject::Malformed);
                }
                self.state = ChunkState::DataLineEnd;
            }
            ChunkState::DataLineEnd => {
                if byte != b'\n' {
                    return Err(Reject::Malformed);
                }
                self.state = ChunkState::Size;
            }
            ChunkState::TrailerEnd => {
                if byte != b'\r' {
                    return Err(Reject::Malformed);
                }
                self.state = ChunkState::TrailerLineEnd;
            }
            ChunkState::TrailerLineEnd => {
                if byte != b'\n' {
                    return Err(Reject::Malformed);
                }
                self.state = ChunkState::Done;
            }
            ChunkState::Done => return Err(Reject::Malformed),
        }
        Ok(())
    }
}

/// Returns every method name the upstream could execute for this request.
///
/// A POST to a non-root URI path is inspected twice: the path method and the
/// JSON body methods must both be allowed, so a URI call cannot hide a body
/// method or the reverse.
fn inspect_methods(plan: &Plan, body: &[u8], settings: &Settings) -> Result<Vec<String>, Reject> {
    let mut methods = Vec::new();
    if let Some(method) = uri_method(&plan.path) {
        methods.push(method);
    }
    if plan.method == HttpMethod::Post && !body.is_empty() {
        methods.extend(inspect_json(body, &settings.json, settings.max_batch)?);
    }
    if methods.is_empty() {
        return Err(Reject::Json);
    }
    Ok(methods)
}

/// Extracts the `CometBFT` URI method from a validated path.
fn uri_method(path: &str) -> Option<String> {
    let path = path.split('?').next().unwrap_or(path);
    let segment = path.strip_prefix('/')?;
    (!segment.is_empty()).then(|| segment.to_owned())
}

/// Parses and validates a JSON-RPC request or batch.
fn inspect_json(body: &[u8], limits: &JsonLimits, max_batch: usize) -> Result<Vec<String>, Reject> {
    if body.is_empty() {
        return Err(Reject::Json);
    }
    let mut methods = Vec::new();
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let seed = TopSeed {
        limits,
        max_batch,
        methods: &mut methods,
    };
    seed.deserialize(&mut deserializer)
        .map_err(|_| Reject::Json)?;
    deserializer.end().map_err(|_| Reject::Json)?;
    if methods.is_empty() {
        return Err(Reject::Json);
    }
    Ok(methods)
}

struct TopSeed<'a> {
    limits: &'a JsonLimits,
    max_batch: usize,
    methods: &'a mut Vec<String>,
}

impl<'de> DeserializeSeed<'de> for TopSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(TopVisitor {
            limits: self.limits,
            max_batch: self.max_batch,
            methods: self.methods,
        })
    }
}

struct TopVisitor<'a> {
    limits: &'a JsonLimits,
    max_batch: usize,
    methods: &'a mut Vec<String>,
}

impl<'de> Visitor<'de> for TopVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON-RPC request object or batch array")
    }

    fn visit_map<A>(self, map: A) -> Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut map = map;
        inspect_request(&mut map, self.limits, self.methods, 1)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut count = 0;
        while seq
            .next_element_seed(RequestSeed {
                limits: self.limits,
                methods: &mut *self.methods,
                depth: 2,
            })?
            .is_some()
        {
            count += 1;
            if count > self.max_batch {
                return Err(de::Error::custom("batch limit exceeded"));
            }
        }
        if count == 0 {
            return Err(de::Error::custom("empty batch"));
        }
        Ok(())
    }
}

struct RequestSeed<'a> {
    limits: &'a JsonLimits,
    methods: &'a mut Vec<String>,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for RequestSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RequestVisitor {
            limits: self.limits,
            methods: self.methods,
            depth: self.depth,
        })
    }
}

struct RequestVisitor<'a> {
    limits: &'a JsonLimits,
    methods: &'a mut Vec<String>,
    depth: usize,
}

impl<'de> Visitor<'de> for RequestVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JSON-RPC request object")
    }

    fn visit_map<A>(self, map: A) -> Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut map = map;
        inspect_request(&mut map, self.limits, self.methods, self.depth)
    }
}

/// Validates one request object and appends its `method` value.
fn inspect_request<'de, A>(
    map: &mut A,
    limits: &JsonLimits,
    methods: &mut Vec<String>,
    depth: usize,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    if depth > limits.max_depth {
        return Err(de::Error::custom("json depth limit exceeded"));
    }
    let mut seen: Vec<String> = Vec::new();
    let mut method: Option<String> = None;
    let mut entries = 0;
    while let Some(key) = map.next_key::<String>()? {
        entries += 1;
        if entries > limits.max_array {
            return Err(de::Error::custom("object size limit exceeded"));
        }
        if seen.iter().any(|existing| existing == &key) {
            return Err(de::Error::custom("duplicate json key"));
        }
        if key == "method" {
            let value: String = map.next_value()?;
            if value.is_empty()
                || value.len() > MAX_METHOD_LEN
                || !value.bytes().all(|byte| byte.is_ascii_graphic())
            {
                return Err(de::Error::custom("invalid method name"));
            }
            method = Some(value);
        } else {
            map.next_value_seed(ValueSeed {
                limits,
                depth: depth + 1,
            })?;
        }
        seen.push(key);
    }
    let method = method.ok_or_else(|| de::Error::missing_field("method"))?;
    methods.push(method);
    Ok(())
}

struct ValueSeed<'a> {
    limits: &'a JsonLimits,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for ValueSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        if self.depth > self.limits.max_depth {
            return Err(de::Error::custom("json depth limit exceeded"));
        }
        deserializer.deserialize_any(ValueVisitor {
            limits: self.limits,
            depth: self.depth,
        })
    }
}

struct ValueVisitor<'a> {
    limits: &'a JsonLimits,
    depth: usize,
}

impl<'de> Visitor<'de> for ValueVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_bool<E>(self, _: bool) -> Result<(), E> {
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<(), E> {
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<(), E> {
        Ok(())
    }

    fn visit_f64<E>(self, _: f64) -> Result<(), E> {
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<(), E>
    where
        E: de::Error,
    {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_none<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut count = 0;
        while seq
            .next_element_seed(ValueSeed {
                limits: self.limits,
                depth: self.depth + 1,
            })?
            .is_some()
        {
            count += 1;
            if count > self.limits.max_array {
                return Err(de::Error::custom("json array limit exceeded"));
            }
        }
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut seen: Vec<String> = Vec::new();
        let mut entries = 0;
        while let Some(key) = map.next_key::<String>()? {
            entries += 1;
            if entries > self.limits.max_array {
                return Err(de::Error::custom("json object limit exceeded"));
            }
            if seen.iter().any(|existing| existing == &key) {
                return Err(de::Error::custom("duplicate json key"));
            }
            map.next_value_seed(ValueSeed {
                limits: self.limits,
                depth: self.depth + 1,
            })?;
            seen.push(key);
        }
        Ok(())
    }
}

/// Builds the upstream request from the validated plan.
///
/// Only a small, fixed header set is forwarded. Everything the client sent
/// that could change framing, routing, or connection semantics is dropped.
fn build_request(plan: &Plan, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(plan.path.len() + body.len() + 128);
    out.extend_from_slice(plan.method.as_str().as_bytes());
    out.push(b' ');
    out.extend_from_slice(plan.path.as_bytes());
    out.extend_from_slice(b" HTTP/1.1\r\nHost: ");
    out.extend_from_slice(plan.host.as_bytes());
    out.extend_from_slice(b"\r\n");
    if plan.method == HttpMethod::Post {
        out.extend_from_slice(b"Content-Type: application/json\r\nContent-Length: ");
        out.extend_from_slice(body.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    if let Some(accept) = &plan.accept {
        out.extend_from_slice(b"Accept: ");
        out.extend_from_slice(accept.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"Connection: close\r\n\r\n");
    out.extend_from_slice(body);
    out
}

async fn write_request<U>(
    upstream: &mut U,
    request: &[u8],
    bandwidth: Option<&BandwidthLimiter>,
) -> Result<(), Error>
where
    U: AsyncWrite + Unpin,
{
    for chunk in request.chunks(COPY_BUFFER_SIZE) {
        if let Some(bandwidth) = bandwidth {
            bandwidth.acquire(chunk.len()).await;
        }
        upstream.write_all(chunk).await?;
    }
    Ok(())
}

async fn copy_response<U, S>(
    upstream: &mut U,
    client: &mut S,
    bandwidth: Option<&BandwidthLimiter>,
) -> Result<u64, Error>
where
    U: AsyncRead + Unpin,
    S: AsyncWrite + Unpin,
{
    let mut buffer = vec![0_u8; COPY_BUFFER_SIZE];
    let mut total = 0_u64;
    loop {
        let read = upstream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        if let Some(bandwidth) = bandwidth {
            bandwidth.acquire(read).await;
        }
        client.write_all(&buffer[..read]).await?;
        total += read as u64;
    }
    let _ = client.shutdown().await;
    Ok(total)
}

/// Sends a minimal error response and closes the connection.
///
/// The peer may still be sending a large head or body that the guard has
/// already stopped reading. Closing a socket with unread data can make the
/// kernel answer with a reset and discard the error response, so the guard
/// drains a bounded amount for a short time before closing.
async fn send_error<S>(stream: &mut S, reject: Reject)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let body = format!("{}\n", reject.reason());
    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        reject.status(),
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
    let _ = timeout(Duration::from_millis(100), async {
        let mut buffer = [0_u8; 1024];
        let mut drained = 0;
        while drained < 64 * 1024 {
            match stream.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => drained += read,
            }
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> JsonLimits {
        JsonLimits {
            max_depth: 8,
            max_array: 16,
        }
    }

    fn settings() -> Settings {
        Settings {
            max_header_bytes: 4096,
            max_header_count: 32,
            header_timeout: Duration::from_secs(1),
            max_body_bytes: 4096,
            body_timeout: Duration::from_secs(1),
            json: limits(),
            max_batch: 4,
        }
    }

    fn head(text: &str) -> Head {
        let mut headers = vec![httparse::EMPTY_HEADER; 32];
        let mut request = httparse::Request::new(&mut headers);
        let length = match request.parse(text.as_bytes()).expect("parse head") {
            httparse::Status::Complete(length) => length,
            httparse::Status::Partial => panic!("incomplete head"),
        };
        Head::from_request(&request, length).expect("head")
    }

    fn plan(text: &str) -> Result<Plan, Reject> {
        check_head(&head(text), &settings())
    }

    #[test]
    fn accepts_post_json_rpc() {
        let plan = plan(
            "POST / HTTP/1.1\r\nHost: node\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n",
        )
        .expect("valid post");

        assert_eq!(plan.method, HttpMethod::Post);
        assert_eq!(plan.path, "/");
        assert!(matches!(plan.body, BodyKind::Length(2)));
    }

    #[test]
    fn accepts_get_uri_call() {
        let plan = plan("GET /status?height=1 HTTP/1.1\r\nHost: node\r\n\r\n").expect("valid get");

        assert_eq!(plan.method, HttpMethod::Get);
        assert_eq!(uri_method(&plan.path).as_deref(), Some("status"));
    }

    #[test]
    fn rejects_connect_and_upgrade() {
        assert_eq!(
            plan("CONNECT node:26657 HTTP/1.1\r\nHost: node\r\n\r\n").expect_err("connect"),
            Reject::Method
        );
        assert_eq!(
            plan("GET /status HTTP/1.1\r\nHost: node\r\nUpgrade: websocket\r\n\r\n")
                .expect_err("upgrade"),
            Reject::Upgrade
        );
        assert_eq!(
            plan("GET /status HTTP/1.1\r\nHost: node\r\nConnection: keep-alive, Upgrade\r\n\r\n")
                .expect_err("upgrade token"),
            Reject::Upgrade
        );
    }

    #[test]
    fn rejects_compressed_and_ambiguous_bodies() {
        assert_eq!(
            plan("POST / HTTP/1.1\r\nHost: node\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n")
                .expect_err("encoding"),
            Reject::Encoding
        );
        assert_eq!(
            plan("POST / HTTP/1.1\r\nHost: node\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n")
                .expect_err("smuggling"),
            Reject::Malformed
        );
        assert_eq!(
            plan("POST / HTTP/1.1\r\nHost: node\r\nTransfer-Encoding: gzip\r\n\r\n")
                .expect_err("coding"),
            Reject::Encoding
        );
    }

    #[test]
    fn rejects_ambiguous_uri_variants() {
        for target in [
            "//status",
            "/../status",
            "/status/",
            "/%73tatus",
            "/status#fragment",
            "/",
        ] {
            assert!(
                plan(&format!("GET {target} HTTP/1.1\r\nHost: node\r\n\r\n")).is_err(),
                "{target} must be rejected"
            );
        }
    }

    #[test]
    fn post_uri_without_body_is_a_uri_call() {
        let uri_plan =
            plan("POST /status HTTP/1.1\r\nHost: node\r\nContent-Length: 0\r\n\r\n").expect("post");
        let methods = inspect_methods(&uri_plan, b"", &settings()).expect("uri method");
        assert_eq!(methods, vec!["status"]);

        let root =
            plan("POST / HTTP/1.1\r\nHost: node\r\nContent-Length: 0\r\n\r\n").expect("post");
        assert!(inspect_methods(&root, b"", &settings()).is_err());
    }

    #[test]
    fn post_uri_with_body_inspects_both_methods() {
        let plan = plan("POST /status HTTP/1.1\r\nHost: node\r\nContent-Length: 21\r\n\r\n")
            .expect("post");
        let methods =
            inspect_methods(&plan, br#"{"method":"eth_call"}"#, &settings()).expect("methods");
        assert_eq!(methods, vec!["status", "eth_call"]);
    }

    #[test]
    fn inspects_json_methods_exactly() {
        let methods = inspect_json(
            br#"{"jsonrpc":"2.0","method":"eth_call","params":[],"id":1}"#,
            &limits(),
            4,
        )
        .expect("valid request");
        assert_eq!(methods, vec!["eth_call"]);

        let escaped = inspect_json(br#"{"method":"\u0065th_call","id":1}"#, &limits(), 4)
            .expect("escaped method");
        assert_eq!(
            escaped,
            vec!["eth_call"],
            "escapes are decoded, not matched raw"
        );
    }

    #[test]
    fn rejects_duplicate_and_missing_method_keys() {
        assert!(
            inspect_json(
                br#"{"method":"eth_call","method":"debug_traceCall","id":1}"#,
                &limits(),
                4
            )
            .is_err()
        );
        assert!(inspect_json(br#"{"params":[],"id":1}"#, &limits(), 4).is_err());
        assert!(inspect_json(br#"{"method":7,"id":1}"#, &limits(), 4).is_err());
    }

    #[test]
    fn rejects_deep_and_wide_json() {
        let deep = format!(
            "{{\"method\":\"eth_call\",\"params\":{}{}}}",
            "[".repeat(20),
            "]".repeat(20)
        );
        assert!(inspect_json(deep.as_bytes(), &limits(), 4).is_err());

        let wide = format!(
            "{{\"method\":\"eth_call\",\"params\":[{}]}}",
            "1,".repeat(20)
        );
        assert!(inspect_json(wide.as_bytes(), &limits(), 4).is_err());
    }

    #[test]
    fn inspects_every_batch_item_and_caps_length() {
        let batch = br#"[{"method":"eth_call","id":1},{"method":"eth_blockNumber","id":2}]"#;
        let methods = inspect_json(batch, &limits(), 4).expect("batch");
        assert_eq!(methods, vec!["eth_call", "eth_blockNumber"]);

        let too_long =
            br#"[{"method":"a"},{"method":"b"},{"method":"c"},{"method":"d"},{"method":"e"}]"#;
        assert!(inspect_json(too_long, &limits(), 4).is_err());
        assert!(inspect_json(br#"[{"method":"a"},{"method":7}]"#, &limits(), 4).is_err());
        assert!(inspect_json(b"[]", &limits(), 4).is_err());
    }

    #[test]
    fn method_policy_is_exact_not_substring() {
        let config = config::L7 {
            allow_methods: vec!["eth_call".to_owned()],
            deny_methods: vec!["debug_traceCall".to_owned()],
            ..config::L7::default()
        };
        let context = L7Context::new(&config);
        let policy = RuntimePolicy::from_config(&test_config());

        assert!(context.admit(&["eth_call".to_owned()], &policy).is_ok());
        assert!(
            context
                .admit(&["eth_call_debug_trace".to_owned()], &policy)
                .is_err()
        );
        assert!(
            context
                .admit(&["x_debug_traceCall".to_owned()], &policy)
                .is_err()
        );
    }

    #[test]
    fn deny_wins_over_allow() {
        let config = config::L7 {
            allow_methods: vec!["eth_call".to_owned()],
            deny_methods: vec!["eth_call".to_owned()],
            ..config::L7::default()
        };
        let context = L7Context::new(&config);
        let policy = RuntimePolicy::from_config(&test_config());

        assert!(context.admit(&["eth_call".to_owned()], &policy).is_err());
    }

    #[test]
    fn empty_allow_list_denies_every_method() {
        let context = L7Context::new(&config::L7::default());
        let policy = RuntimePolicy::from_config(&test_config());

        assert!(context.admit(&["eth_call".to_owned()], &policy).is_err());
    }

    #[test]
    fn emergency_block_denies_every_method() {
        let mut config = test_config();
        config.policy.emergency_block = true;
        let policy = RuntimePolicy::from_config(&config);
        let context = L7Context::new(&config::L7 {
            allow_methods: vec!["eth_call".to_owned()],
            ..config::L7::default()
        });

        assert_eq!(
            context
                .admit(&["eth_call".to_owned()], &policy)
                .unwrap_err(),
            Reject::Policy
        );
    }

    #[tokio::test(start_paused = true)]
    async fn expensive_methods_consume_rate_and_concurrency() {
        let mut expensive = BTreeMap::new();
        expensive.insert(
            "eth_call".to_owned(),
            config::MethodLimit {
                rate_per_second: 2,
                concurrent: 1,
            },
        );
        let context = L7Context::new(&config::L7 {
            allow_methods: vec!["eth_call".to_owned()],
            expensive_methods: expensive,
            ..config::L7::default()
        });
        let policy = RuntimePolicy::from_config(&test_config());

        let first = context
            .admit(&["eth_call".to_owned()], &policy)
            .expect("first call");
        assert_eq!(
            context
                .admit(&["eth_call".to_owned()], &policy)
                .unwrap_err(),
            Reject::Quota,
            "concurrency limit is one"
        );
        drop(first);
        assert!(context.admit(&["eth_call".to_owned()], &policy).is_ok());
        assert_eq!(
            context
                .admit(&["eth_call".to_owned()], &policy)
                .unwrap_err(),
            Reject::Quota,
            "rate burst is two"
        );
    }

    #[test]
    fn builds_a_new_request_and_drops_client_headers() {
        let plan = plan(
            "POST / HTTP/1.1\r\nHost: node\r\nContent-Type: application/json\r\nContent-Length: 2\r\nX-Forwarded-For: 10.0.0.1\r\n\r\n",
        )
        .expect("valid post");
        let request = build_request(&plan, b"{}");
        let text = std::str::from_utf8(&request).expect("request text");

        assert!(text.starts_with("POST / HTTP/1.1\r\n"));
        assert!(text.contains("Host: node\r\n"));
        assert!(text.contains("Content-Length: 2\r\n"));
        assert!(text.contains("Connection: close\r\n"));
        assert!(!text.contains("X-Forwarded-For"));
        assert!(text.ends_with("\r\n\r\n{}"));
    }

    #[test]
    fn decodes_chunked_bodies() {
        let mut decoder = ChunkedDecoder::new(64);
        for byte in b"3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n" {
            decoder.feed(*byte).expect("chunk");
        }
        assert!(decoder.is_done());
        assert_eq!(decoder.into_body(), b"abcde");
    }

    #[test]
    fn rejects_bad_chunked_bodies() {
        for body in [
            &b"3\r\nabcX\r\n0\r\n\r\n"[..],
            &b"g\r\n"[..],
            &b"1\r\na\r\n0\r\nX"[..],
        ] {
            let mut decoder = ChunkedDecoder::new(64);
            let mut result = Ok(());
            for byte in body {
                result = decoder.feed(*byte);
                if result.is_err() {
                    break;
                }
            }
            assert!(result.is_err(), "body must be rejected");
        }
    }

    #[test]
    fn chunked_body_respects_decoded_limit() {
        let mut decoder = ChunkedDecoder::new(2);
        let mut result = Ok(());
        for byte in b"3\r\nabc\r\n0\r\n\r\n" {
            result = decoder.feed(*byte);
            if result.is_err() {
                break;
            }
        }
        assert_eq!(result, Err(Reject::Body));
    }

    fn test_config() -> config::Config {
        config::Config {
            limits: config::Limits::default(),
            policy: config::Policy {
                allow: vec![
                    "10.0.0.0/8".parse().expect("cidr"),
                    "::/0".parse().expect("cidr"),
                ],
                deny: Vec::new(),
                emergency_block: false,
            },
            observe: config::Observe::default(),
            upstreams: Vec::new(),
            listeners: Vec::new(),
        }
    }
}
