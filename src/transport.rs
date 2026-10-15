use bytes::Bytes;
use http::{HeaderValue, Method, Request, Uri, header, uri::Authority};
use http_body_util::Full;
use hyper::client::conn::http1;
use hyper_util::client::legacy::connect::HttpConnector;
use std::future::poll_fn;
use std::{
    collections::HashMap,
    error::Error,
    net::IpAddr,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;
use tower::Service;
use url::Url;

mod body;
pub(super) use body::RetainedBody;

type BoxError = Box<dyn Error + Send + Sync>;

static CONNECTORS: OnceLock<Mutex<HashMap<Option<IpAddr>, HttpConnector>>> = OnceLock::new();

fn connector_for(source: Option<IpAddr>) -> HttpConnector {
    CONNECTORS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .entry(source)
        .or_insert_with(|| {
            let mut connector = HttpConnector::new();
            connector.set_local_address(source);
            connector
        })
        .clone()
}

pub(super) struct ParsedBase {
    base: Arc<Url>,
    uri: Option<Uri>,
    authority: Option<Authority>,
    host: Option<HeaderValue>,
}

impl ParsedBase {
    pub fn new(base: Arc<Url>) -> Arc<Self> {
        let uri = base.as_str().parse::<Uri>().ok();
        let authority = uri.as_ref().and_then(|uri| uri.authority().cloned());
        let host = authority.as_ref().and_then(|authority| {
            let host = authority
                .as_str()
                .rsplit_once('@')
                .map_or(authority.as_str(), |(_, host)| host);
            HeaderValue::from_str(host).ok()
        });
        Arc::new(Self {
            base,
            uri,
            authority,
            host,
        })
    }

    pub fn url(&self) -> &Arc<Url> {
        &self.base
    }
}

pub(super) struct Client {
    base: Arc<ParsedBase>,
    connector: HttpConnector,
    sender: Option<http1::SendRequest<Full<Bytes>>>,
    connected_authority: Option<Authority>,
    connection_task: Option<JoinHandle<()>>,
    request_started: Option<Instant>,
    request_elapsed: Duration,
    connections: Arc<Mutex<super::connections::Connections>>,
}

pub(super) struct Response {
    pub status: u16,
    pub retained: Option<RetainedBody>,
    pub error_excerpt: Bytes,
    pub failure: Option<(&'static str, String)>,
    pub latency_ms: f64,
}

impl Response {
    pub(super) fn failed(
        status: u16,
        kind: &'static str,
        reason: impl Into<String>,
        latency_ms: f64,
    ) -> Self {
        Self {
            status,
            retained: None,
            error_excerpt: Bytes::new(),
            failure: Some((kind, reason.into())),
            latency_ms,
        }
    }
}

impl Client {
    pub fn new(
        base: Arc<ParsedBase>,
        source: Option<IpAddr>,
        connections: Arc<Mutex<super::connections::Connections>>,
    ) -> Self {
        Self {
            base,
            connector: connector_for(source),
            sender: None,
            connected_authority: None,
            connection_task: None,
            request_started: None,
            request_elapsed: Duration::ZERO,
            connections,
        }
    }

    pub async fn request(
        &mut self,
        method: Method,
        path: &str,
        token: Option<&HeaderValue>,
        request_body: Option<Bytes>,
        keep_feed_body: bool,
        capture_excerpt: bool,
    ) -> Result<Response, BoxError> {
        let mut redirect_url: Option<Url> = None;
        let mut method = method;
        let mut request_body = request_body;
        let mut token = token;
        let mut redirect_count = 0;
        self.request_started = None;
        self.request_elapsed = Duration::ZERO;
        loop {
            let uri = if let Some(url) = &redirect_url {
                url.as_str().parse()?
            } else {
                request_uri(self.base.uri.as_ref().ok_or("invalid base HTTP URI")?, path)?
            };
            if self.connected_authority.as_ref() != uri.authority() {
                self.discard_connection();
            }
            let mut reconnects = 0;
            let response = loop {
                if self.sender.is_none() {
                    self.pause_request_latency();
                    Box::pin(self.connect(uri.clone())).await?;
                }
                if let Err(error) = self
                    .sender
                    .as_mut()
                    .expect("connected sender")
                    .ready()
                    .await
                {
                    self.pause_request_latency();
                    self.discard_connection();
                    if reconnects == 0 {
                        reconnects += 1;
                        continue;
                    }
                    return Err(Box::new(error));
                }
                self.resume_request_latency();
                let mut request = Request::builder()
                    .method(method.clone())
                    .uri(origin_form(&uri)?);
                let headers = request.headers_mut().expect("valid request builder");
                let authority = uri.authority().ok_or("missing authority")?;
                if self.base.authority.as_ref() == Some(authority) {
                    headers.insert(
                        header::HOST,
                        self.base.host.as_ref().expect("base host parses").clone(),
                    );
                } else {
                    let host = authority
                        .as_str()
                        .rsplit_once('@')
                        .map_or(authority.as_str(), |(_, host)| host);
                    headers.insert(header::HOST, HeaderValue::from_str(host)?);
                }
                headers.insert(header::ACCEPT, HeaderValue::from_static("*/*"));
                if let Some(token) = token {
                    headers.insert(header::AUTHORIZATION, token.clone());
                    headers.insert(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                }
                let request = request.body(Full::new(request_body.clone().unwrap_or_default()))?;
                let sent = self
                    .sender
                    .as_mut()
                    .expect("connected sender")
                    .try_send_request(request)
                    .await;
                match sent {
                    Ok(response) => break response,
                    Err(error) => {
                        let can_retry = error.message().is_some() && reconnects == 0;
                        self.discard_connection();
                        if can_retry {
                            self.pause_request_latency();
                            reconnects += 1;
                            continue;
                        }
                        return Err(Box::new(error.into_error()));
                    }
                }
            };
            let status = response.status().as_u16();
            if is_redirect(status)
                && redirect_count < 10
                && let Some(location) = response.headers().get(header::LOCATION)
            {
                let location = std::str::from_utf8(location.as_bytes())?;
                let current_url = match redirect_url.take() {
                    Some(url) => url,
                    None => self.base.base.join(path)?,
                };
                let next = current_url.join(location)?;
                let same_origin = current_url.origin() == next.origin();
                let next_uri: Uri = next.as_str().parse()?;
                if let Err(error) = body::read_response(response.into_body(), false, false, 0).await
                {
                    self.discard_connection();
                    return Err(Box::new(error));
                }
                if status == 303 || ((status == 301 || status == 302) && method == Method::POST) {
                    method = Method::GET;
                    request_body = None;
                }
                if !same_origin {
                    token = None;
                }
                if uri.authority() != next_uri.authority() {
                    self.discard_connection();
                }
                redirect_url = Some(next);
                redirect_count += 1;
                continue;
            }
            let gzip = response
                .headers()
                .get(header::CONTENT_ENCODING)
                .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"gzip"));
            let retain = keep_feed_body && status == 200;
            let excerpt_cap = if !retain && !(200..400).contains(&status) && capture_excerpt {
                1025
            } else {
                0
            };
            return match body::read_response(response.into_body(), gzip, retain, excerpt_cap).await
            {
                Ok(body) => Ok(Response {
                    status,
                    retained: body.retained,
                    error_excerpt: body.error_excerpt,
                    failure: None,
                    latency_ms: self.take_request_latency_ms(),
                }),
                Err(error) => {
                    self.discard_connection();
                    Ok(Response::failed(
                        status,
                        "body_read",
                        error.to_string(),
                        self.take_request_latency_ms(),
                    ))
                }
            };
        }
    }

    pub fn discard_connection(&mut self) {
        self.sender = None;
        self.connected_authority = None;
        if let Some(task) = self.connection_task.take() {
            task.abort();
        }
    }

    pub fn take_request_latency_ms(&mut self) -> f64 {
        self.pause_request_latency();
        std::mem::take(&mut self.request_elapsed).as_secs_f64() * 1000.0
    }

    fn pause_request_latency(&mut self) {
        if let Some(started) = self.request_started.take() {
            self.request_elapsed = self.request_elapsed.saturating_add(started.elapsed());
        }
    }

    fn resume_request_latency(&mut self) {
        self.request_started.get_or_insert_with(Instant::now);
    }

    async fn connect(&mut self, uri: Uri) -> Result<(), BoxError> {
        let authority = uri.authority().cloned();
        let mut attempt = super::connections::ConnectionAttempt::start(self.connections.clone());
        let result = async {
            poll_fn(|cx| self.connector.poll_ready(cx)).await?;
            let io = self.connector.call(uri).await?;
            let mut builder = http1::Builder::new();
            builder.read_buf_chunk_size(1024);
            let (sender, connection) = builder.handshake(io).await?;
            Ok::<_, BoxError>((sender, connection))
        }
        .await;
        match result {
            Ok((sender, connection)) => {
                attempt.finish(None);
                self.sender = Some(sender);
                self.connected_authority = authority;
                self.connection_task = Some(tokio::spawn(async move {
                    let _ = connection.await;
                }));
                Ok(())
            }
            Err(error) => {
                attempt.finish(Some(error.to_string()));
                Err(error)
            }
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(task) = self.connection_task.take() {
            task.abort();
        }
    }
}

fn request_uri(base: &Uri, path: &str) -> Result<Uri, BoxError> {
    let scheme = base.scheme_str().ok_or("base URL is missing a scheme")?;
    let authority = base.authority().ok_or("base URL is missing an authority")?;
    Ok(Uri::builder()
        .scheme(scheme)
        .authority(authority.clone())
        .path_and_query(path)
        .build()?)
}

fn origin_form(uri: &Uri) -> Result<Uri, BoxError> {
    Ok(uri
        .path_and_query()
        .map_or("/", |path| path.as_str())
        .parse()?)
}

const fn is_redirect(status: u16) -> bool {
    matches!(status, 301 | 302 | 303 | 307 | 308)
}
