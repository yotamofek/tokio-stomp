use bytes::{BufMut, BytesMut};
use failure::{bail, format_err};

use std::borrow::Cow;

use crate::{AckMode, FromServer, Message, Result, ToServer, ToServerType};

type OptionalCowBytes<'a> = Option<Cow<'a, [u8]>>;

#[derive(Debug)]
pub(crate) struct Frame<'a> {
    command: &'a [u8],
    // TODO use ArrayVec to keep headers on the stack
    // (makes this object zero-allocation)
    headers: Vec<(&'a [u8], Cow<'a, [u8]>)>,
    body: Option<&'a [u8]>,
}

impl<'a> Frame<'a> {
    pub(crate) fn new(
        command: &'a [u8],
        headers: &[(&'a [u8], OptionalCowBytes<'a>)],
        body: Option<&'a [u8]>,
    ) -> Frame<'a> {
        let headers = headers
            .iter()
            // filter out headers with None value
            .filter_map(|(k, v)| v.as_ref().cloned().map(|v| (*k, v)))
            .collect();
        
        Frame {
            command,
            headers,
            body,
        }
    }

    pub(crate) fn serialize(&self, buffer: &mut BytesMut) {
        fn write_escaped(b: u8, buffer: &mut BytesMut) {
            match b {
                b'\r' => {
                    buffer.put_u8(b'\\');
                    buffer.put_u8(b'r')
                }
                b'\n' => {
                    buffer.put_u8(b'\\');
                    buffer.put_u8(b'n')
                }
                b':' => {
                    buffer.put_u8(b'\\');
                    buffer.put_u8(b'c')
                }
                b'\\' => {
                    buffer.put_u8(b'\\');
                    buffer.put_u8(b'\\')
                }
                b => buffer.put_u8(b),
            }
        }
        let requires = self.command.len()
            + self.body.map(|b| b.len() + 20).unwrap_or(0)
            + self
                .headers
                .iter()
                .fold(0, |acc, &(ref k, ref v)| acc + k.len() + v.len())
            + 30;
        if buffer.remaining_mut() < requires {
            buffer.reserve(requires);
        }
        buffer.put_slice(self.command);
        buffer.put_u8(b'\n');
        self.headers.iter().for_each(|&(key, ref val)| {
            for byte in key {
                write_escaped(*byte, buffer);
            }
            buffer.put_u8(b':');
            for byte in val.iter() {
                write_escaped(*byte, buffer);
            }
            buffer.put_u8(b'\n');
        });
        if let Some(body) = self.body {
            buffer.put_slice(&get_content_length_header(&body));
            buffer.put_u8(b'\n');
            buffer.put_slice(body);
        } else {
            buffer.put_u8(b'\n');
        }
        buffer.put_u8(b'\x00');
    }
}

// Nom definitions

named!(eol, preceded!(opt!(tag!("\r")), tag!("\n")));

named!(
    parse_header<(&[u8], Cow<[u8]>)>,
    pair!(
        take_until_either!(":\n"),
        preceded!(
            tag!(":"),
            map!(take_until_and_consume!("\n"), |bytes| Cow::Borrowed(
                strip_cr(bytes)
            ))
        )
    )
);

fn get_content_length(headers: &[(&[u8], Cow<[u8]>)]) -> Option<u32> {
    headers
        .iter()
        .find(|(name, _)| name == b"content-length")
        .and_then(|(_, value)| std::str::from_utf8(value).ok())
        .and_then(|value| value.parse().ok())
}

fn is_empty_slice(s: &[u8]) -> Option<&[u8]> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

named!(
    pub(crate) parse_frame<Frame>,
    do_parse!(
        many0!(eol)
            >> command: map!(take_until_and_consume!("\n"), strip_cr)
            >> headers: many0!(parse_header)
            >> eol
            >> body: switch!(value!(get_content_length(&*headers)),
                Some(v) => map!(take!(v), Some) |
                None => map!(take_until!("\x00"), is_empty_slice)
            )
            >> tag!("\x00")
            >> many0!(complete!(eol))
            >> (Frame {
                command,
                headers,
                body,
            })
    )
);

fn strip_cr(buf: &[u8]) -> &[u8] {
    if let Some(&b'\r') = buf.last() {
        &buf[..buf.len() - 1]
    } else {
        buf
    }
}

fn fetch_header<'a>(headers: &'a [(&'a [u8], Cow<'a, [u8]>)], key: &'a str) -> Option<String> {
    let kk = key.as_bytes();
    for &(k, ref v) in headers {
        if &*k == kk {
            return String::from_utf8(v.to_vec()).ok();
        }
    }
    None
}

fn expect_header<'a>(headers: &'a [(&'a [u8], Cow<'a, [u8]>)], key: &'a str) -> Result<String> {
    fetch_header(headers, key).ok_or_else(|| format_err!("Expected header '{}' missing", key))
}

impl<'a> Frame<'a> {
    #[allow(dead_code)]
    pub(crate) fn to_client_msg(&'a self) -> Result<Message<ToServer>> {
        use self::expect_header as eh;
        use self::fetch_header as fh;
        use ToServer::*;
        let h = &self.headers;

        let command_type = match ToServerType::parse_from_bytes(self.command) {
            Some(command_type) => command_type,
            None => bail!(
                "Frame not recognized: {:?}",
                String::from_utf8_lossy(self.command)
            ),
        };

        let expected_headers = command_type.expected_headers();

        let content = match command_type {
            ToServerType::Connect => {
                let heartbeat = fh(h, "heart-beat").map(parse_heartbeat).transpose()?;

                Connect {
                    accept_version: eh(h, "accept-version")?,
                    host: eh(h, "host")?,
                    login: fh(h, "login"),
                    passcode: fh(h, "passcode"),
                    heartbeat,
                }
            }
            ToServerType::Disconnect => {
                Disconnect {
                    receipt: fh(h, "receipt"),
                }
            }
            ToServerType::Send => {
                Send {
                    destination: eh(h, "destination")?,
                    transaction: fh(h, "transaction"),
                    body: self.body.map(|v| v.to_vec()),
                }
            }
            ToServerType::Subscribe => {
                Subscribe {
                    destination: eh(h, "destination")?,
                    id: eh(h, "id")?,
                    ack: match fh(h, "ack").as_deref() {
                        Some("auto") => Some(AckMode::Auto),
                        Some("client") => Some(AckMode::Client),
                        Some("client-individual") => Some(AckMode::ClientIndividual),
                        Some(other) => bail!("Invalid ack mode: {}", other),
                        None => None,
                    },
                }
            }
            ToServerType::Unsubscribe => {
                Unsubscribe { id: eh(h, "id")? }
            }
            ToServerType::Ack => {
                Ack {
                    id: eh(h, "id")?,
                    transaction: fh(h, "transaction"),
                }
            }
            ToServerType::Nack => {
                Nack {
                    id: eh(h, "id")?,
                    transaction: fh(h, "transaction"),
                }
            }
            ToServerType::Begin => {
                Begin {
                    transaction: eh(h, "transaction")?,
                }
            }
            ToServerType::Commit => {
                Commit {
                    transaction: eh(h, "transaction")?,
                }
            }
            ToServerType::Abort => {
                Abort {
                    transaction: eh(h, "transaction")?,
                }
            }
        };

        let extra_headers = h
            .iter()
            .filter_map(|&(k, ref v)| {
                if !expected_headers.contains(&k) {
                    Some((k.to_vec(), (&*v).to_vec()))
                } else {
                    None
                }
            })
            .collect();
        Ok(Message {
            content,
            extra_headers,
        })
    }

    pub(crate) fn to_server_msg(&'a self) -> Result<Message<FromServer>> {
        use self::expect_header as eh;
        use self::fetch_header as fh;
        use FromServer::{Connected, Error, Message as Msg, Receipt};
        let h = &self.headers;
        let expect_keys: &[&[u8]];
        let content = match self.command {
            b"CONNECTED" | b"connected" => {
                expect_keys = &[b"version", b"session", b"server", b"heart-beat"];
                Connected {
                    version: eh(h, "version")?,
                    session: fh(h, "session"),
                    server: fh(h, "server"),
                    heartbeat: fh(h, "heart-beat"),
                }
            }
            b"MESSAGE" | b"message" => {
                expect_keys = &[b"destination", b"message-id", b"subscription"];
                Msg {
                    destination: eh(h, "destination")?,
                    message_id: eh(h, "message-id")?,
                    subscription: eh(h, "subscription")?,
                    body: self.body.map(|v| v.to_vec()),
                }
            }
            b"RECEIPT" | b"receipt" => {
                expect_keys = &[b"receipt-id"];
                Receipt {
                    receipt_id: eh(h, "receipt-id")?,
                }
            }
            b"ERROR" | b"error" => {
                expect_keys = &[b"message"];
                Error {
                    message: fh(h, "message"),
                    body: self.body.map(|v| v.to_vec()),
                }
            }
            other => bail!("Frame not recognized: {:?}", String::from_utf8_lossy(other)),
        };
        let extra_headers = h
            .iter()
            .filter_map(|&(k, ref v)| {
                if !expect_keys.contains(&k) {
                    Some((k.to_vec(), (&*v).to_vec()))
                } else {
                    None
                }
            })
            .collect();
        Ok(Message {
            content,
            extra_headers,
        })
    }
}

fn opt_str_to_bytes(s: &Option<String>) -> Option<Cow<'_, [u8]>> {
    s.as_ref().map(|v| Cow::Borrowed(v.as_bytes()))
}

fn get_content_length_header(body: &[u8]) -> Vec<u8> {
    format!("content-length:{}\n", body.len()).into()
}

fn parse_heartbeat<S: AsRef<str>>(hb: S) -> Result<(u32, u32)> {
    let mut split = hb.as_ref().splitn(1, ',');
    let left = split.next().ok_or_else(|| format_err!("Bad heartbeat"))?;
    let right = split.next().ok_or_else(|| format_err!("Bad heartbeat"))?;
    Ok((left.parse()?, right.parse()?))
}

impl ToServer {
    pub(crate) fn to_frame(&self) -> Frame {
        use self::opt_str_to_bytes as sb;
        use Cow::*;
        use ToServer::*;
        match *self {
            Connect {
                ref accept_version,
                ref host,
                ref login,
                ref passcode,
                ref heartbeat,
            } => Frame::new(
                b"CONNECT",
                &[
                    (b"accept-version", Some(Borrowed(accept_version.as_bytes()))),
                    (b"host", Some(Borrowed(host.as_bytes()))),
                    (b"login", sb(login)),
                    (b"passcode", sb(passcode)),
                    (
                        b"heart-beat",
                        heartbeat.map(|(v1, v2)| Owned(format!("{},{}", v1, v2).into())),
                    ),
                ],
                None,
            ),
            Disconnect { ref receipt } => {
                Frame::new(b"DISCONNECT", &[(b"receipt", sb(&receipt))], None)
            }
            Subscribe {
                ref destination,
                ref id,
                ref ack,
            } => Frame::new(
                b"SUBSCRIBE",
                &[
                    (b"destination", Some(Borrowed(destination.as_bytes()))),
                    (b"id", Some(Borrowed(id.as_bytes()))),
                    (
                        b"ack",
                        ack.map(|ack| match ack {
                            AckMode::Auto => Borrowed(&b"auto"[..]),
                            AckMode::Client => Borrowed(&b"client"[..]),
                            AckMode::ClientIndividual => Borrowed(&b"client-individual"[..]),
                        }),
                    ),
                ],
                None,
            ),
            Unsubscribe { ref id } => Frame::new(
                b"UNSUBSCRIBE",
                &[(b"id", Some(Borrowed(id.as_bytes())))],
                None,
            ),
            Send {
                ref destination,
                ref transaction,
                ref body,
            } => Frame::new(
                b"SEND",
                &[
                    (b"destination", Some(Borrowed(destination.as_bytes()))),
                    (b"id", sb(transaction)),
                ],
                body.as_ref().map(|v| v.as_ref()),
            ),
            Ack {
                ref id,
                ref transaction,
            } => Frame::new(
                b"ACK",
                &[
                    (b"id", Some(Borrowed(id.as_bytes()))),
                    (b"transaction", sb(transaction)),
                ],
                None,
            ),
            Nack {
                ref id,
                ref transaction,
            } => Frame::new(
                b"NACK",
                &[
                    (b"id", Some(Borrowed(id.as_bytes()))),
                    (b"transaction", sb(transaction)),
                ],
                None,
            ),
            Begin { ref transaction } => Frame::new(
                b"BEGIN",
                &[(b"transaction", Some(Borrowed(transaction.as_bytes())))],
                None,
            ),
            Commit { ref transaction } => Frame::new(
                b"COMMIT",
                &[(b"transaction", Some(Borrowed(transaction.as_bytes())))],
                None,
            ),
            Abort { ref transaction } => Frame::new(
                b"ABORT",
                &[(b"transaction", Some(Borrowed(transaction.as_bytes())))],
                None,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_serialize_connect() {
        let data = b"CONNECT
accept-version:1.2
host:datafeeds.here.co.uk
login:user
passcode:password\n\n\x00"
            .to_vec();
        let (_, frame) = parse_frame(&data).unwrap();
        assert_eq!(frame.command, b"CONNECT");
        let headers_expect: Vec<(&[u8], &[u8])> = vec![
            (&b"accept-version"[..], &b"1.2"[..]),
            (b"host", b"datafeeds.here.co.uk"),
            (b"login", b"user"),
            (b"passcode", b"password"),
        ];
        let fh: Vec<_> = frame.headers.iter().map(|&(k, ref v)| (k, &**v)).collect();
        assert_eq!(fh, headers_expect);
        assert_eq!(frame.body, None);
        let stomp = frame.to_client_msg().unwrap();
        let mut buffer = BytesMut::new();
        stomp.to_frame().serialize(&mut buffer);
        assert_eq!(&*buffer, &*data);
    }

    #[test]
    fn parse_and_serialize_message() {
        let mut data = b"\nMESSAGE
destination:datafeeds.here.co.uk
message-id:12345
subscription:some-id
empty-header:
"
        .to_vec();
        let body = "this body contains \x00 nulls \n and \r\n newlines \x00 OK?";
        let rest = format!("content-length:{}\n\n{}\x00", body.len(), body);
        data.extend_from_slice(rest.as_bytes());
        let (_, frame) = parse_frame(&data).unwrap();
        assert_eq!(frame.command, b"MESSAGE");
        let headers_expect: Vec<(&[u8], &[u8])> = vec![
            (&b"destination"[..], &b"datafeeds.here.co.uk"[..]),
            (b"message-id", b"12345"),
            (b"subscription", b"some-id"),
            (b"empty-header", b""),
            (b"content-length", b"50"),
        ];
        let fh: Vec<_> = frame.headers.iter().map(|&(k, ref v)| (k, &**v)).collect();
        assert_eq!(fh, headers_expect);
        assert_eq!(frame.body, Some(body.as_bytes()));
        frame.to_server_msg().unwrap();
        // TODO to_frame for FromServer
        // let roundtrip = stomp.to_frame().serialize();
        // assert_eq!(roundtrip, data);
    }
}
