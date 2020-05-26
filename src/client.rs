use std::net::ToSocketAddrs;

use bytes::{Buf, BytesMut};
use futures::prelude::*;
use futures::sink::SinkExt;
use tokio::io::{AsyncRead, AsyncWrite};

use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

type ClientTransport<S> = Framed<S, ClientCodec>;

use crate::frame;
use crate::{FromServer, Message, Result, ToServer};

/// Connect to a STOMP server via TCP, including the connection handshake.
/// If successful, returns a tuple of a message stream and a sender,
/// which may be used to receive and send messages respectively.
pub async fn connect(
    address: impl Into<String>,
    login: Option<String>,
    passcode: Option<String>,
) -> Result<
    impl Stream<Item = Result<Message<FromServer>>> + Sink<Message<ToServer>, Error = failure::Error>,
> {
    let address = address.into();
    let addr = address.as_str().to_socket_addrs().unwrap().next().unwrap();
    let tcp = TcpStream::connect(&addr).await?;
    let mut transport = ClientCodec.framed(tcp);
    client_handshake(&mut transport, address, login, passcode).await?;
    Ok(transport)
}

/// Connect to a STOMP server via TCP, including the connection handshake.
/// If successful, returns a tuple of a message stream and a sender,
/// which may be used to receive and send messages respectively.
pub async fn connect_stream<S>(
    stream: S,
    host: String,
    login: Option<String>,
    passcode: Option<String>,
) -> Result<
    impl Stream<Item = Result<Message<FromServer>>> + Sink<Message<ToServer>, Error = failure::Error>,
> where S: AsyncRead + AsyncWrite + Sized + Unpin {
    let mut transport = ClientCodec.framed(stream);
    client_handshake(&mut transport, host, login, passcode).await?;
    Ok(transport)
}

async fn client_handshake<S>(
    transport: &mut ClientTransport<S>,
    host: String,
    login: Option<String>,
    passcode: Option<String>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Sized + Unpin,
{
    let connect = Message {
        content: ToServer::Connect {
            accept_version: "1.2".into(),
            host,
            login,
            passcode,
            heartbeat: None,
        },
        extra_headers: vec![],
    };
    // Send the message
    transport.send(connect).await?;
    // Receive reply
    let msg = transport.next().await.transpose()?;
    if let Some(FromServer::Connected { .. }) = msg.as_ref().map(|m| &m.content) {
        Ok(())
    } else {
        Err(failure::format_err!("unexpected reply: {:?}", msg))
    }
}

/// Convenience function to build a Subscribe message
pub fn subscribe(dest: impl Into<String>, id: impl Into<String>) -> Message<ToServer> {
    ToServer::Subscribe {
        destination: dest.into(),
        id: id.into(),
        ack: None,
    }
    .into()
}

struct ClientCodec;

impl Decoder for ClientCodec {
    type Item = Message<FromServer>;
    type Error = failure::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let (item, offset) = match frame::parse_frame(&src) {
            Ok((remain, frame)) => (
                Message::<FromServer>::from_frame(frame),
                remain.as_ptr() as usize - src.as_ptr() as usize,
            ),
            Err(nom::Err::Incomplete(_)) => return Ok(None),
            Err(e) => failure::bail!("Parse failed: {:?}", e),
        };
        src.advance(offset);
        item.map(Some)
    }
}

impl Encoder for ClientCodec {
    type Item = Message<ToServer>;
    type Error = failure::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<()> {
        item.to_frame().serialize(dst);
        Ok(())
    }
}
