use std::io::Cursor;

use futures_util::{StreamExt, stream::SplitStream};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use wtransport::RecvStream;

use crate::{buffer::Buffer, leb::LebCodec};

pub(crate) struct StreamReader {
    pub ws: Option<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    pub wt: Option<RecvStream>,
}

impl StreamReader {
    #[inline(always)]
    pub(crate) async fn read(&mut self) -> Result<Buffer, ReaderError> {
        if let Some(stream) = self.ws.as_mut() {
            if let Some(Ok(frame)) = stream.next().await {
                if frame.is_binary() {
                    let data = frame.into_data();
                    if !data.is_empty() {
                        Ok(Buffer::new(Cursor::new(data.to_vec())))
                    } else {
                        Err(ReaderError::StreamEmpty(format!(
                            "tried to read {} byte(s) from ws",
                            data.len(),
                        )))
                    }
                } else if frame.is_close() {
                    Err(ReaderError::StreamClosed(String::from(
                        "ws stream requested to close",
                    )))
                } else if frame.is_ping() {
                    Ok(Buffer::empty())
                } else {
                    Err(ReaderError::Unknown(format!(
                        "obtained an unexpected code for ws: {frame:?}",
                    )))
                }
            } else {
                Err(ReaderError::StreamClosed(String::from(
                    "unable to obtain the next ws frame",
                )))
            }
        } else if let Some(stream) = self.wt.as_mut() {
            let mut leb = [0u8; 16];
            let mut pos = 0;
            {
                let mut recv = [0];
                loop {
                    if let Err(e) = stream.read_exact(&mut recv).await {
                        return Err(ReaderError::StreamError(format!(
                            "unable to read from stream: {e:?}"
                        )));
                    }
                    if pos > leb.len() {
                        return Err(ReaderError::StreamError(String::from(
                            "unexpected leb128 size, unable to read from stream",
                        )));
                    }
                    leb[pos] = recv[0];
                    pos += 1;
                    if recv[0] & 0x80 == 0 {
                        break;
                    }
                }
            }
            let mut size = LebCodec::decode(&leb);
            let mut data = Vec::new();
            while size > 0 {
                let mut buf = vec![0; 0xffff.min(size as usize)];
                let read = match stream.read(&mut buf).await {
                    Ok(Some(read)) => read,
                    Ok(None) => {
                        return Err(ReaderError::StreamError(String::from("unexpected eof")));
                    }
                    Err(e) => {
                        return Err(ReaderError::StreamError(format!(
                            "unable to read from stream: {e:?}"
                        )));
                    }
                };
                data.extend_from_slice(&buf[..read]);
                size -= read as u64;
            }
            Ok(Buffer::new(Cursor::new(data)))
        } else {
            Err(ReaderError::Unknown(String::from(
                "no stream open to read from",
            )))
        }
    }

    /*#[inline(always)]
    pub async fn shutdown(&mut self) {
        self.stream = None;
    }*/
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum ReaderError {
    StreamError(String),
    StreamEmpty(String),
    StreamClosed(String),
    Unknown(String),
}
