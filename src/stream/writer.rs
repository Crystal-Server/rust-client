use std::io::{Cursor, Error, ErrorKind, Result as IoResult};

use bytes::Bytes;
use futures_util::{SinkExt, stream::SplitSink};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};
use wtransport::SendStream;

use crate::{buffer::Buffer, unwrap_return};

pub(crate) struct StreamWriter {
    pub ws: Option<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
    pub wt: Option<SendStream>,
}

impl StreamWriter {
    #[inline(always)]
    pub(crate) async fn write(&mut self, data: &Buffer) -> Result<(), WriterError> {
        /*#[cfg(feature = "__dev")]
        info!("wrote data: {:?}", data.container.get_ref().to_str_lossy());*/
        if let Some(stream) = self.ws.as_mut() {
            unwrap_return!(
                stream
                    .send(Message::Binary(Bytes::copy_from_slice(
                        data.container.get_ref()
                    )))
                    .await,
                Err(WriterError::StreamError(format!(
                    "unable to write {:?} byte(s) to a ws",
                    data.container.get_ref().len(),
                )))
            );
            unwrap_return!(
                stream.flush().await,
                Err(WriterError::StreamError(String::from(
                    "unable to flush the writer of a ws {:?}",
                )))
            );
            Ok(())
        } else if let Some(stream) = self.wt.as_mut() {
            let base_len = data.container.get_ref().len();
            let mut buf = Buffer::new(Cursor::new(Vec::with_capacity(base_len + 16)));
            unwrap_return!(
                buf.write_leb_u64(base_len as u64),
                Err(WriterError::StreamError(String::from(
                    "unable to write to final buffer"
                )))
            );
            unwrap_return!(
                buf.write_all(data.container.get_ref()),
                Err(WriterError::StreamError(String::from(
                    "unable to write to final buffer"
                )))
            );
            unwrap_return!(
                stream.write_all(buf.container.get_ref()).await,
                Err(WriterError::StreamError(format!(
                    "unable to write {:?} byte(s) to a wt",
                    buf.container.get_ref().len()
                )))
            );
            Ok(())
        } else {
            Err(WriterError::Unknown(String::from(
                "no stream open to write to",
            )))
        }
    }

    #[inline(always)]
    pub async fn write_pong(&mut self) -> IoResult<()> {
        if let Some(stream) = self.ws.as_mut() {
            if stream.send(Message::Pong(Bytes::new())).await.is_err() {
                Err(Error::from(ErrorKind::BrokenPipe))
            } else {
                Ok(())
            }
        } else if self.wt.is_none() {
            Err(Error::from(ErrorKind::BrokenPipe))
        } else {
            Ok(())
        }
    }

    #[inline(always)]
    pub async fn shutdown(&mut self) {
        if let Some(mut stream) = self.ws.take() {
            let _ = stream.close().await;
        }
        if let Some(mut stream) = self.wt.take() {
            let _ = stream.finish().await;
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub enum WriterError {
    StreamError(String),
    StreamClosed(String),
    Unknown(String),
}
