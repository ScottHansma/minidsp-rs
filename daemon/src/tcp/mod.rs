//! TCP server compatible with the official mobile and desktop application
use core::panic;
use std::{
    collections::VecDeque,
    net::{Ipv4Addr, ToSocketAddrs},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::{pin_mut, SinkExt, StreamExt};
use minidsp::{
    commands::{Commands, Responses},
    packet,
    transport::{
        net::{discovery, Codec},
        Transport,
    },
    MiniDSPError,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    select,
};
use tokio_util::codec::Framed;

use crate::{device_manager::Device, App};

use super::config;

/// Forwards a TCP client's traffic to the shared USB transport with per-client
/// command-response filtering.
///
/// The Hub broadcasts every device response to all consumers (this TCP client,
/// the daemon's HTTP-API multiplexer, any other connected TCP client). Without
/// filtering, an unrelated response (e.g., from a `GET /devices/0/config` poll
/// HA fired during this client's session) would be interleaved into the TCP
/// stream and crash strict clients like the macOS Device Console (which
/// asserts on the byte layout of every response it receives).
///
/// To prevent that: parse outgoing TCP frames as `Commands` and queue them;
/// when a device response arrives, parse it as a `Responses`, match against
/// the queue's front, and only forward when it matches. Responses that don't
/// match (because they belong to a different consumer's command, or because
/// they're an unsolicited push event) are dropped on the TCP path. The
/// trade-off is that this client won't see unsolicited events like front-panel
/// knob notifications — Device Console's polling catches up within seconds.
async fn forward<T>(tcp: T, device: Transport) -> Result<()>
where
    T: AsyncRead + AsyncWrite + 'static,
{
    // Truncate each HID frame after its length-byte prefix.
    let mut device = device.map(|frame| {
        let frame = frame?;
        if frame.is_empty() {
            return Err(MiniDSPError::MalformedResponse(
                "Received an empty frame".to_string(),
            ));
        }

        let len = frame[0] as usize;
        if frame.len() < len {
            return Err(MiniDSPError::MalformedResponse(format!(
                "Expected frame of length {}, got {}",
                len,
                frame.len()
            )));
        }

        Ok::<_, MiniDSPError>(frame.slice(0..len))
    });

    // Apply framing to the TCP stream
    let remote = Framed::new(tcp, Codec::new_server());
    pin_mut!(remote);

    let mut pending: VecDeque<Commands> = VecDeque::new();

    loop {
        select! {
            frame = device.next() => {
                match frame {
                    Some(frame) => {
                        let frame = frame?;
                        if response_matches_pending(&frame, &mut pending) {
                            remote.send(frame).await.context("remote.send failed")?;
                        }
                        // else: response is for another consumer (or
                        // unsolicited); drop on this TCP path.
                    }
                    None => {
                        return Err(MiniDSPError::TransportClosed.into());
                    }
                }
            },
            frame = remote.next() => {
                let frame = frame.ok_or(MiniDSPError::TransportClosed)?
                    .context("decoding frame")?;
                if let Some(cmd) = parse_outgoing(&frame) {
                    pending.push_back(cmd);
                }
                device.send(frame).await.context("device_tx.send failed")?;
            },
        }
    }
}

/// Parse an outgoing TCP frame as a `Commands` so we know what response shape
/// to expect on the way back. Returns `None` for un-parseable frames; those
/// will be forwarded but won't filter any incoming response.
fn parse_outgoing(frame: &Bytes) -> Option<Commands> {
    let payload = packet::unframe(frame.clone()).ok()?;
    Commands::from_bytes(payload).ok()
}

/// Check whether an incoming device response matches the front of the TCP
/// client's pending queue. Pops the queue on match.
///
/// The default `Commands::matches_response` returns true unconditionally for
/// `Commands::Unknown` — too permissive when the queue front is unknown but
/// the device just happened to respond to someone else's known command. We
/// override that case with a strict cmd_id equality check.
fn response_matches_pending(
    frame: &Bytes,
    pending: &mut VecDeque<Commands>,
) -> bool {
    let payload = match packet::unframe(frame.clone()) {
        Ok(p) => p,
        Err(_) => return false,
    };
    let response = match Responses::from_bytes(payload) {
        Ok(r) => r,
        Err(_) => return false,
    };
    let Some(front) = pending.front() else {
        return false;
    };
    let matches = match (front, &response) {
        (
            Commands::Unknown { cmd_id: req, .. },
            Responses::Unknown { cmd_id: resp, .. },
        ) => req == resp,
        (Commands::Unknown { .. }, _) => false,
        _ => front.matches_response(&response),
    };
    if matches {
        pending.pop_front();
    }
    matches
}

pub fn start_advertise(_app: &App, cfg: Arc<config::TcpServer>) -> Result<(), anyhow::Error> {
    // Process only THIS tcp_server's advertise block (one call per [[tcp_server]]).
    // The previous code looped over `app.config.tcp_servers` but used the outer
    // `cfg` for device_matches() — with multiple tcp_servers each main() spawned
    // an advertiser per other tcp_server while always carrying the outer cfg's
    // device data. Result: crossed identities in broadcasts that confused
    // Device Console (entries flickered, devices showed blank / "no presets").
    {
        let srv = cfg.as_ref();
        if let Some(ref advertise) = srv.advertise {
            let ip_address = Ipv4Addr::from_str(&advertise.ip)?;
            let hostname: Arc<str> = Arc::from(advertise.name.clone());
            let cfg = cfg.clone();
            let packet_fn = move || -> Option<discovery::DiscoveryPacket> {
                // Find a suitable device to forward this client to
                let device = device_matches(&cfg).ok()?;
                let device_info = device.device_info()?;

                // Derive the MAC from the device serial via a multiplicative
                // hash so the bytes scatter widely. The Device Console app uses
                // the MAC to distinguish devices on the device-list page; the
                // previous IP-based MAC was fine when one daemon served one DSP
                // but with two devices on the same daemon the IPs differed in
                // only one byte, the MACs differed in only one byte, and the UI
                // could not tell the entries apart (entries flickered, selection
                // wouldn't stick). First octet 0x02 = locally-administered
                // unicast — must not have the multicast bit set or the apps
                // drop the packet entirely.
                let h = device_info.serial.wrapping_mul(2_654_435_761);
                let mut packet = discovery::DiscoveryPacket {
                    mac_address: [
                        0x02,
                        ((h >> 24) & 0xFF) as u8,
                        ((h >> 16) & 0xFF) as u8,
                        ((h >> 8) & 0xFF) as u8,
                        (h & 0xFF) as u8,
                        0xFF,
                    ],
                    ip_address,
                    hwid: device_info.hw_id,
                    dsp_id: device_info.dsp_version,
                    fw_major: device_info.fw_major,
                    fw_minor: device_info.fw_minor,
                    sn: ((device_info.serial - 900000) & 0xFFFF) as u16,
                    hostname: hostname.to_string(),
                };

                Some(packet)
            };

            let bind_addr = match &advertise.bind_address {
                None => None,
                Some(addr) => Some(addr.to_socket_addrs()?.next().ok_or_else(|| {
                    anyhow::anyhow!("bind adddress didn't resolve to a usable address")
                })?),
            };

            let interval = Duration::from_secs(1);
            tokio::spawn(discovery::server::advertise_packet(
                bind_addr, packet_fn, interval,
            ));
        }
    }
    Ok(())
}

pub async fn main(cfg: config::TcpServer) -> Result<(), MiniDSPError> {
    let app = super::APP.get().unwrap();
    let app = app.read().await;
    let cfg = Arc::new(cfg);

    let bind_address = cfg.bind_address.as_deref().unwrap_or("0.0.0.0:5333");

    if let Err(adv_err) = start_advertise(&app, cfg.clone()) {
        log::error!("error launching advertisement task: {adv_err:?}");
    }

    let listener = TcpListener::bind(&bind_address).await?;
    log::info!("Listening on {}", &bind_address);
    loop {
        select! {
           result = listener.accept() => {
                let (stream, addr) = result?;
                log::info!("[{addr:?}] New connection");

                // Find a suitable device to forward this client to
                let device = {
                    let cfg = cfg.clone();
                    tokio::task::spawn_blocking(move || device_matches(&cfg).ok()).await.unwrap()
                };

                log::info!("[{:?}] New connection assiged to {}",
                    addr,
                    device.as_ref().map(|dev| dev.url.clone()).unwrap_or_else(|| "(no devices found)".to_string())
                );

                if let Some(hub) = device.and_then(|dev| dev.to_hub()) {
                    tokio::spawn(async move {
                        let result = forward(stream, Box::pin(hub)).await;

                        if let Err(e) = result {
                            log::info!("[{addr}] Connection closed: {e:?}");
                        }

                        log::info!("[{addr:?}] Closed");
                    });
                }
           },
        }
    }
}

fn device_matches(cfg: &config::TcpServer) -> Result<Arc<Device>> {
    let app = super::APP.get().unwrap();
    let app = app.blocking_read();
    let device = {
        let mut devices = app
            .device_manager
            .as_ref()
            .ok_or(MiniDSPError::TransportClosed)?
            .devices();

        if let Some(serial) = cfg.device_serial {
            devices.into_iter().find(|dev| {
                dev.device_info()
                    .map(|di| di.serial == serial)
                    .unwrap_or(false)
            })
        } else if let Some(device_index) = cfg.device_index {
            devices.into_iter().nth(device_index)
        } else {
            devices.sort_by_key(|dev| !dev.is_local());
            devices.into_iter().next()
        }
    };

    device.ok_or_else(|| anyhow::anyhow!("no matching devices found"))
}
