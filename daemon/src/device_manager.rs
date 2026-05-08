//! Device Manager: Reacts to discovery events, probe devices and make them ready for use by other components
use std::{
    collections::HashSet,
    net::IpAddr,
    sync::{Arc, RwLock, Weak},
};

use anyhow::{anyhow, Result};
use futures::{StreamExt, TryFutureExt};
use minidsp::{
    client::Client,
    device::{self, probe},
    logging,
    transport::{self, SharedService},
    utils::OwnedJoinHandle,
    DeviceInfo, MiniDSP, MiniDSPError,
};
use tokio::sync::Mutex;
use url2::Url2;

use super::discovery::{DiscoveryEvent, Registry};

pub struct DeviceManager {
    #[allow(dead_code)]
    inner: Arc<RwLock<DeviceManagerInner>>,
    #[allow(dead_code)]
    handles: Vec<OwnedJoinHandle<Result<(), anyhow::Error>>>,
}

impl DeviceManager {
    pub fn new(
        registry: Registry,
        ignore_net_ip: HashSet<IpAddr>,
        ignore_advertisements: bool,
    ) -> Self {
        let inner = DeviceManagerInner {
            registry,
            ..Default::default()
        };

        let inner = Arc::new(std::sync::RwLock::new(inner));
        let mut handles = Vec::new();

        {
            // Start tasks for discovery processes
            let discovery_hid = {
                let inner = inner.clone();
                tokio::spawn(
                    super::discovery::tasks::hid_discovery_task(move |dev| {
                        let inner = inner.read().unwrap();
                        inner.registry.register(dev, false);
                    })
                    .err_into(),
                )
                .into()
            };
            handles.push(discovery_hid);

            if !ignore_advertisements {
                let discovery_net = {
                    let inner = inner.clone();
                    tokio::spawn(super::discovery::tasks::net_discovery_task(
                        move |dev| {
                            let inner = inner.read().unwrap();
                            inner.registry.register(dev, false);
                        },
                        ignore_net_ip,
                    ))
                    .into()
                };
                handles.push(discovery_net);
            }

            let task = {
                let inner = inner.clone();
                tokio::spawn(async move {
                    DeviceManager::task(inner).await;
                    Ok(())
                })
                .into()
            };
            handles.push(task);
        }

        DeviceManager { inner, handles }
    }

    pub fn get_device(&self, index: usize) -> Option<Arc<Device>> {
        let devices = self.devices();
        let serial_match = devices.iter().find(|d| match d.device_info() {
            Some(device_info) => device_info.serial == index as u32,
            None => false,
        });
        if let Some(device) = serial_match {
            return Some(device.clone());
        }
        if index >= devices.len() {
            return None;
        }
        Some(devices[index].clone())
    }

    pub async fn get_minidsp(&self, index: usize) -> Option<MiniDSP<'static>> {
        let mut attempts = 0;
        let mut device = self.get_device(index)?;
        loop {
            attempts += 1;
            let err = match device.to_minidsp() {
                Ok(minidsp) => {
                    let status = minidsp.get_master_status().await;
                    if let Some(err) = status.err() {
                        err
                    } else {
                        return Some(minidsp);
                    }
                }
                Err(err) => err,
            };
            log::warn!(
                "failed to connect to device {} (attempt {}): {}",
                device.url,
                attempts,
                err
            );
            if attempts > 3 || !err.is_retryable() {
                log::warn!("giving up attempting to connect.");
                return None;
            }
            if err.should_reconnect() {
                device = self.reconnect_device(device).await;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    pub async fn reconnect_device(&self, device: Arc<Device>) -> Arc<Device> {
        let url = device.url.clone();
        {
            let mut inner = self.inner.write().unwrap();
            inner.devices.retain(|dev| dev.url != url);
        }

        device.shutdown().await;

        let mut inner = self.inner.write().unwrap();
        let weak_inner = Arc::downgrade(&self.inner);

        let new_device: Arc<Device> = Device::new(url, weak_inner).into();
        inner.devices.push(new_device.clone());
        new_device
    }

    pub fn register_static(&self, dev: &str) {
        let inner = self.inner.write().unwrap();
        inner.registry.register(dev, true);
    }

    pub fn devices(&self) -> Vec<Arc<Device>> {
        let inner = self.inner.read().unwrap();
        inner.devices.clone()
    }

    async fn task(inner: Arc<RwLock<DeviceManagerInner>>) {
        let mut discovery_events = {
            let inner = inner.read().unwrap();
            inner.registry.subscribe()
        };

        loop {
            while let Some(event) = discovery_events.next().await {
                log::trace!("{:?}", &event);

                let weak_inner = Arc::downgrade(&inner);
                let mut inner = inner.write().unwrap();
                match event {
                    DiscoveryEvent::Added(id) => {
                        inner.devices.push(Device::new(id, weak_inner).into());
                    }
                    DiscoveryEvent::Timeout { id, last_seen } => {
                        log::info!(
                            "Device hasn't been seen since timeout period: {id} (last seen at {last_seen:?})"
                        );

                        // Remove that device from the list
                        inner.devices.retain(|d| !d.url.eq(id.as_str()));
                    }
                }
            }
        }
    }
}

#[derive(Default)]
pub struct DeviceManagerInner {
    registry: Registry,
    devices: Vec<Arc<Device>>,
}

impl DeviceManagerInner {
    pub fn remove(&mut self, url: &str) {
        self.devices.retain(|dev| dev.url != url);
        self.registry.remove(url);
    }
}

pub struct Device {
    pub url: String,
    #[allow(dead_code)]
    inner: Arc<RwLock<DeviceInner>>,
    #[allow(dead_code)]
    handles: Mutex<Vec<OwnedJoinHandle<Result<(), anyhow::Error>>>>,
}

impl Device {
    pub fn new(url: String, device_manager: Weak<RwLock<DeviceManagerInner>>) -> Self {
        let inner = Arc::new(std::sync::RwLock::new(DeviceInner {
            url: url.clone(),
            device_manager,
            ..Default::default()
        }));

        let mut handles = Vec::new();
        {
            let inner = inner.clone();
            let handle = tokio::spawn(async move { Device::task(inner).await });
            handles.push(handle.into());
        }

        Device {
            url,
            inner,
            handles: Mutex::new(handles),
        }
    }

    pub async fn shutdown(&self) {
        // Cap each blocking step so a wedged hidapi call can't hang the whole daemon.
        // The OwnedJoinHandle::abort() above signals the OS-level recv thread; if hid_read
        // is mid-call it will eventually drop, but we don't wait around for it.
        const SHUTDOWN_STEP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
        {
            let mut handles = self.handles.lock().await;

            for handle in handles.iter() {
                handle.abort();
            }
            for handle in handles.drain(..) {
                match tokio::time::timeout(SHUTDOWN_STEP_TIMEOUT, handle).await {
                    Ok(Err(e)) if !e.is_cancelled() => {
                        log::error!("device inner task ended with error: {e}");
                    }
                    Err(_) => {
                        log::warn!(
                            "device inner task did not finish within shutdown timeout; abandoning"
                        );
                    }
                    _ => {}
                }
            }

            let handle = { self.inner.write().unwrap().handle.take() };
            match handle {
                Some(handle) => {
                    if tokio::time::timeout(SHUTDOWN_STEP_TIMEOUT, handle.transport.shutdown())
                        .await
                        .is_err()
                    {
                        log::warn!("transport shutdown timed out; abandoning");
                    }
                    let mplex = handle.service.lock().await;
                    if tokio::time::timeout(SHUTDOWN_STEP_TIMEOUT, mplex.shutdown())
                        .await
                        .is_err()
                    {
                        log::warn!("multiplexer shutdown timed out; abandoning");
                    }
                }
                None => {
                    log::warn!("shutting down device, but transport was already closed");
                }
            }
        }
    }

    pub fn is_local(&self) -> bool {
        self.url.starts_with("usb:")
    }

    pub fn to_hub(&self) -> Option<transport::Hub> {
        let inner = self.inner.read().unwrap();
        inner.handle.as_ref()?.to_hub()
    }

    pub fn to_minidsp(&self) -> Result<MiniDSP<'static>, MiniDSPError> {
        let inner = self.inner.read().unwrap();
        let result = inner
            .handle
            .as_ref()
            .ok_or(MiniDSPError::DeviceNotReady)?
            .to_minidsp();
        result
    }

    pub fn device_info(&self) -> Option<DeviceInfo> {
        let inner = self.inner.read().unwrap();
        inner.handle.as_ref()?.device_info
    }

    pub fn device_spec(&self) -> Option<&'static minidsp::device::Device> {
        let inner = self.inner.read().unwrap();
        inner.handle.as_ref()?.device_spec
    }

    async fn task_inner(inner: Arc<RwLock<DeviceInner>>) -> anyhow::Result<()> {
        let url = {
            let inner = inner.read().unwrap();
            inner.url.clone()
        };

        log::info!("Connecting to {}", url.as_str());

        // Connect to the device by url, and get a frame-level transport
        let (mut transportlocal, decoder) = {
            let url = Url2::try_parse(url.as_str()).expect("Device::run had invalid url");
            let stream = transport::open_url(&url).await?;

            // If we have any logging options, log this stream
            let app = super::APP.get().unwrap();
            let app = app.read().await;
            let (decoder, stream) =
                logging::transport_logging(stream, app.opts.verbose, app.opts.log.clone());
            (transport::Hub::new(stream), decoder)
        };
        // Wrap the transport into a multiplexed service for command-level multiplexing
        let service = {
            let transport = transportlocal
                .try_clone()
                .ok_or_else(|| anyhow!("transport closed prematurely"))?;
            let mplex = transport::Multiplexer::from_transport(transport);
            Arc::new(Mutex::new(mplex.to_service()))
        };

        // Probe the device hardware id and dsp version in order to get the right specs
        // Keep going if we do not know the device type, but it has successfully responsed to
        // probing commands. This can be used to support a common subset of features without
        // knowing the device-specific memory layout.
        let device_info = {
            let client = Client::new(service.clone());
            client.get_device_info().await.ok()
        };
        let device_spec = device_info.map(|dev| device::probe(&dev));

        let devhandle = DeviceHandle {
            service,
            transport: transportlocal
                .try_clone()
                .ok_or_else(|| anyhow!("transport closed prematurely"))?,
            device_spec,
            device_info,
        };

        log::info!(
            "Identified {} as {} (serial# {})",
            &url,
            device_spec
                .map(|spec| spec.product_name)
                .unwrap_or("(unknown device)"),
            device_info
                .map(|di| format!("{}", di.serial))
                .unwrap_or_else(|| "unknown".to_string())
        );

        if let (Some(decoder), Some(device_spec)) = (decoder, device_spec) {
            let mut decoder = decoder.lock().await;
            decoder.set_name_map(device_spec.symbols.iter().copied());
        }

        // Subscribe to multiplexer events before publishing the handle. We don't care about
        // the events themselves; we only need to detect when the recv loop dies (broadcast
        // Sender dropped → recv() returns RecvError::Closed). This is the signal that the
        // device's command path is permanently broken — without it we'd sit in the
        // transport-EOF loop below indefinitely while every command times out.
        let mplex_events = {
            let svc = devhandle.service.lock().await;
            svc.subscribe().ok()
        };

        {
            let mut inner = inner.write().unwrap();
            inner.handle.replace(devhandle);
        }

        // Race transport-level liveness against multiplexer-level liveness; whichever fires
        // first triggers the removal path so discovery can re-add the device cleanly.
        let transport_done = async move {
            while let Some(frame) = transportlocal.next().await {
                if let Err(e) = frame {
                    return format!("transport error: {e}");
                }
            }
            "transport EOF".to_string()
        };
        let mplex_done = async move {
            use tokio::sync::broadcast::error::RecvError;
            match mplex_events {
                Some(mut rx) => loop {
                    match rx.recv().await {
                        Err(RecvError::Closed) => {
                            return "multiplexer recv loop exited".to_string();
                        }
                        // Lagged just means we missed unrelated events; keep watching.
                        Err(RecvError::Lagged(_)) | Ok(_) => continue,
                    }
                },
                // subscribe() returned Err — recv loop had already died by the time we
                // got here. Exit immediately so the device is removed and re-added.
                None => "multiplexer recv loop already dead at startup".to_string(),
            }
        };

        let exit_reason = tokio::select! {
            r = transport_done => r,
            r = mplex_done => r,
        };

        log::warn!("Device at {} is closing ({})", &url, exit_reason);

        // Notify the device manager that this device is to be removed. This clears both
        // inner.devices and inner.registry, so the next discovery tick will re-fire
        // DiscoveryEvent::Added and create a fresh Device with a fresh transport.
        if let Some(device_manager) = inner.read().unwrap().device_manager.upgrade() {
            let mut device_manager = device_manager.write().unwrap();
            device_manager.remove(&url);
        }

        Ok(())
    }

    /// Main device task
    /// This is spawned when the device is first discovered and manages it's complete lifecycle.
    async fn task(inner: Arc<RwLock<DeviceInner>>) -> anyhow::Result<()> {
        loop {
            // Try to probe the device until we're successful
            let res = Self::task_inner(inner.clone()).await;
            match res {
                Ok(_) => return Ok(()),
                Err(e) => {
                    log::warn!("fail to connect: {e}");
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}
#[derive(Default)]
pub struct DeviceInner {
    url: String,
    handle: Option<DeviceHandle>,

    device_manager: Weak<RwLock<DeviceManagerInner>>,
}

pub struct DeviceHandle {
    // A pre-configured multiplexer ready to be bound to a `Client`
    pub service: SharedService,

    // Frame-level multiplexer
    pub transport: transport::Hub,

    // Probed hardware id and dsp version
    pub device_info: Option<DeviceInfo>,

    // Device spec structure indicating the address of every component
    pub device_spec: Option<&'static minidsp::device::Device>,
}

impl DeviceHandle {
    pub fn to_minidsp(&self) -> Result<MiniDSP<'static>, MiniDSPError> {
        let client = Client::new(self.service.clone());

        let device_info = match self.device_info {
            Some(x) => x,
            None => futures::executor::block_on(client.get_device_info())?,
        };

        let spec = self.device_spec.unwrap_or_else(|| probe(&device_info));
        let dsp = MiniDSP::from_client(client, spec, device_info);
        Ok(dsp)
    }

    pub fn to_hub(&self) -> Option<transport::Hub> {
        self.transport.try_clone()
    }
}
