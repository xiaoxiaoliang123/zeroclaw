use super::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{StreamExt, SinkExt};
use std::time::{Duration, Instant};

/// WeCom (WeChat Enterprise) Bot channel with both Webhook and WebSocket support.
///
/// Sends messages via either WeCom Bot Webhook API or WebSocket. Incoming messages are received
/// through WebSocket for real-time communication.
pub struct WeComChannel {
    // Webhook configuration
    webhook_key: Option<String>,
    // WebSocket configuration
    bot_id: Option<String>,
    bot_secret: Option<String>,
    // Common configuration
    allowed_users: Vec<String>,
    // WebSocket state
    ws_client: Arc<Mutex<Option<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>>,
    ws_url: String,
    last_heartbeat: Arc<Mutex<Instant>>,
    reconnect_interval: Duration,
}

impl Clone for WeComChannel {
    fn clone(&self) -> Self {
        Self {
            webhook_key: self.webhook_key.clone(),
            bot_id: self.bot_id.clone(),
            bot_secret: self.bot_secret.clone(),
            allowed_users: self.allowed_users.clone(),
            ws_client: self.ws_client.clone(),
            ws_url: self.ws_url.clone(),
            last_heartbeat: self.last_heartbeat.clone(),
            reconnect_interval: self.reconnect_interval,
        }
    }
}

impl WeComChannel {
    pub fn new(
        webhook_key: Option<String>,
        bot_id: Option<String>,
        bot_secret: Option<String>,
        allowed_users: Vec<String>,
    ) -> Self {
        Self {
            webhook_key,
            bot_id,
            bot_secret,
            allowed_users,
            ws_client: Arc::new(Mutex::new(None)),
            ws_url: "wss://openws.work.weixin.qq.com".to_string(),
            last_heartbeat: Arc::new(Mutex::new(Instant::now())),
            reconnect_interval: Duration::from_secs(5),
        }
    }

    fn http_client(&self) -> reqwest::Client {
        crate::config::build_runtime_proxy_client("channel.wecom")
    }

    fn webhook_url(&self) -> Option<String> {
        self.webhook_key.as_ref().map(|key| {
            format!(
                "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={}",
                key
            )
        })
    }

    fn is_user_allowed(&self, user_id: &str) -> bool {
        self.allowed_users.iter().any(|u| u == "*" || u == user_id)
    }

    /// Check if WebSocket is configured
    fn is_websocket_configured(&self) -> bool {
        self.bot_id.is_some() && self.bot_secret.is_some()
    }

    /// Check if Webhook is configured
    fn is_webhook_configured(&self) -> bool {
        self.webhook_key.is_some()
    }

    /// Generate a request ID
    fn generate_req_id(&self, prefix: &str) -> String {
        format!("{}_{}", prefix, uuid::Uuid::new_v4())
    }

    /// Connect to WebSocket server
    async fn connect_ws(&self) -> anyhow::Result<()> {
        if !self.is_websocket_configured() {
            anyhow::bail!("WebSocket is not configured");
        }

        tracing::info!("WeCom: connecting to WebSocket server: {}", self.ws_url);
        let (ws_stream, response) = connect_async(&self.ws_url).await.map_err(|e| {
            tracing::error!("WeCom: failed to connect to WebSocket server: {}", e);
            e
        })?;
        
        tracing::info!("WeCom: WebSocket connected, response status: {}", response.status());
        *self.ws_client.lock().await = Some(ws_stream);
        
        // Send authentication
        tracing::info!("WeCom: sending authentication");
        self.send_auth().await.map_err(|e| {
            tracing::error!("WeCom: authentication failed: {}", e);
            e
        })?;
        
        tracing::info!("WeCom: WebSocket connection established and authenticated");
        Ok(())
    }

    /// Send authentication frame
    async fn send_auth(&self) -> anyhow::Result<()> {
        let bot_id = self.bot_id.as_ref().unwrap();
        let bot_secret = self.bot_secret.as_ref().unwrap();
        
        let auth_frame = serde_json::json!({
            "cmd": "aibot_subscribe",
            "headers": { "req_id": self.generate_req_id("subscribe") },
            "body": {
                "bot_id": bot_id,
                "secret": bot_secret
            }
        });
        
        self.send_ws_message(auth_frame).await
    }

    /// Send heartbeat
    async fn send_heartbeat(&self) -> anyhow::Result<()> {
        let heartbeat_frame = serde_json::json!({
            "cmd": "ping",
            "headers": { "req_id": self.generate_req_id("ping") }
        });
        
        self.send_ws_message(heartbeat_frame).await
    }

    /// Send WebSocket message
    async fn send_ws_message(&self, message: serde_json::Value) -> anyhow::Result<()> {
        let mut ws_client = self.ws_client.lock().await;
        if let Some(ref mut client) = *ws_client {
            let message_str = serde_json::to_string(&message)?;
            client.send(Message::Text(message_str.into())).await?;
            Ok(())
        } else {
            anyhow::bail!("WebSocket not connected");
        }
    }

    /// Handle WebSocket message
    async fn handle_ws_message(&self, message: Message, tx: &tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        match message {
            Message::Text(text) => {
                let frame: serde_json::Value = serde_json::from_str(&text)?;
                
                // Handle different message types
                if let Some(cmd) = frame.get("cmd").and_then(|v| v.as_str()) {
                    match cmd {
                        "aibot_msg_callback" => {
                            // Handle incoming message
                            if let Some(body) = frame.get("body") {
                                // Extract message content based on msgtype
                                let content_opt = if let Some(text) = body.get("text") {
                                    Some(text.get("content").and_then(|v| v.as_str()).unwrap_or("").to_string())
                                } else if let Some(image) = body.get("image") {
                                    Some(format!("[pic] {}", image.get("url").and_then(|v| v.as_str()).unwrap_or("")))
                                } else if let Some(voice) = body.get("voice") {
                                    Some(format!("[voice] {}", voice.get("content").and_then(|v| v.as_str()).unwrap_or("")))
                                } else if let Some(file) = body.get("file") {
                                    Some(format!("[file] {}", file.get("url").and_then(|v| v.as_str()).unwrap_or("")))
                                } else {
                                    tracing::debug!("Unknown message type: {:?}", body);
                                    None
                                };
                                
                                if let Some(content) = content_opt {
                                    let sender = body.get("from")
                                        .and_then(|f| f.get("userid"))
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string();
                                    
                                    let channel_message = ChannelMessage {
                                        id: body.get("msgid").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                        content,
                                        sender: sender.clone(),
                                        reply_target: sender,
                                        channel: "wecom".to_string(),
                                        timestamp: body.get("create_time")
                                            .and_then(|v| v.as_u64())
                                            .unwrap_or_else(|| std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_secs()),
                                        thread_ts: body.get("chatid").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                        interruption_scope_id: None,
                                    };
                                    
                                    tracing::info!("Received WeCom message from {}: {}", channel_message.sender, channel_message.content);
                                    tx.send(channel_message).await?;
                                }
                            }
                        }
                        "aibot_event_callback" => {
                            // Handle event
                            tracing::info!("Received WeCom event: {:?}", frame);
                        }
                        _ => {
                            tracing::debug!("Received unknown WebSocket message: {:?}", frame);
                        }
                    }
                } else if frame.get("errcode").is_some() {
                    // Handle response (authentication, heartbeat)
                    let errcode = frame.get("errcode").and_then(|v| v.as_i64()).unwrap_or(-1);
                    if errcode != 0 {
                        let errmsg = frame.get("errmsg").and_then(|v| v.as_str()).unwrap_or("unknown error");
                        tracing::error!("WeCom WebSocket error: errcode={}, errmsg={}", errcode, errmsg);
                    }
                }
            }
            Message::Ping(_) => {
                // Send pong
                let mut ws_client = self.ws_client.lock().await;
                if let Some(ref mut client) = *ws_client {
                    client.send(Message::Pong(vec![].into())).await?;
                }
            }
            Message::Pong(_) => {
                // Update last heartbeat time
                *self.last_heartbeat.lock().await = Instant::now();
            }
            Message::Close(_) => {
                tracing::info!("WebSocket connection closed");
            }
            _ => {
                tracing::debug!("Received non-text WebSocket message");
            }
        }
        
        Ok(())
    }

    /// Reconnect WebSocket
    async fn reconnect_ws(&self) -> anyhow::Result<()> {
        tracing::info!("Reconnecting WebSocket...");
        tokio::time::sleep(self.reconnect_interval).await;
        self.connect_ws().await
    }
}

#[async_trait]
impl Channel for WeComChannel {
    fn name(&self) -> &str {
        "wecom"
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        // Try WebSocket first if configured
        if self.is_websocket_configured() {
            match self.send_via_websocket(message).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    tracing::warn!("WebSocket send failed, falling back to webhook: {}", e);
                    // Fall back to webhook if WebSocket fails
                }
            }
        }

        // Use webhook if WebSocket is not configured or failed
        if let Some(webhook_url) = self.webhook_url() {
            let body = serde_json::json!({
                "msgtype": "text",
                "text": {
                    "content": message.content,
                }
            });

            let resp = self
                .http_client()
                .post(&webhook_url)
                .json(&body)
                .send()
                .await?;

            if !resp.status().is_success() {
                let status = resp.status();
                let err = resp.text().await.unwrap_or_default();
                anyhow::bail!("WeCom webhook send failed ({status}): {err}");
            }

            // WeCom returns {"errcode":0,"errmsg":"ok"} on success.
            let result: serde_json::Value = resp.json().await?;
            let errcode = result.get("errcode").and_then(|v| v.as_i64()).unwrap_or(-1);
            if errcode != 0 {
                let errmsg = result
                    .get("errmsg")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown error");
                anyhow::bail!("WeCom API error (errcode={errcode}): {errmsg}");
            }

            Ok(())
        } else {
            anyhow::bail!("Neither WebSocket nor Webhook is configured");
        }
    }

    async fn listen(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        if self.is_websocket_configured() {
            // Try to start WebSocket listener
            match self.start_websocket_listener(tx.clone()).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    tracing::error!("Failed to start WebSocket listener: {}", e);
                    // Fall back to webhook mode (send-only)
                    tracing::info!("WeCom: channel ready (send-only via Bot Webhook)");
                    tx.closed().await;
                    Ok(())
                }
            }
        } else {
            // Fall back to webhook mode (send-only)
            tracing::info!("WeCom: channel ready (send-only via Bot Webhook)");
            tx.closed().await;
            Ok(())
        }
    }

    async fn health_check(&self) -> bool {
        // Check WebSocket if configured
        if self.is_websocket_configured() {
            match self.check_websocket_health().await {
                Ok(true) => return true,
                _ => {
                    tracing::warn!("WebSocket health check failed, falling back to webhook");
                }
            }
        }

        // Check Webhook if configured
        if let Some(webhook_url) = self.webhook_url() {
            let resp = self
                .http_client()
                .post(&webhook_url)
                .json(&serde_json::json!({
                    "msgtype": "text",
                    "text": {
                        "content": "health_check"
                    }
                }))
                .send()
                .await;

            match resp {
                Ok(r) => r.status().is_success(),
                Err(_) => false,
            }
        } else {
            false
        }
    }
}

impl WeComChannel {
    /// Send message via WebSocket
    async fn send_via_websocket(&self, message: &SendMessage) -> anyhow::Result<()> {
        let reply_frame = serde_json::json!({
            "cmd": "aibot_respond_msg",
            "headers": { "req_id": self.generate_req_id("response") },
            "body": {
                "msgtype": "text",
                "text": {
                    "content": message.content
                }
            }
        });
        
        self.send_ws_message(reply_frame).await
    }

    /// Start WebSocket listener
    async fn start_websocket_listener(&self, tx: tokio::sync::mpsc::Sender<ChannelMessage>) -> anyhow::Result<()> {
        tracing::info!("WeCom: starting WebSocket listener");
        
        // Connect to WebSocket
        self.connect_ws().await?;
        
        // Start heartbeat task
        let self_clone = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                if let Err(e) = self_clone.send_heartbeat().await {
                    tracing::warn!("Failed to send heartbeat: {}", e);
                }
            }
        });
        
        // Main message loop
        let self_clone = self.clone();
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            loop {
                let mut ws_client = self_clone.ws_client.lock().await;
                match ws_client.as_mut() {
                    Some(ws) => {
                        match ws.next().await {
                            Some(Ok(message)) => {
                                if let Err(e) = self_clone.handle_ws_message(message, &tx_clone).await {
                                    tracing::warn!("Error handling WebSocket message: {}", e);
                                }
                            }
                            Some(Err(e)) => {
                                tracing::error!("WebSocket error: {}", e);
                                // Reconnect
                                if let Err(e) = self_clone.reconnect_ws().await {
                                    tracing::error!("Failed to reconnect: {}", e);
                                }
                            }
                            None => {
                                tracing::info!("WebSocket connection closed");
                                // Reconnect
                                if let Err(e) = self_clone.reconnect_ws().await {
                                    tracing::error!("Failed to reconnect: {}", e);
                                }
                            }
                        }
                    }
                    None => {
                        // Reconnect
                        if let Err(e) = self_clone.reconnect_ws().await {
                            tracing::error!("Failed to reconnect: {}", e);
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });
        
        // Keep the listener alive
        tx.closed().await;
        Ok(())
    }

    /// Check WebSocket health
    async fn check_websocket_health(&self) -> anyhow::Result<bool> {
        if let Some(_) = *self.ws_client.lock().await {
            // Try to send a heartbeat
            match self.send_heartbeat().await {
                Ok(_) => Ok(true),
                Err(e) => {
                    tracing::warn!("WebSocket health check failed: {}", e);
                    Ok(false)
                }
            }
        } else {
            // Try to connect
            match self.connect_ws().await {
                Ok(_) => Ok(true),
                Err(e) => {
                    tracing::warn!("WebSocket connection failed: {}", e);
                    Ok(false)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_name() {
        let ch = WeComChannel::new(Some("test-key".into()), None, None, vec![]);
        assert_eq!(ch.name(), "wecom");
    }

    #[test]
    fn test_webhook_url() {
        let ch = WeComChannel::new(Some("abc-123".into()), None, None, vec![]);
        assert_eq!(
            ch.webhook_url(),
            Some("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abc-123".into())
        );
    }

    #[test]
    fn test_user_allowed_wildcard() {
        let ch = WeComChannel::new(Some("key".into()), None, None, vec!["*".into()]);
        assert!(ch.is_user_allowed("anyone"));
    }

    #[test]
    fn test_user_allowed_specific() {
        let ch = WeComChannel::new(Some("key".into()), None, None, vec!["user123".into()]);
        assert!(ch.is_user_allowed("user123"));
        assert!(!ch.is_user_allowed("other"));
    }

    #[test]
    fn test_user_denied_empty() {
        let ch = WeComChannel::new(Some("key".into()), None, None, vec![]);
        assert!(!ch.is_user_allowed("anyone"));
    }

    #[test]
    fn test_is_websocket_configured() {
        let ch1 = WeComChannel::new(None, None, None, vec![]);
        assert!(!ch1.is_websocket_configured());

        let ch2 = WeComChannel::new(None, Some("bot-id".into()), None, vec![]);
        assert!(!ch2.is_websocket_configured());

        let ch3 = WeComChannel::new(None, None, Some("bot-secret".into()), vec![]);
        assert!(!ch3.is_websocket_configured());

        let ch4 = WeComChannel::new(None, Some("bot-id".into()), Some("bot-secret".into()), vec![]);
        assert!(ch4.is_websocket_configured());
    }

    #[test]
    fn test_is_webhook_configured() {
        let ch1 = WeComChannel::new(None, None, None, vec![]);
        assert!(!ch1.is_webhook_configured());

        let ch2 = WeComChannel::new(Some("webhook-key".into()), None, None, vec![]);
        assert!(ch2.is_webhook_configured());
    }

    #[test]
    fn test_config_serde() {
        let toml_str = r#"
webhook_key = "key-abc-123"
bot_id = "bot-123"
bot_secret = "secret-456"
allowed_users = ["user1", "*"]
"#;
        let config: crate::config::schema::WeComConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.webhook_key, Some("key-abc-123".into()));
        assert_eq!(config.bot_id, Some("bot-123".into()));
        assert_eq!(config.bot_secret, Some("secret-456".into()));
        assert_eq!(config.allowed_users, vec!["user1", "*"]);
    }

    #[test]
    fn test_config_serde_defaults() {
        let toml_str = r#"
webhook_key = "key"
"#;
        let config: crate::config::schema::WeComConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.webhook_key, Some("key".into()));
        assert_eq!(config.bot_id, None);
        assert_eq!(config.bot_secret, None);
        assert!(config.allowed_users.is_empty());
    }

    #[test]
    fn test_config_serde_websocket_only() {
        let toml_str = r#"
bot_id = "bot-123"
bot_secret = "secret-456"
allowed_users = ["user1"]
"#;
        let config: crate::config::schema::WeComConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.webhook_key, None);
        assert_eq!(config.bot_id, Some("bot-123".into()));
        assert_eq!(config.bot_secret, Some("secret-456".into()));
        assert_eq!(config.allowed_users, vec!["user1"]);
    }
}
