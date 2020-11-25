
use std::time;

#[derive(Debug, serde::Deserialize)]
pub struct KafkaProducerConfig {
    brokers: String,
    message_timeout_ms: u32,
    topic: String,
    attempt_timeout_ms: u32,
}

pub(super) struct KafkaProducer {
    config: KafkaProducerConfig,
    producer: rdkafka::producer::FutureProducer,
}

impl KafkaProducer {
    pub fn new(config: KafkaProducerConfig) -> Result<Self, String> {
        log::trace!("Creating kafka producer (topic: {})...", config.topic);
        let producer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", &config.message_timeout_ms.to_string())
            .create()
            .map_err(|err| err.to_string())?;

        Ok(Self { config, producer } )
    }
}

impl KafkaProducer {
    pub async fn write_data(&self, key: &[u8], data: &[u8]) -> Result<(), String> {
        let str_key = hex::encode(key);
        loop {
            log::trace!("Producing record, topic: {}, key: {}, size: {}", self.config.topic, str_key, data.len());
            let now = std::time::Instant::now();
            let produce_future = self.producer.send(
                rdkafka::producer::FutureRecord::to(&self.config.topic)
                    .key(key)
                    .payload(data),
                0,
            );
            match produce_future.await {
                Ok(Ok(_)) => {
                    log::trace!("Produced record, topic: {}, key: {}, time: {} mcs", self.config.topic, str_key, now.elapsed().as_micros());
                    break;
                },
                Ok(Err((e, _))) => log::warn!("Error while producing into kafka, topic: {}, key: {}, error: {}", self.config.topic, str_key, e),
                Err(e) => log::warn!("Internal error while producing into kafka, topic: {}, key: {}, error: {}", self.config.topic, str_key, e),
            }
            futures_timer::Delay::new(
                time::Duration::from_millis(
                    self.config.attempt_timeout_ms as u64
                )
            ).await;
        }
        Ok(())
    }
}