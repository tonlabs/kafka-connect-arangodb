use rand::RngCore;
extern crate base64;

mod kafka_producer;
use kafka_producer::KafkaProducer;

pub(crate) fn init_logger() {
    let level = log::LevelFilter::Info;
    let stdout = log4rs::append::console::ConsoleAppender::builder()
        .target(log4rs::append::console::Target::Stdout)
        .build();

    let config = log4rs::config::Config::builder()
        .appender(
            log4rs::config::Appender::builder()
                .filter(Box::new(log4rs::filter::threshold::ThresholdFilter::new(level)))
                .build("stdout", Box::new(stdout)),
        )
        .build(
            log4rs::config::Root::builder()
                .appender("stdout")
                .build(log::LevelFilter::Trace),
        )
        .unwrap();

    let result = log4rs::init_config(config);
    if let Err(e) = result {
        println!("Error init log: {}", e);
    }
}

fn create_producer(config_path: &str) -> Result<KafkaProducer, String> {
    let file = std::fs::File::open(config_path)
        .map_err(|e| format!("Error while opening config file: {}", e))?;
    let config = serde_json::from_reader(file)
        .map_err(|e| format!("Error while parsing config file: {}", e))?;
    
    KafkaProducer::new(config)
        .map_err(|e| format!("Error while initializing kafka messages producer: {}", e))
}

async fn send_blocks(producer: KafkaProducer, block: &[u8], rate: u32) -> Result<(), String> {
    let mut rng = rand::thread_rng();
    let mut id: Vec<u8> = Vec::new();
    id.resize(32, 0);
    let now_all = std::time::Instant::now();
    let mut blocks_sent_all = 0;
    loop {
        let now_sec = std::time::Instant::now();
        let mut blocks_sent_sec = 0;
        while now_sec.elapsed().as_millis() < 1000 && blocks_sent_sec < rate {
            rng.fill_bytes(&mut id);
            producer.write_data(&id, block).await?;
            blocks_sent_all += 1;
            blocks_sent_sec += 1;
        }
        let delay = 1000 - now_sec.elapsed().as_millis() as i64;
        if delay > 0 {
            //println!("delay {}", delay);
            futures_timer::Delay::new(
                std::time::Duration::from_millis(delay as u64)
            ).await;
        }

        println!(
            "Blocks per second {}, average {}",
            blocks_sent_sec,
            (blocks_sent_all * 1000) as f64 / now_all.elapsed().as_millis() as f64);
        println!("Blocks sent {}", blocks_sent_all);
    }
}

async fn send_folder(producer: KafkaProducer, path: &str) -> Result<(), String> {
    let mut blocks_sent = 0;
    let mut dirs = vec![path.into()];
    let mut processed_dirs = 0;
    while dirs.len() > processed_dirs {
        let dir = std::fs::read_dir(&dirs[processed_dirs])
            .map_err(|e| format!("Can not open folder: {}", e))?;
        for entry in dir {
            let entry = entry
                .map_err(|e| format!("Can not use folder entry: {}", e))?;
            let path = entry.path();
            if path.is_file() {
                let block = std::fs::read(&path)
                    .map_err(|e| format!("Can not read file {:#?}: {}", path, e))?;
                let file_name = path.file_name().unwrap().to_str().unwrap().split(".").next().unwrap();
                let id = hex::decode(file_name)
                    .map_err(|e| format!("Can not decode block ID {}: {}", file_name, e))?;
                println!("Sending {}", file_name);
                producer.write_data(&id, &block).await?;
                blocks_sent += 1;
            } else {
                dirs.push(path);
            }
        }
        processed_dirs += 1;
    }
    println!("Blocks sent {}", blocks_sent);
    Ok(())
}

#[tokio::main]
async fn main() {

    let app = clap::App::new("TON blockchain parsing service")
        //.version("1.0")
        .arg(clap::Arg::with_name("config")
            .short("c")
            .long("config")
            .help("specifies service's config JSON")
            .value_name("config")
            .default_value("config.json"))
        .arg(clap::Arg::with_name("block file")
            .short("b")
            .long("block")
            .help("specifies block file to send")
            .value_name("block file")
            .default_value("block.boc"))
        .arg(clap::Arg::with_name("rate")
            .short("r")
            .long("rate")
            .help("specifies blocks sending rate (blocks per second)")
            .value_name("rate")
            .default_value("1"))
        .arg(clap::Arg::with_name("folder")
            .short("f")
            .long("folder")
            .help("specifies folder with blocks to send")
            .value_name("folder"));

    init_logger();

    let matches = app.get_matches();

    let config = matches.value_of("config").unwrap();
    println!("Using config from {}", config);
    let producer = match create_producer(config) {
        Ok(c) => c,
        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    if let Some(folder) = matches.value_of("folder") {
        println!("Sending blocks from {}", folder);
        if let Err(err) = send_folder(producer, folder).await {
            log::error!("{}", err);
        }
        return;
    }

    let block_path = matches.value_of("block file").unwrap();
    println!("Using block from {}", block_path);
    let block = match std::fs::read(block_path) {
        Ok(c) => c,
        Err(e) => {
            println!("Cannot read block: {}", e);
            return;
        }
    };

    let rate = match u32::from_str_radix(matches.value_of("rate").unwrap(), 10) {
        Ok(c) => c,
        Err(e) => {
            println!("Cannot parse rate: {}", e);
            return;
        }
    };
    if rate == 0 {
        println!("Sending block once");
        println!("Using filename as bas64 encoded block id");
        // Send once
        let id:Vec<u8> = base64::decode(block_path).unwrap();
        if let Err(err) = producer.write_data(&id, &block).await {
            log::error!("{}", err);
        }
    } else {
        println!("Using rate {} blocks per second", rate);
        if let Err(err) = send_blocks(producer, &block, rate).await {
            log::error!("{}", err);
        }
    }
}
