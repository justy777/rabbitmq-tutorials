use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let severity = args.first().map_or("info", String::as_str);
    let message = match args.len() {
        x if x < 2 => "Hello, World!".to_string(),
        _ => args[1..].join(" ").to_string(),
    };

    let addr = "amqp://localhost";
    let connection = Connection::connect(addr, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;

    channel
        .exchange_declare(
            "direct_logs",
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    channel
        .basic_publish(
            "direct_logs",
            severity,
            BasicPublishOptions::default(),
            message.as_bytes(),
            BasicProperties::default(),
        )
        .await?;

    println!("[x] Sent {severity}:{message:?}");

    Ok(())
}
