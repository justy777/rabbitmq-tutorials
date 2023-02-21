use futures_lite::StreamExt;
use lapin::{
    options::{BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties, ExchangeKind,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let severities: Vec<_> = std::env::args().skip(1).collect();
    if severities.is_empty() {
        eprintln!(
            "Usage: {} [info] [warning] [error]\n",
            std::env::args()
                .next()
                .unwrap_or_else(|| "receive-direct".into())
        );
        std::process::exit(1);
    }

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

    let queue = channel
        .queue_declare(
            "",
            QueueDeclareOptions {
                exclusive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    for severity in &severities {
        channel
            .queue_bind(
                queue.name().as_str(),
                "direct_logs",
                severity,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await?;
    }

    let mut consumer = channel
        .basic_consume(
            queue.name().as_str(),
            "consumer",
            BasicConsumeOptions {
                no_ack: false,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;

    println!("[*] Waiting for logs. To Exit press CTRL+C");

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            println!(
                "[x] {}:{:?}",
                delivery.routing_key,
                std::str::from_utf8(&delivery.data)?
            );
        }
    }

    Ok(())
}
