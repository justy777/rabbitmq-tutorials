use futures_lite::stream::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "amqp://localhost";
    let connection = Connection::connect(addr, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;

    channel
        .queue_declare(
            "test",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            "test",
            "consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!("[*] Waiting for messages. To exit press CTRL+C");

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            println!("[x] Received {:?}", std::str::from_utf8(&delivery.data)?);
            delivery.ack(BasicAckOptions::default()).await?;
        }
    }

    Ok(())
}
