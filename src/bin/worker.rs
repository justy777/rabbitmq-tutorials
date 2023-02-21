use futures_lite::StreamExt;
use lapin::options::BasicQosOptions;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};
use std::{thread, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "amqp://localhost";
    let connection = Connection::connect(addr, ConnectionProperties::default()).await?;
    let channel = connection.create_channel().await?;

    channel
        .queue_declare(
            "task_queue",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    channel.basic_qos(1, BasicQosOptions::default()).await?;

    let mut consumer = channel
        .basic_consume(
            "task_queue",
            "consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    println!("[*] Waiting for messages. To exit press CTRL+C");

    while let Some(delivery) = consumer.next().await {
        if let Ok(delivery) = delivery {
            println!("[x] Received {:?}", std::str::from_utf8(&delivery.data)?);
            thread::sleep(Duration::from_secs(delivery.data.len() as u64));
            println!("[x] Done");
            delivery.ack(BasicAckOptions::default()).await?;
        }
    }

    Ok(())
}
