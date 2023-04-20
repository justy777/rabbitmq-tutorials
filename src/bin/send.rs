use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
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

    let payload = b"Hello World";

    channel
        .basic_publish(
            "",
            "test",
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default(),
        )
        .await?;

    println!("[x] Sent message");

    connection.close(0, "").await?;

    Ok(())
}
