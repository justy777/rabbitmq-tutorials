use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let message = match args.len() {
        0 => "hello".to_string(),
        _ => args.join(" ").to_string(),
    };

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

    channel
        .basic_publish(
            "",
            "task_queue",
            BasicPublishOptions::default(),
            message.as_bytes(),
            BasicProperties::default(),
        )
        .await?;

    println!("[x] Sent {message:?}");

    connection.close(0, "").await?;

    Ok(())
}
