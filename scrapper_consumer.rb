require "kafka"

kafka_servers = ( ENV['KAFKA_HOSTS'] || "localhost:9092" ).split(/;/)
kafka_group_id = ENV['KAFKA_GROUP_ID'] || 'scraper-consumer-group'

kafka = Kafka.new(kafka_servers)

# Consumers with the same group id will form a Consumer Group together.
consumer = kafka.consumer(group_id: kafka_group_id)

# It's possible to subscribe to multiple topics by calling `subscribe`
# repeatedly.
consumer.subscribe("greetings")

# Stop the consumer when the SIGTERM signal is sent to the process.
# It's better to shut down gracefully than to kill the process.
trap("TERM") { consumer.stop }

# This will loop indefinitely, yielding each message in turn.
consumer.each_message do |message|
  puts message.topic, message.partition
  puts message.offset, message.key, message.value
end

