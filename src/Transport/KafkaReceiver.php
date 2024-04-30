<?php

namespace Ilbee\Symfony\Messenger\Bridge\Kafka\Transport;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Receiver\ListableReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class KafkaReceiver implements ListableReceiverInterface
{
    private Connection $connection;
    private SerializerInterface $serializer;

    public function __construct(Connection $connection, SerializerInterface $serializer) {
        $this->connection = $connection;
        $this->serializer = $serializer;
    }
    
    public function all(?int $limit = null): iterable
    {
        dd('all', $limit);
    }

    public function find(mixed $id): ?Envelope
    {
        dd('find', $id);
    }

    public function get(): iterable
    {
        $consumer = $this->connection->createConsumer();
        $message = $consumer->consume(5000);

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $message->err) {
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR__PARTITION_EOF: // No more messages
                case RD_KAFKA_RESP_ERR__TIMED_OUT: // Attempt to connect again
                    return;
                default:
                    throw new TransportException($message->errstr(), $message->err);
            }
        }

        yield new Envelope($message);
    }

    public function ack(Envelope $envelope): void
    {
        dd('ack', $envelope);
    }

    public function reject(Envelope $envelope): void
    {
        dd('reject', $envelope);
    }
}
