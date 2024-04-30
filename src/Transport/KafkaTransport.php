<?php

namespace Ilbee\Symfony\Messenger\Bridge\Kafka\Transport;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * @author Julien Prigent <julien.prigent@dbmail.com>
 */
class KafkaTransport implements TransportInterface
{
    private Connection $connection;
    private SerializerInterface $serializer;
    private KafkaReceiver $receiver;

    public function __construct(Connection $connection, SerializerInterface $serializer)
    {
        $this->connection = $connection;
        $this->serializer = $serializer;
    }

    public function get(): iterable
    {
        return $this->getReceiver()->get();
    }

    public function ack(Envelope $envelope): void
    {
        dd('ack', $envelope);
    }

    public function reject(Envelope $envelope): void
    {
        dd('reject', $envelope);
    }

    public function send(Envelope $envelope): Envelope
    {
        dd('send', $envelope);
    }

    private function getReceiver(): KafkaReceiver
    {
        return $this->receiver ??= new KafkaReceiver($this->connection, $this->serializer);
    }
}
