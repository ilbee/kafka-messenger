<?php

namespace Ilbee\Symfony\Messenger\Bridge\Kafka\Transport;

use Ilbee\Symfony\Messenger\Bridge\Kafka\Kafka\ConnectionOption;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * @author Julien Prigent <julien.prigent@dbmail.com>
 */
final class KafkaTransportFactory implements TransportFactoryInterface
{
    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        if (!\extension_loaded('rdkafka')) {
            throw new TransportException(sprintf(
                'You cannot use the "%s" as the "rdkafka" extension is not installed.',
                __CLASS__
            ));
        }

        return new KafkaTransport(
            new Connection($dsn, $options),
            $serializer
        );
    }

    public function supports(string $dsn, array $options): bool
    {
        return preg_match(ConnectionOption::DSN_REGEX, $dsn);
    }
}
