<?php

namespace Ilbee\Symfony\Messenger\Bridge\Kafka\Transport;

use Ilbee\Symfony\Messenger\Bridge\Kafka\Kafka\ConnectionOption;
use RdKafka\KafkaConsumer;

/**
 * @author Julien Prigent <julien.prigent@dbmail.com>
 */
class Connection
{
    private ConnectionOption $connectionOption;

    public function __construct(string $dsn, array $options) {
        $this->connectionOption = new ConnectionOption($dsn, $options);
    }

    public function createConsumer(): KafkaConsumer
    {
        $consumer = new KafkaConsumer($this->connectionOption->getConsumerConfig());
        $consumer->subscribe(['test-topic']);

        return $consumer;
    }
}
