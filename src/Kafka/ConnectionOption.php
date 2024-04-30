<?php

namespace Ilbee\Symfony\Messenger\Bridge\Kafka\Kafka;

use RdKafka\Conf;

class ConnectionOption
{
    private string $dsn;
    private array $options;
    private bool $enableAutoCommit = false;
    private bool $enablePartitionEof = true;
    private ?string $clientId;
    private ?string $groupId;
    private array $brokerList;
    private string $autoOffsetReset;

    public const DSN_REGEX = '/^(?P<scheme>kafka(\+ssl)?):\/\/(?P<hostname>[a-zA-Z0-9.-]+)(?P<port>:([0-9]{1,5}))?/';

    public function __construct($dsn, $options)
    {
        $this->setDsn($dsn);
        $this->setOptions($options);

        $this->initialize();
    }

    private function initialize()
    {
        $this->setAutoOffsetReset('earliest');

        preg_match(self::DSN_REGEX, $this->dsn, $matches);
        return $this->addBroker($matches['hostname'] . ($matches['port'] ?? ':9092'));
    }

    private function addBroker(string $broker): self
    {
        $this->brokerList[] = $broker;

        return $this;
    }

    private function setDsn(string $dsn): self
    {
        $this->dsn = $dsn;

        return $this;
    }

    private function setOptions(array $options): self
    {
        $this->options = $options;

        return $this;
    }

    private function isEnableAutoCommit(): bool
    {
        return $this->enableAutoCommit;
    }

    private function isEnablePartitionEof(): bool
    {
        return $this->enablePartitionEof;
    }

    private function getGroupId(): string
    {
        return $this->groupId ?? 'ilbee.messenger.kafka.bridge';
    }

    private function getClientId(): string
    {
        return $this->clientId ?? hash('sha256', $this->getBrokerList(), time());
    }

    private function getBrokerList(): string
    {
        return implode(',', $this->brokerList);
    }

    private function setAutoOffsetReset(string $autoOffsetReset): self
    {
        $this->autoOffsetReset = $autoOffsetReset;

        return $this;
    }

    private function getAutoOffsetReset(): string
    {
        return $this->autoOffsetReset;
    }

    public function getConsumerConfig(): Conf
    {
        $conf = new Conf();
        $conf->set('metadata.broker.list', $this->getBrokerList());
        $conf->set('client.id', $this->getClientId());
        $conf->set('group.id', $this->getGroupId());
        $conf->set('enable.auto.commit', $this->isEnableAutoCommit() ? 'true' : 'false');
        $conf->set('auto.offset.reset', $this->getAutoOffsetReset());
        $conf->set('enable.partition.eof', $this->isEnablePartitionEof() ? 'true' : 'false');

        return $conf;
    }
}