<?php

namespace Cassandra;

/**
 * Cassandra Framed transport.
 */
class Transport
{
  const ERROR = 0x00;
  const STARTUP = 0x01;
  const READY = 0x02;
  const AUTHENTICATE = 0x03;
  const CREDENTIALS = 0x04;
  const OPTIONS = 0x05;
  const SUPPORTED = 0x06;
  const QUERY = 0x07;
  const RESULT = 0x08;
  const PREPARE = 0x09;
  const EXECUTE = 0x0A;
  const REGISTER = 0x0B;
  const EVENT = 0x0C;

  protected $connection;
  protected $streams = array();

  /**
   * Constructor.
   *
   * @param string $host
   *   Hostname
   * @param float $timeout
   *   Connect timeout
   * @param float $stream_timeout
   *   Stream timeout
   */
  public function __construct($host, $connect_timeout = 3, $stream_timeout = 10) {
    if (strstr($host, ':')) {
      $port = (int) substr(strstr($host, ':'), 1);
      $host = substr($host, 0, -1 - strlen($port));
      if (!$port) {
        throw new InvalidArgumentException('Invalid port number');
      }
    }
    else {
      $port = 9042;
    }
    $this->connection = @fsockopen($host, $port, $errno, $errstr, $connect_timeout);
    if (!$this->connection) {
      throw new Exception('Unable to connect to Cassandra node: ' . $errstr, $errno);
    }
    stream_set_timeout($this->connection, (int) $stream_timeout, ($stream_timeout - (int) $stream_timeout) * 1000000);
  }

  /**
   * Send a frame.
   *
   * @param int $opcode
   * @param string $body
   * @return int
   *   Stream number used to match response
   */
  public function sendFrame($opcode, $body = NULL) {
    $version = 0x01; // Request frame
    $flags = 0x00;
    $stream = $this->getStream();
    $length = strlen($body);

    $header = pack('CCcCN', $version, $flags, $stream, $opcode, $length);
    fwrite($this->connection, $header);
    fwrite($this->connection, $body);

    return $stream;
  }

  /**
   * Retreives a frame.
   *
   * @return array
   *   Array with keys 'version', 'flags', 'stream' and 'length'
   */
  public function fetchFrame() {
    $data = $this->fetchData(8);
    $data = unpack('Cversion/Cflags/cstream/Copcode/Nlength', $data);
    if ($data['length']) {
      $data['data'] = new DataStream($this->fetchData($data['length']));
    }
    else {
      $data['data'] = new DataStream('');
    }
    if ($data['opcode'] == self::ERROR) {
      $code = $data['data']->readInt();
      $message = $data['data']->readString();
      throw new \Exception($message, $code);
    }
    return $data;
  }

  /**
   * Read data from stream.
   *
   * @param int $length
   * @return string
   */
  protected function fetchData($length) {
    $data = fread($this->connection, $length);
    $info = stream_get_meta_data($this->connection);
    if ($info['timed_out']) {
      throw new Exception('Connection timed out');
    }
    return $data;
  }

  /**
   * Get stream number for new frame.
   *
   * @return int
   */
  protected function getStream() {
    return 0;
  }

  /**
   * Release stream.
   *
   * @param int $n
   */
  protected function releaseStream($n) {

  }
}
