<?php

namespace Cassandra\Cql;

use Exception;
use InvalidArgumentException;

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
    public function __construct($host, $connect_timeout = 3, $stream_timeout = 10)
    {
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
        $this->connection = socket_create(AF_INET , SOCK_STREAM, SOL_TCP);
        socket_set_option($this->connection, getprotobyname('TCP'), 1 /*TCP_NODELAY*/, 1);
        socket_set_option($this->connection,SOL_SOCKET,SO_RCVTIMEO, array("sec"=>$stream_timeout,"usec"=>0));
        if (!socket_connect($this->connection, $host, $port)) {
            throw new Exception('Unable to connect to Cassandra node: ');
        }
    }

    /**
     * Send a frame.
     *
     * @param int $opcode
     * @param string $body
     * @return int
     *   Stream number used to match response
     */
    public function sendFrame($opcode, $body = null)
    {
        $version = 0x01; // Request frame
        $flags = 0x00;
        $stream = $this->getStream();
        $length = strlen($body);

        $header = pack('CCcCN', $version, $flags, $stream, $opcode, $length);
        socket_write($this->connection, $header);
        socket_write($this->connection, $body);

        return $stream;
    }

    /**
     * Retreives a frame.
     *
     * @return array
     *   Array with keys 'version', 'flags', 'stream' and 'length'
     */
    public function fetchFrame()
    {
        $data = $this->fetchData(8);
        $data = unpack('Cversion/Cflags/cstream/Copcode/Nlength', $data);
        if ($data['length']) {
            $data['data'] = new DataStream($this->fetchData($data['length']));
        }
        else {
            $data['data'] = new DataStream('');
        }
        if ($data['opcode'] == self::ERROR) {
            throw $this->readError($data['data']);
        }
        return $data;
    }

    /**
     * Read error frame.
     *
     * @param DataStream $data
     * @return Exception
     */
    public function readError(DataStream $data)
    {
        $code = $data->readInt();
        $messages = array(
            0x0000 => 'Server error',
            0x000A => 'Protocol error',
            0x0100 => 'Bad credentials',
            0x1000 => 'Unavailable',
            0x1001 => 'Overloaded',
            0x1002 => 'Is bootstrapping',
            0x1003 => 'Truncate error',
            0x1100 => 'Write timeout',
            0x1200 => 'Read timeout',
            0x2000 => true,
            0x2100 => 'Unauthorized',
            0x2200 => true,
            0x2300 => 'Config error',
            0x2400 => 'Already exists',
            0x2500 => 'Unprepared',
        );
        if (isset($messages[$code])) {
            $message = $messages[$code];
            if ($message === true) {
                $message = $data->readString();
            }
        }
        else {
            $message = 'Unknown error';
        }
        return new Exception($message, $code);
    }

    /**
     * Read data from stream.
     *
     * @param int $length
     * @return string
     */
    protected function fetchData($length)
    {
        $data = socket_read($this->connection, $length);
        while(strlen($data) < $length) {
            $data .= socket_read($this->connection, $length);
        }
        if (socket_last_error($this->connection) == 110) {
            throw new Exception('Connection timed out');
        }
        return $data;
    }

    /**
     * Get stream number for new frame.
     *
     * @return int
     */
    protected function getStream()
    {
        return 0;
    }

    /**
     * Release stream.
     *
     * @param int $n
     */
    protected function releaseStream($n)
    {
        // @TODO
    }
    
}
