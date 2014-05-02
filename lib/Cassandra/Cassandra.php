<?php
namespace Cassandra;

require __DIR__ . '/DataStream.php';
require __DIR__ . '/Rows.php';
require __DIR__ . '/Serialize.php';
require __DIR__ . '/Transport.php';

/**
 * Cassandra client library.
 *
 * Implementation of Cassandra binary protocol v1.
 *
 * @see https://git-wip-us.apache.org/repos/asf?p=cassandra.git;a=blob_plain;f=doc/native_protocol.spec;hb=refs/heads/cassandra-1.2
 */
class Connection
{
  // Consistency levels.
  const CONSISTENCY_ANY = 0x0000;
  const CONSISTENCY_ONE = 0x0001;
  const CONSISTENCY_TWO = 0x0002;
  const CONSISTENCY_THREE = 0x0003;
  const CONSISTENCY_QUORUM = 0x0004;
  const CONSISTENCY_ALL = 0x0005;
  const CONSISTENCY_LOCAL_QUORUM = 0x0006;
  const CONSISTENCY_EACH_QUORUM = 0x0007;
  const CONSISTENCY_LOCAL_ONE = 0x0010;

  protected $options = array(
    'cql_version' => '3.0.0',
    'connect_timeout' => 3,
    'stream_timeout' => 10,
    'default_consistency' => self::CONSISTENCY_QUORUM,
  );

  protected $transport;

  /**
   * Connect to Cassandra cluster.
   *
   * @param mixed $host
   *   Hostname as string or array of hostnames.
   * @param string $keyspace
   * @param array $options
   */
  public function __construct($host, $keyspace = NULL, $options = array()) {
    $this->options += $options;

    if (empty($host)) {
      throw new InvalidArgumentException('Invalid host');
    }
    if (!is_array($host)) {
      $host = array($host);
    }
    shuffle($host);
    while ($host) {
      $hostname = array_pop($host);
      try {
        $this->transport = new Transport($hostname, $this->options['connect_timeout'], $this->options['stream_timeout']);
        break;
      }
      catch (Exception $e) {
        if (empty($host)) {
          // No other hosts available, rethrow exception.
          throw $e;
        }
      }
    }

    // Send STARTUP frame.
    $body = Serialize::stringList(array('CQL_VERSION' => $this->options['cql_version']));
    $this->transport->sendFrame(Transport::STARTUP, $body);
    $frame = $this->transport->fetchFrame();
    if ($frame['opcode'] == Transport::AUTHENTICATE) {
      $this->authenticate();
    }
    if ($frame['opcode'] != Transport::READY) {
      throw new Exception('Invalid response from server');
    }

    // Use namespace.
    if ($keyspace) {
      $this->useKeyspace($keyspace);
    }
  }

  /**
   * Authenticate.
   */
  protected function authenticate() {
    if (empty($this->options['authenticate'])) {
      throw new InvalidArgumentException('Missing authenticate options');
    }
    // @todo: Implement authentication.
    throw new Exception('Authentication not supported');
  }

  /**
   * Switch to keyspace.
   *
   * @param string $name
   */
  public function useKeyspace($name) {
    if (!preg_match('/^[a-z][a-z0-9_]*$/si', $name)) {
      throw new InvalidArgumentException('Illegal keyspace name');
    }
    $this->query("USE $name");
  }

  /**
   * Execute query.
   *
   * @param string $cql
   * @param int $consistency
   */
  public function query($cql, $consistency = NULL) {
    if (is_null($consistency)) {
      $consistency = $this->options['default_consistency'];
    }
    $body = Serialize::longString($cql) . Serialize::short($consistency);
    $this->transport->sendFrame(Transport::QUERY, $body);
    $data = $this->transport->fetchFrame();
    $kind = $data['data']->readInt();
    switch ($kind) {
      // Void
      case 0x0001:
        return TRUE;
      // Rows
      case 0x0002:
        return new Rows($data['data']);
        break;
      // Set keyspace
      case 0x0003:
        return TRUE;
      // Prepared
      case 0x0004:
        return TRUE;
      // Schema change
      case 0x0005:
        return TRUE;
      default:
        throw new \Exception('Unknown response from server');
    }
  }
}
