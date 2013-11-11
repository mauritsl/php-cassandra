<?php

namespace Cassandra;

/**
 * DataStream class for reading binary data.
 */
class DataStream
{
  protected $data;
  protected $length;

  /**
   * Create new DataStream.
   *
   * @param string $data
   */
  public function __construct($data) {
    $this->data = $data;
    $this->length = strlen($data);
  }

  /**
   * Read data from stream.
   *
   * @param int $length
   */
  protected function read($length) {
    if ($this->length < $length) {
      throw new \Exception('Reading while at end of stream');
    }
    $output = substr($this->data, 0, $length);
    $this->data = substr($this->data, $length);
    $this->length -= $length;
    return $output;
  }

  /**
   * Read single charactre.
   */
  public function readChar() {
    return reset(unpack('C', $this->read(1)));
  }

  /**
   * Read unsigned short.
   */
  public function readShort() {
    return reset(unpack('n', $this->read(2)));
  }

  /**
   * Read unsigned int.
   */
  public function readInt() {
    return reset(unpack('N', $this->read(4)));
  }

  /**
   * Read string.
   */
  public function readString() {
    $length = $this->readShort();
    return $this->read($length);
  }

  /**
   * Read long string.
   */
  public function readLongString() {
    $length = $this->readInt();
    return $this->read($length);
  }

  /**
   * Read bytes.
   */
  public function readBytes() {
    $length = $this->readInt();
    return $this->read($length);
  }

  /**
   * Read uuid.
   */
  public function readUuid() {
    $uuid = '';
    $data = $this->read(16);
    for ($i = 0; $i < 16; ++$i) {
      if ($i == 4 || $i == 6 || $i == 8 || $i == 10) {
        $uuid .= '-';
      }
      $uuid .= str_pad(dechex(ord($data{$i})), 2, '0', STR_PAD_LEFT);
    }
    return $uuid;
  }

  /**
   * Read timestamp.
   *
   * Cassandra is using the default java date representation, which is the
   * milliseconds since epoch. Since we cannot use 64 bits integers without
   * extra libraries, we are reading this as two 32 bits numbers and calculate
   * the seconds since epoch.
   */
  public function readTimestamp() {
    return round($this->readInt() * 4294967.296 + ($this->readInt() / 1000));
  }

  /**
   * Read list.
   *
   * @param TypeSpec $valueType
   * @return array
   */
  public function readList(TypeSpec $valueType) {
    $list = array();
    $count = $this->readShort();
    for ($i = 0; $i < $count; ++$i) {
      $list[] = $this->readByType($valueType);
    }
    return $list;
  }

  /**
   * Read map.
   *
   * @param string $keyType
   * @param string $valueType
   * @return array
   */
  public function readMap($keyType, $valueType) {
    $map = array();
    $count = $this->readShort();
    for ($i = 0; $i < $count; ++$i) {
      $map[$this->readByType($keyType)] = $this->readByType($valueType);
    }
    return $map;
  }

  /**
   * Read float.
   *
   * @return float
   */
  public function readFloat() {
    return reset(unpack('f', strrev($this->read(4))));
  }

  /**
   * Read double.
   *
   * @return double
   */
  public function readDouble() {
    return reset(unpack('d', strrev($this->read(8))));
  }

  /**
   * Read boolean.
   *
   * @return bool
   */
  public function readBoolean() {
    return (bool) $this->readChar();
  }

  /**
   * Read inet.
   *
   * @return string
   */
  public function readInet() {
    // @todo: Implement IPv6
    $inet = array();
    for ($i = 0; $i < 4; ++$i) {
      $inet[] = $this->readChar();
    }
    return implode('.', $inet);
  }

  /**
   * Read type provided by TypeSpec.
   *
   * @param TypeSpec $type
   * @return mixed
   */
  public function readByType(TypeSpec $type) {
    switch ($type->getType()) {
      case TypeSpec::CUSTOM:
        // @todo
        break;
      case TypeSpec::ASCII:
      case TypeSpec::VARCHAR:
      case TypeSpec::TEXT:
        return $this->data;
      case TypeSpec::BIGINT:
      case TypeSpec::COUNTER:
        // @todo
        break;
      case TypeSpec::BLOB:
        return $this->readBytes();
      case TypeSpec::BOOLEAN:
        return $this->readBoolean();
      case TypeSpec::DECIMAL:
        // @todo
        break;
      case TypeSpec::DOUBLE:
        return $this->readDouble();
      case TypeSpec::FLOAT:
        return $this->readFloat();
      case TypeSpec::INT:
        return $this->readInt();
      case TypeSpec::TIMESTAMP:
        return $this->readTimestamp();
      case TypeSpec::UUID:
        return $this->readUuid();
      case TypeSpec::VARINT:
        // @todo
        break;
      case TypeSpec::TIMEUUID:
        return $this->readUuid();
      case TypeSpec::INET:
        return $this->readInet();
        break;
      case TypeSpec::COLLECTION_LIST:
      case TypeSpec::COLLECTION_SET:
        return $this->readList($type->getValueType());
      case TypeSpec::COLLECTION_MAP:
        return $this->readMap($type->getKeyType(), $type->getValueType());
    }
  }
}
