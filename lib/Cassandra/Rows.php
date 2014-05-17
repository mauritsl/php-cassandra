<?php
namespace Cassandra;

/**
 * Rows
 */
class Rows implements \Iterator
{
  protected $columns = array();
  protected $rowCount;
  protected $columnCount;
  protected $current = 0;
  protected $rows = array();

  /**
   * Create Rows object from binary data.
   *
   * @param DataStream $data
   */
  public function __construct(DataStream $data) {
    $flags = $data->readInt();
    $this->columnCount = $data->readInt();
    if ($flags & 0x0001) {
      // Keyspace and tablename are specified globally.
      $keyspace = $data->readString();
      $tablename = $data->readString();
    }
    else {
      // Keyspace and tablename are specified per column.
      $keyspace = NULL;
      $tablename = NULL;
    }
    for ($i = 0; $i < $this->columnCount; ++$i) {
      $this->columns[] = new ColumnSpec($data, $keyspace, $tablename);
    }
    $this->rowCount = $data->readInt();
    for ($i = 0; $i < $this->rowCount; ++$i) {
      $row = array();
      for ($j = 0; $j < $this->columnCount; ++$j) {
        try {
          $row[] = $data->readBytes();
        } catch (\Exception $e) {
          $row[] = NULL;
        }
      }
      $this->rows[] = $row;
    }
  }

  /**
   * Get columns.
   */
  public function getColumns() {
    return $this->columns;
  }

  /**
   * Get number of rows.
   */
  public function count() {
    return $this->rowCount;
  }

  /**
   * Get current row.
   *
   * @see Iterator::current()
   */
  public function current() {
    if (!isset($this->rows[$this->current])) {
      throw new \OutOfRangeException('Invalid index');
    }
    $row = $this->rows[$this->current];
    $object = new \stdClass();
    for ($i = 0; $i < $this->columnCount; ++$i) {
      try {
        $data = new DataStream($this->rows[$this->current][$i]);
        $row[$i] = $data->readByType($this->columns[$i]->getType());
      }
      catch (\Exception $e) {
        $row[$i] = NULL;
      }
    }
    return $row;
  }

  /**
   * Get current row number.
   *
   * @see Iterator::key()
   */
  public function key() {
    return $this->current;
  }

  /**
   * Move to next row.
   *
   * @see Iterator::next()
   */
  public function next() {
    ++$this->current;
  }

  /**
   * Move to first row.
   *
   * @see Iterator::rewind()
   */
  public function rewind() {
    $this->current = 0;
  }

  /**
   * Check if current row exists.
   *
   * @see Iterator::valid()
   */
  public function valid() {
    return $this->current < $this->rowCount;
  }
}

/**
 * Column specification.
 */
class ColumnSpec
{
  protected $keyspace;
  protected $tablename;
  protected $name;
  protected $type;

  /**
   * Create ColumnSpec from binary data.
   *
   * @param DataStream $data
   * @param string $keyspace
   * @param string $tablename
   */
  public function __construct(DataStream $data, $keyspace = NULL, $tablename = NULL) {
    if (is_null($keyspace)) {
      $this->keyspace = $data->readString();
      $this->tablename = $data->readString();
    }
    else {
      $this->keyspace = $keyspace;
      $this->tablename = $tablename;
    }
    $this->name = $data->readString();
    $this->type = new TypeSpec($data);
  }

  /**
   * Get keyspace.
   *
   * @return string
   */
  public function getKeyspace() {
    return $this->keyspace;
  }

  /**
   * Get tablename.
   *
   * @return string
   */
  public function getTablename() {
    return $this->tablename;
  }

  /**
   * Get columnname.
   *
   * @return string
   */
  public function getName() {
    return $this->name;
  }

  /**
   * Get column type.
   *
   * @return TypeSpec
   */
  public function getType() {
    return $this->type;
  }

  /**
   * Get string representation of this column as used in CQL.
   *
   * @return string
   */
  public function __toString() {
    return $this->name . ' ' . ((string) $this->type);
  }
}

/**
 * Type specification.
 *
 */
class TypeSpec
{
  const CUSTOM = 0x0000;
  const ASCII = 0x0001;
  const BIGINT = 0x0002;
  const BLOB = 0x0003;
  const BOOLEAN = 0x0004;
  const COUNTER = 0x0005;
  const DECIMAL = 0x0006;
  const DOUBLE = 0x0007;
  const FLOAT = 0x0008;
  const INT = 0x0009;
  const TEXT = 0x000A;
  const TIMESTAMP = 0x000B;
  const UUID = 0x000C;
  const VARCHAR = 0x000D;
  const VARINT = 0x000E;
  const TIMEUUID = 0x000F;
  const INET = 0x0010;
  const COLLECTION_LIST = 0x0020;
  const COLLECTION_MAP = 0x0021;
  const COLLECTION_SET = 0x0022;

  protected $type;
  protected $customTypename;
  protected $keyType;
  protected $valueType;

  /**
   * Construct new TypeSpec from binary data.
   *
   * @param DataStream $data
   */
  public function __construct(DataStream $data) {
    $this->type = $data->readShort();
    switch ($this->type) {
      case self::CUSTOM:
        $this->customTypename = $data->readString();
        break;
      case self::COLLECTION_LIST:
      case self::COLLECTION_SET:
        $this->valueType = new TypeSpec($data);
        break;
      case self::COLLECTION_MAP:
        $this->keyType = new TypeSpec($data);
        $this->valueType = new TypeSpec($data);
        break;
    }
  }

  /**
   * Get binary representation for type.
   *
   * @return TypeSpec
   */
  public function getType() {
    return $this->type;
  }

  /**
   * Get custom typename.
   *
   * @return string
   */
  public function getCustomTypename() {
    return $this->customTypename;
  }

  /**
   * Get key type (applies for maps).
   *
   * @return TypeSpec
   */
  public function getKeyType() {
    return $this->keyType;
  }

  /**
   * Get value type (applies for collections).
   *
   * @return TypeSpec
   */
  public function getValueType() {
    return $this->valueType;
  }

  /**
   * Get typename.
   *
   * @return string
   */
  public function getTypeName() {
    $names = array(
      self::CUSTOM => 'custom',
      self::ASCII => 'ascii',
      self::BIGINT => 'bigint',
      self::BLOB => 'blob',
      self::BOOLEAN => 'boolean',
      self::COUNTER => 'counter',
      self::DECIMAL => 'decimal',
      self::DOUBLE => 'double',
      self::FLOAT => 'float',
      self::INT => 'int',
      self::TEXT => 'text',
      self::TIMESTAMP => 'timestamp',
      self::UUID => 'uuid',
      self::VARCHAR => 'varchar',
      self::VARINT => 'varint',
      self::TIMEUUID => 'timeuuid',
      self::INET => 'inet',
      self::COLLECTION_LIST => 'list',
      self::COLLECTION_MAP => 'map',
      self::COLLECTION_SET => 'set',
    );
    if (isset($names[$this->type])) {
      return $names[$this->type];
    }
  }

  /**
   * Get textual representation of type, as used in CQL.
   *
   * @return string
   */
  public function __toString() {
    switch ($this->type) {
      case self::COLLECTION_LIST:
        $valueType = (string) $this->valueType;
        return "list<$valueType>";
      case self::COLLECTION_SET:
        $valueType = (string) $this->valueType;
        return "set<$valueType>";
      case self::COLLECTION_MAP:
        $keyType = (string) $this->keyType;
        $valueType = (string) $this->valueType;
        return "map<$keyType,$valueType>";
      default:
        return $this->getTypeName();
    }
  }
}
