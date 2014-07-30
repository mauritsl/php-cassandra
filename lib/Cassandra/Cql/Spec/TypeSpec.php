<?php

namespace Cassandra\Cql\Spec;

use Cassandra\Cql\DataStream;

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
    public function __construct(DataStream $data)
    {
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
    public function getType()
    {
        return $this->type;
    }

    /**
     * Get custom typename.
     *
     * @return string
     */
    public function getCustomTypename()
    {
        return $this->customTypename;
    }

    /**
     * Get key type (applies for maps).
     *
     * @return TypeSpec
     */
    public function getKeyType()
    {
        return $this->keyType;
    }

    /**
     * Get value type (applies for collections).
     *
     * @return TypeSpec
     */
    public function getValueType()
    {
        return $this->valueType;
    }

    /**
     * Get typename.
     *
     * @return string
     */
    public function getTypeName()
    {
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
    public function __toString()
    {
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
