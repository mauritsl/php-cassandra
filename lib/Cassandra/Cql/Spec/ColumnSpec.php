<?php

namespace Cassandra\Cql\Spec;

use Cassandra\Cql\DataStream;

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
    public function __construct(DataStream $data, $keyspace = NULL, $tablename = NULL)
    {
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
    public function getKeyspace()
    {
        return $this->keyspace;
    }

    /**
     * Get tablename.
     *
     * @return string
     */
    public function getTablename()
    {
        return $this->tablename;
    }

    /**
     * Get columnname.
     *
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * Get column type.
     *
     * @return TypeSpec
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * Get string representation of this column as used in CQL.
     *
     * @return string
     */
    public function __toString()
    {
        return $this->name . ' ' . ((string) $this->type);
    }

}
