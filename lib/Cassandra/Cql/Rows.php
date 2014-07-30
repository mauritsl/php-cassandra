<?php

namespace Cassandra\Cql;

use Iterator;
use Exception;
use OutOfRangeException;
use stdClass;
use Cassandra\Cql\Spec\ColumnSpec;

/**
 * Rows
 */
class Rows implements Iterator
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
    public function __construct(DataStream $data)
    {
        $flags = $data->readInt();
        $this->columnCount = $data->readInt();
        if ($flags & 0x0001) {
            // Keyspace and tablename are specified globally.
            $keyspace = $data->readString();
            $tablename = $data->readString();
        }
        else {
            // Keyspace and tablename are specified per column.
            $keyspace = null;
            $tablename = null;
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
                } catch (Exception $e) {
                    $row[] = null;
                }
            }
            $this->rows[] = $row;
        }
    }

    /**
     * Get columns.
     */
    public function getColumns()
    {
        return $this->columns;
    }

    /**
     * Get number of rows.
     */
    public function count()
    {
        return $this->rowCount;
    }

    /**
     * Get current row.
     *
     * @see Iterator::current()
     */
    public function current()
    {
        if (!isset($this->rows[$this->current])) {
            throw new OutOfRangeException('Invalid index');
        }
        $row = $this->rows[$this->current];
        $object = new stdClass();
        for ($i = 0; $i < $this->columnCount; ++$i) {
            try {
                $data = new DataStream($this->rows[$this->current][$i]);
                $row[$i] = $data->readByType($this->columns[$i]->getType());
            }
            catch (Exception $e) {
                $row[$i] = null;
            }
        }
        return $row;
    }

    /**
     * Get current row number.
     *
     * @see Iterator::key()
     */
    public function key()
    {
        return $this->current;
    }

    /**
     * Move to next row.
     *
     * @see Iterator::next()
     */
    public function next()
    {
        ++$this->current;
    }

    /**
     * Move to first row.
     *
     * @see Iterator::rewind()
     */
    public function rewind()
    {
        $this->current = 0;
    }

    /**
     * Check if current row exists.
     *
     * @see Iterator::valid()
     */
    public function valid()
    {
        return $this->current < $this->rowCount;
    }
  
}
