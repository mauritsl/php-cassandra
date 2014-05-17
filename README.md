php-cassandra
=============

Cassandra client library for PHP, using the native binary protocol.

## Installation

PHP 5.3+ is required. There is no need for additional libraries.

## Basic usage

```php
<?php

require('./Cassandra/Cassandra.php');

// Connect to server.
$connection = new Cassandra\Connection('localhost', 'test');

// If you have slow connect - try to use IP instead (Windows bug)
// $connection = new Cassandra\Connection('127.0.0.1', 'test');

// Run query.
$rows = $connection->query('SELECT * FROM user');

// Get rowcount.
echo "Rows: " . $rows->count() . "\n";

// Fetch results.
foreach ($rows as $row) {
  var_dump($row);
}
?>
```

You can also specify a list of servers to connect to a random server from the list:

```php
<?php
$connection = new Cassandra\Connection(array('host:port', 'host:port'), 'namespace');
?>
```

## Iteration

The resultset is inherited from the Iterator class and can therefore be used in foreach loops. It is also possible to use its internal functions in a while loop:

```php
<?php
while ($rows->valid()) {
  $row = $rows->current();
  var_dump($row);
  $rows->next();
}
?>
```
## Column specification

It is possible to get a specification of the columns in the resultset:

```php
<?php
$columns = $rows->getColumns();
foreach ($columns as $column) {
  // Will print the column declaration as used in CREATE TABLE queries.
  echo (string) $column;
  
  // Get the keyspace.
  echo $column->getKeyspace();
  
  // Get the tablename.
  echo $column->getTablename();
  
  // Get type, will return an instance of the TypeSpec class.
  $type = $column->getType();
  
  // Prints the readable type declaration (e.g. "varchar" or "list<uuid>").
  echo (string) $type;
  
  // Prints the basetype (e.g. "list").
  echo $type->getTypeName();
  
  // You can get the types for keys and values in collections.
  if ($type->getType() == Cassandra\TypeSpec::COLLECTION_MAP) {
    // Only applicable to maps.
    echo $type->getKeyType()->getTypeName();
    // Applicable to all collections.
    echo $type->getValueType()->getTypeName();
  }
}
?>
```

## Supported datatypes

All types are supported.

* *ascii, varchar, text*
  Result will be a string.
* *bigint, counter, varint*
  Converted to strings using bcmath.
* *blob*
  Result will be a string.
* *boolean*
  Result will be a boolean as well.
* *decimal*
  Converted to strings using bcmath.
* *double, float, int*
  Result is using native PHP datatypes.
* *timestamp*
  Converted to integer. Milliseconds precision is lost.
* *uuid, timeuuid, inet*
  No native PHP datatype available. Converted to strings.
* *list, set*
  Converted to array (numeric keys).
* *map*
  Converted to keyed array.

