php-cassandra
=============

Cassandra client library for PHP, using the native binary protocol.

## Installation

PHP 5.3+ is required. There is no need for additional libraries.

## Basic usage

```php
  <?php
  
  require('./cassandra/Cassandra.php');
  
  use Cassandra\Connection as Cassandra;
  
  $connection = new Cassandra('localhost', 'test');
  
  // Run query.
  $rows = $connection->query('SELECT * FROM user');

  // Fetch results.
  foreach ($rows as $row) {
    var_dump($row);
  }
  
  // Get rowcount.
  echo "Rows: " . $rows->count() . "\n";
  
  // Get column specification.
  $columns = $rows->getColumns();
  foreach ($columns as $column) {
    echo (string) $column;
    echo "\n";
  }
```


