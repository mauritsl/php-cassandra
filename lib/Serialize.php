<?php

namespace Cassandra;

class Serialize
{
  public function string($value) {
    return pack('n', strlen($value)) . $value;
  }

  public function stringList($values) {
    $output = pack('n', count($values));
    foreach ($values as $name => $value) {
      $output .= Serialize::string($name);
      $output .= Serialize::string($value);
    }
    return $output;
  }

  public function short($value) {
    return pack('n', $value);
  }

  public function longString($value) {
    return pack('N', strlen($value)) . $value;
  }
}
