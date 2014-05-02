<?php
namespace Cassandra;

/**
 * Serialize class for serializing to binary data.
 */
class Serialize
{
  /**
   * Serialize string.
   *
   * @param string $value
   * @return string
   */
  public static function string($value) {
    return pack('n', strlen($value)) . $value;
  }

  /**
   * Serialize stringlist.
   *
   * @param array $values
   * @return string
   */
  public static function stringList($values) {
    $output = pack('n', count($values));
    foreach ($values as $name => $value) {
      $output .= Serialize::string($name);
      $output .= Serialize::string($value);
    }
    return $output;
  }

  /**
   * Serialize short.
   *
   * @param int $value
   * @return string
   */
  public static function short($value) {
    return pack('n', $value);
  }

  /**
   * Serialize longstring.
   *
   * @param string $value
   * @return string
   */
  public static function longString($value) {
    return pack('N', strlen($value)) . $value;
  }
}
