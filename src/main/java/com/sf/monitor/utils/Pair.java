package com.sf.monitor.utils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Pair<K, V> {
  @JsonProperty("key")
  public K k;
  @JsonProperty("value")
  public V v;

  public Pair(K k, V v) {
    this.k = k;
    this.v = v;
  }

  public static <K, V> Pair of(K k, V v) {
    return new Pair<K, V>(k, v);
  }
}
