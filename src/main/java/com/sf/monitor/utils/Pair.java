package com.sf.monitor.utils;

public class Pair<K, V> {
  public K k;
  public V v;

  public Pair(K k, V v) {
    this.k = k;
    this.v = v;
  }

  public static <K, V> Pair of(K k, V v) {
    return new Pair<K, V>(k, v);
  }
}
