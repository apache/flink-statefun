/*
 * Copyright (C) 2002-2017 Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.unimi.dsi.fastutil.objects;

import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;

import it.unimi.dsi.fastutil.HashCommon;
import java.util.Arrays;

/**
 * A type-specific hash map with a fast, small-footprint implementation.
 *
 * <p>Instances of this class use a hash table to represent a map. The table is filled up to a
 * specified <em>load factor</em>, and then doubled in size to accommodate new entries. If the table
 * is emptied below <em>one fourth</em> of the load factor, it is halved in size; however, the table
 * is never reduced to a size smaller than that at creation time: this approach makes it possible to
 * create maps with a large capacity in which insertions and deletions do not cause immediately
 * rehashing. Moreover, halving is not performed when deleting entries from an iterator, as it would
 * interfere with the iteration process.
 *
 * @see HashCommon
 */
public final class ObjectOpenHashMap<K, V> {

  /** The initial default size of a hash table. */
  private static final int DEFAULT_INITIAL_SIZE = 16;
  /** The default load factor of a hash table. */
  private static final float DEFAULT_LOAD_FACTOR = .75f;
  /** We never resize below this threshold, which is the construction-time {#n}. */
  private final transient int minN;
  /** The acceptable load factor. */
  private final float f;
  /** The array of keys. */
  private transient K[] key;
  /** The array of values. */
  private transient V[] value;
  /** The mask for wrapping a position counter. */
  private transient int mask;
  /** Whether this map contains the key zero. */
  private transient boolean containsNullKey;
  /** The current table size. */
  private transient int n;
  /** Threshold after which we rehash. It must be the table size times {@link #f}. */
  private transient int maxFill;
  /** Number of entries in the set (including the key zero, if present). */
  private int size;

  /**
   * Creates a new hash map.
   *
   * <p>The actual table size will be the least power of two greater than {@code expected}/{@code
   * f}.
   *
   * @param expected the expected number of elements in the hash map.
   * @param f the load factor.
   */
  @SuppressWarnings({"unchecked", "WeakerAccess"})
  public ObjectOpenHashMap(final int expected, final float f) {
    if (f <= 0 || f > 1) {
      throw new IllegalArgumentException(
          "Load factor must be greater than 0 and smaller than or equal to 1");
    }
    if (expected < 0) {
      throw new IllegalArgumentException("The expected number of elements must be nonnegative");
    }
    this.f = f;
    minN = n = arraySize(expected, f);
    mask = n - 1;
    maxFill = maxFill(n, f);
    key = (K[]) new Object[n + 1];
    value = (V[]) new Object[n + 1];
  }

  /**
   * Creates a new hash map with {@link #DEFAULT_LOAD_FACTOR} as load factor.
   *
   * @param expected the expected number of elements in the hash map.
   */
  @SuppressWarnings({"WeakerAccess", "unused"})
  public ObjectOpenHashMap(final int expected) {
    this(expected, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Creates a new hash map with initial expected {@link #DEFAULT_INITIAL_SIZE} entries and {@link
   * #DEFAULT_LOAD_FACTOR} as load factor.
   */
  @SuppressWarnings({"WeakerAccess", "unused"})
  public ObjectOpenHashMap() {
    this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
  }

  @SuppressWarnings({"unused"})
  public V put(final K k, final V v) {
    final int pos = find(k);
    if (pos < 0) {
      insert(-pos - 1, k, v);
      return null;
    }
    final V oldValue = value[pos];
    value[pos] = v;
    return oldValue;
  }

  @SuppressWarnings({"unchecked", "unused"})
  public V get(final Object k) {
    if (k == null) {
      return containsNullKey ? value[n] : null;
    }
    K curr;
    final K[] key = this.key;
    int pos;
    // The starting point.
    if (((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k).hashCode())) & mask])
        == null)) {
      return null;
    }
    if (((k).equals(curr))) {
      return value[pos];
    }
    // There's always an unused entry.
    while (true) {
      if (((curr = key[pos = (pos + 1) & mask]) == null)) {
        return null;
      }
      if (((k).equals(curr))) {
        return value[pos];
      }
    }
  }

  @SuppressWarnings({"unchecked", "unused"})
  public boolean containsKey(final Object k) {
    if (k == null) {
      return containsNullKey;
    }
    K curr;
    final K[] key = this.key;
    int pos;
    // The starting point.
    if (((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k).hashCode())) & mask])
        == null)) {
      return false;
    }
    if (((k).equals(curr))) {
      return true;
    }
    // There's always an unused entry.
    while (true) {
      if (((curr = key[pos = (pos + 1) & mask]) == null)) {
        return false;
      }
      if (((k).equals(curr))) {
        return true;
      }
    }
  }

  @SuppressWarnings({"unchecked", "unused"})
  public V remove(final Object k) {
    if (k == null) {
      if (containsNullKey) {
        return removeNullEntry();
      }
      return null;
    }
    K curr;
    final K[] key = this.key;
    int pos;
    // The starting point.
    if (((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k).hashCode())) & mask])
        == null)) {
      return null;
    }
    if (((k).equals(curr))) {
      return removeEntry(pos);
    }
    while (true) {
      if (((curr = key[pos = (pos + 1) & mask]) == null)) {
        return null;
      }
      if (((k).equals(curr))) {
        return removeEntry(pos);
      }
    }
  }

  @SuppressWarnings({"unused"})
  public void clear() {
    if (size == 0) {
      return;
    }
    size = 0;
    containsNullKey = false;
    Arrays.fill(key, (null));
    Arrays.fill(value, null);
  }

  @SuppressWarnings({"unused"})
  public int size() {
    return size;
  }

  @SuppressWarnings({"unused"})
  public boolean isEmpty() {
    return size == 0;
  }

  // -------------------------------------------------------------------------------------------------------------

  private int realSize() {
    return containsNullKey ? size - 1 : size;
  }

  private V removeEntry(final int pos) {
    final V oldValue = value[pos];
    value[pos] = null;
    size--;
    shiftKeys(pos);
    if (n > minN && size < maxFill / 4 && n > DEFAULT_INITIAL_SIZE) {
      rehash(n / 2);
    }
    return oldValue;
  }

  private V removeNullEntry() {
    containsNullKey = false;
    key[n] = null;
    final V oldValue = value[n];
    value[n] = null;
    size--;
    if (n > minN && size < maxFill / 4 && n > DEFAULT_INITIAL_SIZE) {
      rehash(n / 2);
    }
    return oldValue;
  }

  @SuppressWarnings("unchecked")
  private int find(final K k) {
    if (((k) == null)) {
      return containsNullKey ? n : -(n + 1);
    }
    K curr;
    final K[] key = this.key;
    int pos;
    // The starting point.
    if (((curr = key[pos = (it.unimi.dsi.fastutil.HashCommon.mix((k).hashCode())) & mask])
        == null)) {
      return -(pos + 1);
    }
    if (((k).equals(curr))) {
      return pos;
    }
    // There's always an unused entry.
    while (true) {
      if (((curr = key[pos = (pos + 1) & mask]) == null)) {
        return -(pos + 1);
      }
      if (((k).equals(curr))) {
        return pos;
      }
    }
  }

  private void insert(final int pos, final K k, final V v) {
    if (pos == n) {
      containsNullKey = true;
    }
    key[pos] = k;
    value[pos] = v;
    if (size++ >= maxFill) {
      rehash(arraySize(size + 1, f));
    }
  }

  /**
   * Shifts left entries with the specified hash code, starting at the specified position, and
   * empties the resulting free entry.
   *
   * @param pos a starting position.
   */
  private void shiftKeys(int pos) {
    // Shift entries with the same hash.
    int last, slot;
    K curr;
    final K[] key = this.key;
    for (; ; ) {
      pos = ((last = pos) + 1) & mask;
      for (; ; ) {
        if (((curr = key[pos]) == null)) {
          key[last] = (null);
          value[last] = null;
          return;
        }
        slot = (it.unimi.dsi.fastutil.HashCommon.mix((curr).hashCode())) & mask;
        if (last <= pos ? last >= slot || slot > pos : last >= slot && slot > pos) {
          break;
        }
        pos = (pos + 1) & mask;
      }
      key[last] = curr;
      value[last] = value[pos];
    }
  }

  /**
   * Rehashes the map.
   *
   * <p>This method implements the basic rehashing strategy, and may be overridden by subclasses
   * implementing different rehashing strategies (e.g., disk-based rehashing). However, you should
   * not override this method unless you understand the internal workings of this class.
   *
   * @param newN the new size
   */
  @SuppressWarnings({"unchecked", "StatementWithEmptyBody"})
  private void rehash(final int newN) {
    final K key[] = this.key;
    final V value[] = this.value;
    final int mask = newN - 1; // Note that this is used by the hashing macro
    final K newKey[] = (K[]) new Object[newN + 1];
    final V newValue[] = (V[]) new Object[newN + 1];
    int i = n, pos;
    for (int j = realSize(); j-- != 0; ) {
      while (((key[--i]) == null)) {}
      if (!((newKey[pos = (it.unimi.dsi.fastutil.HashCommon.mix((key[i]).hashCode())) & mask])
          == null)) {
        while (!((newKey[pos = (pos + 1) & mask]) == null)) {}
      }
      newKey[pos] = key[i];
      newValue[pos] = value[i];
    }
    newValue[newN] = value[n];
    n = newN;
    this.mask = mask;
    maxFill = maxFill(n, f);
    this.key = newKey;
    this.value = newValue;
  }

  /**
   * Returns a hash code for this map.
   *
   * <p>This method overrides the generic method provided by the superclass. Since {@code equals()}
   * is not overriden, it is important that the value returned by this method is the same value as
   * the one returned by the overriden method.
   *
   * @return a hash code for this map.
   */
  @Override
  public int hashCode() {
    int h = 0;
    for (int j = realSize(), i = 0, t = 0; j-- != 0; ) {
      while (((key[i]) == null)) {
        i++;
      }
      if (this != key[i]) {
        t = ((key[i]).hashCode());
      }
      if (this != value[i]) {
        t ^= ((value[i]) == null ? 0 : (value[i]).hashCode());
      }
      h += t;
      i++;
    }
    // Zero / null keys have hash zero.
    if (containsNullKey) {
      h += ((value[n]) == null ? 0 : (value[n]).hashCode());
    }
    return h;
  }
}
