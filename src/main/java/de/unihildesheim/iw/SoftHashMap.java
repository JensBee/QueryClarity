package de.unihildesheim.iw;

import java.io.Serializable;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Based on code from "The Java(tm) Specialists' Newsletter" by  Dr. Heinz M.
 * Kabutz <br> See http://www.javaspecialists.eu/archive/Issue098.html
 *
 * @author Jens Bertram
 */
public final class SoftHashMap<K, V>
    extends AbstractMap<K, V>
    implements Serializable {
  private static final long serialVersionUID = 3347203345852316364L;
  /**
   * Reference queue for cleared SoftReference objects.
   */
  @SuppressWarnings("PackageVisibleField")
  final ReferenceQueue<V> queue = new ReferenceQueue<>();
  /**
   * The internal HashMap that will hold the SoftReference.
   */
  private final Map<K, SoftReference<V>> hash = new HashMap<>();
  private final Map<SoftReference<V>, K> reverseLookup = new HashMap<>();

  @Override
  public int size() {
    expungeStaleEntries();
    return this.hash.size();
  }

  @SuppressWarnings("SuspiciousMethodCalls")
  @Override
  public V get(final Object key) {
    expungeStaleEntries();
    V result = null;
    // We get the SoftReference represented by that key
    final SoftReference<V> soft_ref = this.hash.get(key);
    if (soft_ref != null) {
      // From the SoftReference we get the value, which can be
      // null if it has been garbage collected
      result = soft_ref.get();
      if (result == null) {
        // If the value has been garbage collected, remove the
        // entry from the HashMap.
        this.hash.remove(key);
        this.reverseLookup.remove(soft_ref);
      }
    }
    return result;
  }

  @SuppressWarnings("ReturnOfNull")
  @Override
  public V put(final K key, final V value) {
    expungeStaleEntries();
    final SoftReference<V> soft_ref = new SoftReference<>(value, this.queue);
    this.reverseLookup.put(soft_ref, key);
    final SoftReference<V> result = this.hash.put(key, soft_ref);
    if (result == null) {
      return null;
    }
    this.reverseLookup.remove(result);
    return result.get();
  }

  @SuppressWarnings("ReturnOfNull")
  @Override
  public V remove(final Object key) {
    expungeStaleEntries();
    final SoftReference<V> result = this.hash.remove(key);
    if (result == null) {
      return null;
    }
    return result.get();
  }

  @Override
  public void clear() {
    this.hash.clear();
    this.reverseLookup.clear();
  }

  /**
   * Returns a copy of the key/values in the map at the point of calling.
   * However, setValue still sets the value in the actual SoftHashMap.
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  @Override
  public Set<Entry<K, V>> entrySet() {
    expungeStaleEntries();
    final Set<Entry<K, V>> result = new LinkedHashSet<>();
    for (final Entry<K, SoftReference<V>> entry : this.hash.entrySet()) {
      final V value = entry.getValue().get();
      if (value != null) {
        result.add(new Entry<K, V>() {
          @Override
          public K getKey() {
            return entry.getKey();
          }

          @Override
          public V getValue() {
            return value;
          }

          @Override
          public V setValue(final V v) {
            entry.setValue(new SoftReference<>(v, SoftHashMap.this.queue));
            return value;
          }
        });
      }
    }
    return result;
  }

  @SuppressWarnings("SuspiciousMethodCalls")
  private void expungeStaleEntries() {
    Reference<? extends V> sv;
    while ((sv = this.queue.poll()) != null) {
      this.hash.remove(this.reverseLookup.remove(sv));
    }
  }
}
