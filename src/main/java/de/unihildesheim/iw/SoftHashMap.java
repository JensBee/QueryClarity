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
 * Kabutz
 * <p/>
 * See http://www.javaspecialists.eu/archive/Issue098.html
 *
 * @author Jens Bertram
 */
public class SoftHashMap<K, V>
    extends AbstractMap<K, V>
    implements Serializable {
  /**
   * The internal HashMap that will hold the SoftReference.
   */
  private final Map<K, SoftReference<V>> hash =
      new HashMap<K, SoftReference<V>>();

  private final Map<SoftReference<V>, K> reverseLookup =
      new HashMap<SoftReference<V>, K>();

  /**
   * Reference queue for cleared SoftReference objects.
   */
  private final ReferenceQueue<V> queue = new ReferenceQueue<V>();

  public V get(final Object key) {
    expungeStaleEntries();
    V result = null;
    // We get the SoftReference represented by that key
    final SoftReference<V> soft_ref = hash.get(key);
    if (soft_ref != null) {
      // From the SoftReference we get the value, which can be
      // null if it has been garbage collected
      result = soft_ref.get();
      if (result == null) {
        // If the value has been garbage collected, remove the
        // entry from the HashMap.
        hash.remove(key);
        reverseLookup.remove(soft_ref);
      }
    }
    return result;
  }

  private void expungeStaleEntries() {
    Reference<? extends V> sv;
    while ((sv = queue.poll()) != null) {
      hash.remove(reverseLookup.remove(sv));
    }
  }

  public V put(final K key, final V value) {
    expungeStaleEntries();
    final SoftReference<V> soft_ref = new SoftReference<V>(value, queue);
    reverseLookup.put(soft_ref, key);
    final SoftReference<V> result = hash.put(key, soft_ref);
    if (result == null) {
      return null;
    }
    reverseLookup.remove(result);
    return result.get();
  }

  public V remove(final Object key) {
    expungeStaleEntries();
    final SoftReference<V> result = hash.remove(key);
    if (result == null) {
      return null;
    }
    return result.get();
  }

  public void clear() {
    hash.clear();
    reverseLookup.clear();
  }

  public int size() {
    expungeStaleEntries();
    return hash.size();
  }

  /**
   * Returns a copy of the key/values in the map at the point of calling.
   * However, setValue still sets the value in the actual SoftHashMap.
   */
  @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
  @Override
  public Set<Entry<K, V>> entrySet() {
    expungeStaleEntries();
    final Set<Entry<K, V>> result = new LinkedHashSet<Entry<K, V>>();
    for (final Entry<K, SoftReference<V>> entry : hash.entrySet()) {
      final V value = entry.getValue().get();
      if (value != null) {
        result.add(new Entry<K, V>() {
          public K getKey() {
            return entry.getKey();
          }

          public V getValue() {
            return value;
          }

          public V setValue(final V v) {
            entry.setValue(new SoftReference<V>(v, queue));
            return value;
          }
        });
      }
    }
    return result;
  }
}
