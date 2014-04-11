/*
 * Copyright (C) 2014 Jens Bertram <code@jens-bertram.net>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.unihildesheim.lucene.index;

import de.unihildesheim.ByteArray;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the externally stored document term-data.
 *
 * @author Jens Bertram
 */
public final class ExternalDocTermDataManager {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          ExternalDocTermDataManager.class);

  /**
   * Store an individual map for each prefix.
   */
  private final Map<String, ConcurrentNavigableMap<Fun.Tuple3<
          Integer, String, ByteArray>, Object>> prefixMap;
  /**
   * R/w lock for {@link #prefixMap}.
   */
  private final ReentrantReadWriteLock prefixMapLock;
  /**
   * Database handling storage.
   */
  private final DB db;
  /**
   * Prefix to use for any keys.
   */
  private final String prefix;

  /**
   * Initialize the manager.
   *
   * @param newDb Database
   * @param newPrefix Prefix
   */
  @SuppressWarnings("CollectionWithoutInitialCapacity")
  ExternalDocTermDataManager(final DB newDb, final String newPrefix) {
    this.prefixMap = new ConcurrentHashMap<>();
    this.prefixMapLock = new ReentrantReadWriteLock();
    this.db = newDb;
    this.prefix = newPrefix;
  }

  /**
   * Check, if the given prefix is known. Throws a runtime {@link Exception},
   * if the prefix is not known.
   *
   * @param newPrefix Prefix to check
   */
  private void checkPrefix(final String newPrefix) {
    if (!this.prefixMap.containsKey(newPrefix)) {
      throw new IllegalArgumentException("Prefixed data was not known. "
              + "Was prefix '" + newPrefix
              + "' registered before being accessed?");
    }
  }

  /**
   * Remove any custom data stored while using the index.
   */
  protected void clear() {
    this.prefixMapLock.writeLock().lock();
    try {
      Iterator<String> prefixIt = this.prefixMap.keySet().iterator();
      while (prefixIt.hasNext()) {
        final String mapPrefix = prefixIt.next();
        final String mapName = this.prefix + mapPrefix;
        db.delete(mapName);
        prefixIt.remove();
      }
    } finally {
      this.prefixMapLock.writeLock().unlock();
    }
  }

  /**
   * Loads a stored prefix map from the database into the cache.
   *
   * @param newPrefix Prefix to load
   */
  protected void loadPrefix(final String newPrefix) {
    if (newPrefix == null || newPrefix.length() == 0) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    final String mapName = this.prefix + newPrefix;
    // stored data
    ConcurrentNavigableMap<Fun.Tuple3<Integer, String, ByteArray>, Object> map;
    if (!db.exists(mapName)) {
      LOG.debug("Creating a new docTermData map with prefix '{}'", newPrefix);
    }
    DB.BTreeMapMaker mapMkr = db.createTreeMap(mapName);
    final BTreeKeySerializer mapKeySerializer
            = new BTreeKeySerializer.Tuple3KeySerializer<>(null, null,
                    Serializer.INTEGER, Serializer.STRING,
                    ByteArray.SERIALIZER);
    mapMkr.keySerializer(mapKeySerializer);
    mapMkr.valueSerializer(Serializer.JAVA);
    this.prefixMapLock.writeLock().lock();
    try {
      map = mapMkr.makeOrGet();
      this.prefixMap.put(newPrefix, map);
    } finally {
      this.prefixMapLock.writeLock().unlock();
    }
  }

  /**
   * Store ter-data to the database.
   *
   * @param newPrefix Data prefix to use
   * @param documentId Document-id the data belongs to
   * @param term Term the data belongs to
   * @param key Key to identify the data
   * @param value Value to store
   * @return Any previous assigned data, or null, if there was none
   */
  protected Object setData(final String newPrefix, final int documentId,
          final ByteArray term, final String key, final Object value) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Null is not allowed as value.");
    }
    checkPrefix(newPrefix);
    Object returnObj = null;
    this.prefixMapLock.writeLock().lock();
    try {
      returnObj = this.prefixMap.get(newPrefix).put(Fun.t3(documentId, key,
              term.clone()), value);
    } catch (Exception ex) {
      LOG.error("EXCEPTION CATCHED: p={} id={} k={} t={} v={}", prefix,
              documentId, key, term, value, ex);
    } finally {
      this.prefixMapLock.writeLock().unlock();
    }
    return returnObj;
  }

  /**
   * Get stored document term-data for a specific prefix and key. This returns
   * only <tt>term, value</tt> pairs for the given document and prefix.
   *
   * @param newPrefix Prefix to lookup
   * @param documentId Document-id whose data to get
   * @param key Key to identify the data to get
   * @return Map with stored data for the given combination or null if there
   * is no data
   */
  @SuppressWarnings(
          "CollectionWithoutInitialCapacity")
  protected Map<ByteArray, Object> getData(final String newPrefix,
          final int documentId, final String key) {
    if (newPrefix == null || newPrefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    Map<ByteArray, Object> map = null;
    this.prefixMapLock.readLock().lock();
    try {
      ConcurrentNavigableMap<Fun.Tuple3<
              Integer, String, ByteArray>, Object> dataMap
              = this.prefixMap.get(newPrefix);
      if (dataMap == null) {
        return null;
      }
      // use the documents term frequency as initial size for the map
      //        map = new HashMap<>((int) (long) getTermFrequency(documentId));
      map = new HashMap<>();
      for (ByteArray term : Fun.filter(dataMap.keySet(), documentId, key)) {
        map.put(term.clone(), dataMap.get(Fun.t3(documentId, key, term)));
      }
    } finally {
      this.prefixMapLock.readLock().unlock();
    }
    return map;
  }

  /**
   * Get a single stored document term-data value for a specific prefix, term
   * and key.
   *
   * @param newPrefix Prefix to lookup
   * @param documentId Document-id whose data to get
   * @param key Key to identify the data to get
   * @param term Term to lookup
   * @return Value stored for the given combination, or null if there was no
   * data stored
   */
  protected Object getData(final String newPrefix, final int documentId,
          final ByteArray term, final String key) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    if (newPrefix == null || newPrefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    checkPrefix(newPrefix);
    return this.prefixMap.get(newPrefix).get(Fun.t3(documentId, key, term));
  }

}
