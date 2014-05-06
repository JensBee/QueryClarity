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
package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.ByteArray;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

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
   * Database handling storage.
   */
  private final DB db;
  /**
   * Prefix to use for any keys.
   */
  private final String prefix;
  /**
   * Map that holds term data.
   */
  private ConcurrentNavigableMap<
      Fun.Tuple3<String, Integer, ByteArray>, Object> map;

  /**
   * Initialize the manager.
   *
   * @param newDb Database
   * @param newPrefix Prefix
   */
  public ExternalDocTermDataManager(final DB newDb, final String newPrefix) {
    this.db = newDb;
    this.prefix = newPrefix;
    getMap(this.prefix);
  }

  /**
   * Remove any custom data stored while using the index.
   */
  public void clear() {
    this.db.delete(this.prefix);
    getMap(this.prefix);
  }

  /**
   * Loads a stored prefix map from the database into the cache.
   *
   * @param newPrefix Prefix to load
   */
  private void getMap(final String newPrefix) {
    if (newPrefix == null || newPrefix.length() == 0) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    // stored data
    if (!db.exists(this.prefix)) {
      LOG.debug("Creating a new docTermData map with prefix '{}'",
          this.prefix);
    }

    final BTreeKeySerializer mapKeySerializer
        = new BTreeKeySerializer.Tuple3KeySerializer<>(null, null,
        Serializer.STRING_INTERN, Serializer.INTEGER,
        ByteArray.SERIALIZER);
    DB.BTreeMapMaker mapMkr = db.createTreeMap(this.prefix)
        .keySerializer(mapKeySerializer)
        .valueSerializer(Serializer.BASIC)
        .nodeSize(16)
        .valuesOutsideNodesEnable();
    this.map = mapMkr.makeOrGet();
  }

  /**
   * Store term-data to the database.
   *
   * @param documentId Document-id the data belongs to
   * @param term Term the data belongs to
   * @param key Key to identify the data
   * @param value Value to store
   * @return Any previous assigned data, or null, if there was none
   */
  public Object setData(final int documentId,
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
    Object returnObj = null;
    try {
      returnObj = this.map.put(Fun.t3(key, documentId, term.clone()), value);
    } catch (Exception ex) {
      LOG.error("EXCEPTION CATCHED: p={} id={} k={} t={} v={}", prefix,
          documentId, key, term, value, ex);
    }
    return returnObj;
  }

  /**
   * Get stored document term-data for a specific key. This returns only
   * <tt>term, value</tt> pairs for the given document and prefix.
   *
   * @param documentId Document-id whose data to get
   * @param key Key to identify the data to get
   * @return Map with stored data for the given combination or null if there is
   * no data
   */
  public Map<ByteArray, Object> getData(final int documentId,
      final String key) {
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Map<ByteArray, Object> retMap = new HashMap<>();
    for (ByteArray term : Fun.filter(this.map.keySet(), key, documentId)) {
      retMap.put(term.clone(), this.map.get(Fun.t3(key, documentId, term)));
    }
    return retMap;
  }

  /**
   * Get a single stored document term-data value for a specific term and key.
   *
   * @param documentId Document-id whose data to get
   * @param key Key to identify the data to get
   * @param term Term to lookup
   * @return Value stored for the given combination, or null if there was no
   * data stored
   */
  public Object getData(final int documentId,
      final ByteArray term, final String key) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    return this.map.get(Fun.t3(key, documentId, term));
  }

}