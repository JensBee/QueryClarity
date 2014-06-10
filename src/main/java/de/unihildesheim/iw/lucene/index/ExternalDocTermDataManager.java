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
import de.unihildesheim.iw.util.StringUtils;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.SortedSet;
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
   * Map that holds term data. Mapping is {@code (Identifier, DocumentId, Term)}
   * to {@code Value}.
   */
  private ConcurrentNavigableMap<
      Fun.Tuple3<String, Integer, ByteArray>, Object> map;

  /**
   * Initialize the manager.
   *
   * @param newDb Database
   * @param newPrefix Storage prefix used to distinguish data from various
   * instances
   */
  public ExternalDocTermDataManager(final DB newDb, final String newPrefix) {
    Objects.requireNonNull(newDb, "DB was null.");
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(newPrefix, "Prefix was null."))) {
      throw new IllegalArgumentException("Prefix was empty.");
    }
    this.db = newDb;

    this.prefix = newPrefix;
    getMap(this.prefix);
  }

  /**
   * Loads a stored prefix map from the database into the cache.
   *
   * @param newPrefix Prefix to load
   */
  private void getMap(final String newPrefix) {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(newPrefix, "Prefix was null."))) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    // stored data
    if (!this.db.exists(this.prefix)) {
      LOG.debug("Creating a new docTermData map with prefix '{}'",
          this.prefix);
    }

    final BTreeKeySerializer mapKeySerializer
        = new BTreeKeySerializer.Tuple3KeySerializer<>(null, null,
        Serializer.STRING_INTERN, Serializer.INTEGER, ByteArray.SERIALIZER);
    final DB.BTreeMapMaker mapMkr = this.db.createTreeMap(this.prefix)
//        .valueSerializer(Serializer.BASIC)
//        .nodeSize(16)
//        .valuesOutsideNodesEnable()
        .keySerializer(mapKeySerializer);
    this.map = mapMkr.makeOrGet();
  }

  /**
   * Remove any custom data stored while using the index.
   */
  public void clear() {
    this.db.delete(this.prefix);
    getMap(this.prefix);
  }

  /**
   * Store a map with term-data to the database.
   *
   * @param documentId Document-id the data belongs to
   * @param key Key to identify the data
   * @param data Key, value pairs to store
   */
  public <T> void setData(final int documentId, final String key, final
  Map<ByteArray, T> data) {
    for (final Map.Entry<ByteArray, T> d : data.entrySet()) {
      setData(documentId, d.getKey(), key, d.getValue());
    }
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
  public <T> T setData(final int documentId,
      final ByteArray term,
      final String key, final T value) {
    Objects.requireNonNull(term, "Term was null.");
    Objects.requireNonNull(value, "Value was null.");

    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(key, "Key was null."))) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    @SuppressWarnings("unchecked")
    final T ret = (T) this.map.put(Fun.t3(key, documentId, term.clone()),
        value);
    return ret;
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
  public <T> Map<ByteArray, T> getData(final int documentId,
      final String key) {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(key, "Key was null."))) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }

    @SuppressWarnings("unchecked")
    final SortedSet<Fun.Tuple3<String, Integer, ByteArray>> subSet =
        ((NavigableSet) this.map.keySet()).subSet(
            Fun.t3(key, documentId, null),
            Fun.t3(key, documentId, Fun.<ByteArray>HI())
        );
    final Map<ByteArray, T> retMap = new HashMap<>(subSet.size());
    for (final Fun.Tuple3<String, Integer, ByteArray> t3 : subSet) {
      @SuppressWarnings("unchecked")
      final T val = (T) this.map.get(Fun.t3(key, documentId, t3.c));
      retMap.put(t3.c, val);
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
  @SuppressWarnings("unchecked")
  public <T> T getData(final int documentId, final ByteArray term,
      final String key) {
    Objects.requireNonNull(term, "Term was null.");
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(key, "Key was null."))) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    return (T) this.map.get(Fun.t3(key, documentId, term));
  }

}
