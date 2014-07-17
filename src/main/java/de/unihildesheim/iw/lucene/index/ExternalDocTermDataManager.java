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
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.mapdb.TupleSerializer;
import de.unihildesheim.iw.util.StringUtils;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.SortedSet;

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
   * Map that holds term data. Mapping is {@code (DocumentId, Term, Identifier)}
   * to {@code Value}.
   */
  private final BTreeMap<Fun.Tuple3<Integer, ByteArray, SerializableByte>,
      Double> map;
  /**
   * Lookup map for {@link #map} with inverted keys mapping.
   */
  private final BTreeMap<Fun.Tuple3<Integer, SerializableByte, ByteArray>,
      Fun.Tuple3<Integer, ByteArray, SerializableByte>> lookupMap;

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
    this.map = getMap();
    this.lookupMap = getLookupMap();

    Bind.secondaryKey(
        this.map, this.lookupMap, new Fun.Function2<
            Fun.Tuple3<Integer, SerializableByte, ByteArray>,
            Fun.Tuple3<Integer, ByteArray, SerializableByte>, Double>() {
          @Override
          public Fun.Tuple3<Integer, SerializableByte, ByteArray> run(
              final Fun.Tuple3<Integer, ByteArray, SerializableByte> key,
              final Double val) {
            return Fun.t3(key.a, key.c, key.b);
          }
        });
  }

  /**
   * Loads a stored prefix map from the database into the cache.
   *
   * @return Map matching the provided prefix
   */
  private BTreeMap<Fun.Tuple3<Integer, ByteArray, SerializableByte>,
      Double> getMap() {
    // stored data
    if (!this.db.exists(this.prefix)) {
      LOG.debug("Creating a new docTermData map with prefix '{}'",
          this.prefix);
    }

    final BTreeKeySerializer mapKeySerializer
        = new BTreeKeySerializer.Tuple3KeySerializer<>(null, null,
        Serializer.INTEGER, ByteArray.SERIALIZER, SerializableByte.SERIALIZER);
    final DB.BTreeMapMaker mapMkr = this.db.createTreeMap(this.prefix)
        .valueSerializer(Serializer.BASIC)
        .nodeSize(6)
        .keySerializer(mapKeySerializer);
    return mapMkr.makeOrGet();
  }

  /**
   * Loads a stored prefix lookup map from the database into the cache.
   *
   * @return Lookup map matching the provided prefix
   */
  private BTreeMap<Fun.Tuple3<Integer, SerializableByte, ByteArray>,
      Fun.Tuple3<Integer, ByteArray, SerializableByte>> getLookupMap() {
    final String mapName = this.prefix + "_lookup";
    // stored data
    if (!this.db.exists(mapName)) {
      LOG.debug("Creating a new docTermData lookup map with prefix '{}'",
          this.prefix);
    }

    final BTreeKeySerializer mapKeySerializer
        = new BTreeKeySerializer.Tuple3KeySerializer<>(null, null,
        Serializer.INTEGER, SerializableByte.SERIALIZER, ByteArray.SERIALIZER);
    final DB.BTreeMapMaker mapMkr = this.db.createTreeMap(mapName)
        .valueSerializer(new TupleSerializer.Tuple3Serializer(
            Serializer.INTEGER, ByteArray.SERIALIZER,
            SerializableByte.SERIALIZER))
        .nodeSize(6)
        .keySerializer(mapKeySerializer);
    return mapMkr.makeOrGet();
  }

  public ExternalDocTermDataManager getSubCollection(final String name) {
    return new ExternalDocTermDataManager(this.db, this.prefix + '_' + name);
  }

  /**
   * Remove any custom data stored while using the index.
   */
  public synchronized void clear() {
    this.map.clear();
    this.lookupMap.clear();
  }

  /**
   * Store a map with term-data to the database.
   *
   * @param documentId Document-id the data belongs to
   * @param key Key to identify the data
   * @param data Key, value pairs to store
   */
  public void setData(final int documentId, final SerializableByte key,
      final Map<ByteArray, Double> data) {
    for (final Map.Entry<ByteArray, Double> d : data.entrySet()) {
      setData(documentId, d.getKey(), key, d.getValue());
    }
  }

  /**
   * Store term-data to the database. Be careful, if called concurrently: the
   * value written is not being verified and is expected to be always the same.
   *
   * @param documentId Document-id the data belongs to
   * @param term Term the data belongs to
   * @param key Key to identify the data
   * @param value Value to store
   * @return Any previous assigned data, or null, if there was none
   */
  public Double setData(final int documentId,
      final ByteArray term,
      final SerializableByte key, final Double value) {
    @SuppressWarnings("unchecked")
    final Double ret =
        this.map.put(Fun.t3(
                documentId,
                new ByteArray(Objects.requireNonNull(term, "Term was null.")),
                Objects.requireNonNull(key, "Key was null.")),
            Objects.requireNonNull(value, "Value was null."));
    return ret;
  }

  public Map<Fun.Tuple3<Integer, ByteArray, SerializableByte>, Double>
  getRawData(final int documentId, final SerializableByte key) {
    return this.map.subMap(Fun.t3(documentId, (ByteArray) null, key), true,
        Fun.t3(documentId, ByteArray.MAX, key), true);
  }

  public Double getDirect(final SerializableByte key, final int documentId,
      final ByteArray term) {
    return this.map.get(Fun.t3(documentId, term, key));
  }

  /**
   * Get stored document term-data for a specific key. This returns only
   * <tt>term, value</tt> pairs for the given document and prefix. This method
   * is rather slow.
   *
   * @param documentId Document-id whose data to get
   * @param key Key to identify the data to get
   * @return Map with stored data for the given combination or null if there is
   * no data
   */
  public Map<ByteArray, Double> getData(final int documentId,
      final SerializableByte key) {
    Objects.requireNonNull(key, "Key was null.");
    final SortedSet<Fun.Tuple3<Integer, SerializableByte, ByteArray>> subSet =
        ((NavigableSet) this.lookupMap.keySet()).subSet(
            Fun.t3(documentId, key, null),
            Fun.t3(documentId, key, ByteArray.MAX)
        );

    if (subSet.isEmpty()) {
      // no data stored
      return Collections.emptyMap();
    }

    final Map<ByteArray, Double> retMap = new HashMap<>(subSet.size());

    for (final Fun.Tuple3<Integer, SerializableByte, ByteArray> t3 : subSet) {
      retMap.put(t3.c, this.map.get(Fun.t3(documentId, t3.c, key)));
    }

    assert !retMap.isEmpty() : "Map is empty!";

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
  public Double getData(final int documentId, final ByteArray term,
      final SerializableByte key) {
    return this.map.get(Fun.t3(
        documentId,
        Objects.requireNonNull(key, "Key was null."),
        Objects.requireNonNull(term, "Term was null.")));
  }

  /**
   * Get the Database reference. Used for unit testing.
   *
   * @return Database reference
   */
  DB getDb() {
    return this.db;
  }
}
