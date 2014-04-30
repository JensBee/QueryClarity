/*
 * Copyright (C) 2014 Jens Bertram
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
import de.unihildesheim.Persistence;
import de.unihildesheim.SerializableByte;
import de.unihildesheim.Tuple;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.util.RandomValue;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram
 */
public abstract class AbstractIndexDataProvider implements IndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          AbstractIndexDataProvider.class);

  /**
   * Write flush delay for async writes of database data.
   */
  private static final int DB_ANSYNC_WRITEFLUSH_DELAY = 100;

  /**
   * Flag indicating, if caches are filled (warmed).
   */
  @SuppressWarnings({ "ProtectedField", "checkstyle:visibilitymodifier" })
  protected boolean warmed = false;

  /**
   * Transient cached overall term frequency of the index.
   */
  @SuppressWarnings({ "ProtectedField", "checkstyle:visibilitymodifier" })
  protected Long idxTf = null;

  /**
   * Transient cached collection of all (non deleted) document-ids.
   */
  @SuppressWarnings({ "ProtectedField", "checkstyle:visibilitymodifier" })
  protected Collection<Integer> idxDocumentIds = null;

  /**
   * Transient cached document-frequency map for all terms in index.
   */
  @SuppressWarnings({ "ProtectedField", "checkstyle:visibilitymodifier" })
  protected ConcurrentNavigableMap<ByteArray, Integer> idxDfMap = null;

  /**
   * List of stop-words to exclude from term frequency calculations.
   */
  @SuppressWarnings({ "ProtectedField", "checkstyle:visibilitymodifier" })
  protected Collection<ByteArray> stopWords = Collections.
          <ByteArray>emptySet();

  /**
   * Transient cached collection of all index terms.
   */
  @SuppressWarnings({ "ProtectedField", "checkstyle:visibilitymodifier" })
  protected Set<ByteArray> idxTerms = null;

  /**
   * Flag indicating, if this instance is temporary (no data is hold
   * persistent).
   */
  private boolean isTemporary = false;

  /**
   * Persistent cached collection of all index terms mapped by document field.
   * Mapping is <tt>(Field, Term)</tt> to <tt>Frequency</tt>. Fields are
   * indexed by {@link #cachedFieldsMap}.
   */
  @SuppressWarnings({ "ProtectedField", "checkstyle:visibilitymodifier" })
  protected ConcurrentNavigableMap<Fun.Tuple2<
          SerializableByte, ByteArray>, Long> idxTermsMap
          = null;

  /**
   * List of fields cached by this instance. Mapping of field name to id
   * value.
   */
  @SuppressWarnings({ "ProtectedField", "checkstyle:visibilitymodifier" })
  protected Map<String, SerializableByte> cachedFieldsMap;

  /**
   * Persistent disk backed storage backend.
   */
  @SuppressWarnings({ "ProtectedField", "checkstyle:visibilitymodifier" })
  protected DB db = null;

  /**
   * DBMaker helpers to create storage objects on the database.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class DbMakers {

    /**
     * Ids of persistent data held in the database.
     */
    public enum Stores {

      /**
       * Mapping of all index terms.
       */
      IDX_TERMS_MAP
    }

    /**
     * Ids of temporary data caches held in the database.
     */
    public enum Caches {

      /**
       * Set of all terms.
       */
      IDX_TERMS,
      /**
       * Document term-frequency map.
       */
      IDX_DFMAP,
      /**
       * Fields mapping.
       */
      IDX_FIELDS,
      /**
       * Overall term frequency.
       */
      IDX_TF
    }

    /**
     * Private empty constructor for utility class.
     */
    private DbMakers() { // empty
    }

    /**
     * Serializer to use for {@link #idxTermsMap}.
     */
    static final BTreeKeySerializer IDX_TERMSMAP_KEYSERIALIZER
            = new BTreeKeySerializer.Tuple2KeySerializer<>(
                    SerializableByte.COMPARATOR, SerializableByte.SERIALIZER,
                    ByteArray.SERIALIZER);

    /**
     * Get a maker for {@link #idxTerms}.
     *
     * @param db Database reference
     * @return Maker for {@link #idxTerms}
     */
    static DB.BTreeSetMaker idxTermsMaker(final DB db) {
      return db.createTreeSet(Caches.IDX_TERMS.name())
              .serializer(new BTreeKeySerializer.BasicKeySerializer(
                              ByteArray.SERIALIZER))
              .counterEnable();
    }

    /**
     * Get a maker for {@link #idxDfMap}.
     *
     * @param db Database reference
     * @return Maker for {@link #idxDfMap}
     */
    static DB.BTreeMapMaker idxDfMapMaker(final DB db) {
      return db.createTreeMap(Caches.IDX_DFMAP.name())
              .keySerializerWrap(ByteArray.SERIALIZER)
              .valueSerializer(Serializer.INTEGER)
              .counterEnable();
    }

    /**
     * Get a maker for {@link #idxTermsMap}.
     *
     * @param db Database reference
     * @return Maker for {@link #idxTermsMap}
     */
    @SuppressWarnings("checkstyle:magicnumber")
    static DB.BTreeMapMaker idxTermsMapMkr(final DB db) {
      return db.createTreeMap(Stores.IDX_TERMS_MAP.name())
              .keySerializer(DbMakers.IDX_TERMSMAP_KEYSERIALIZER)
              .valueSerializer(Serializer.LONG)
              .nodeSize(100);
    }

    /**
     * Get a maker for {@link #cachedFieldsMap}.
     *
     * @param db Database reference
     * @return Maker for {@link #cachedFieldsMap}
     */
    static DB.BTreeMapMaker cachedFieldsMapMaker(final DB db) {
      return db.createTreeMap(Caches.IDX_FIELDS.name())
              .keySerializer(BTreeKeySerializer.STRING)
              .valueSerializer(SerializableByte.SERIALIZER)
              .counterEnable();
    }

    /**
     * Checks all {@link Stores} against the DB, collecting missing ones.
     *
     * @param db Database reference
     * @return List of missing {@link Stores}
     */
    static Collection<Stores> checkStores(final DB db) {
      final Collection<Stores> miss = new ArrayList<>(Stores.values().length);
      for (Stores s : Stores.values()) {
        if (!db.exists(s.name())) {
          miss.add(s);
        }
      }
      return miss;
    }

    /**
     * Checks all {@link Stores} against the DB, collecting missing ones.
     *
     * @param db Database reference
     * @return List of missing {@link Stores}
     */
    static Collection<Caches> checkCaches(final DB db) {
      final Collection<Caches> miss = new ArrayList<>(Caches.values().length);
      for (Caches c : Caches.values()) {
        if (!db.exists(c.name())) {
          miss.add(c);
        }
      }
      return miss;
    }
  }

  /**
   *
   * @param newIdentifier Identifier of implementing class
   * @param isTemp If true, all data will be deleted after terminating the
   * JVM. Gets overridden by {@link Environment} settings,
   */
  protected AbstractIndexDataProvider(final String newIdentifier,
          final boolean isTemp) {
    if (!Environment.isTestRun()) {
      this.isTemporary = isTemp;
    } else {
      this.isTemporary = true;
    }
    if (this.isTemporary) {
      LOG.info("Caches are temporary!");
    }
  }

  /**
   * Collect and cache all document-ids from the index.
   *
   * @return Unique collection of all document ids
   */
  protected abstract Collection<Integer> getDocumentIds();

  /**
   * Checks if this instance is temporary.
   *
   * @return True, if temporary
   */
  protected final boolean isTemporary() {
    return this.isTemporary;
  }

  /**
   * Update cached list of stopwords from the {@link Environment}.
   */
  protected final void setStopwordsFromEnvironment() {
    final Collection<String> newStopWords = Environment.getStopwords();
    this.stopWords = new HashSet<>(newStopWords.size());
    for (String stopWord : newStopWords) {
      try {
        this.stopWords.add(new ByteArray(stopWord.getBytes("UTF-8")));
      } catch (UnsupportedEncodingException ex) {
        LOG.error("Error adding stopword '" + stopWord + "'.", ex);
      }
    }
  }

  /**
   * Pre-cache index terms.
   */
  protected final void warmUpTerms() {
    final TimeMeasure tStep = new TimeMeasure();
    // cache all index terms
    if (this.idxTerms == null || this.idxTerms.isEmpty()) {
      tStep.start();
      LOG.info("Cache warming: terms..");
      getTerms(); // caches this.idxTerms
      LOG.info("Cache warming: collecting {} unique terms took {}.",
              this.idxTerms.size(), tStep.stop().getTimeString());
    } else {
      LOG.info("Cache warming: {} Unique terms already loaded.",
              this.idxTerms.size());
    }
  }

  /**
   * Pre-cache term frequencies.
   */
  protected final void warmUpIndexTermFrequencies() {
    final TimeMeasure tStep = new TimeMeasure();
    // collect index term frequency
    if (this.idxTf == null || this.idxTf == 0) {
      tStep.start();
      LOG.info("Cache warming: index term frequencies..");
      getTermFrequency(); // caches this.idxTf
      LOG.info("Cache warming: index term frequency calculation "
              + "for {} terms took {}.", this.idxTerms.size(), tStep.stop().
              getTimeString());
    } else {
      LOG.info("Cache warming: "
              + "{} index term frequency values already loaded.", this.idxTf);
    }
  }

  /**
   * Pre-cache document-ids.
   */
  protected final void warmUpDocumentIds() {
    final TimeMeasure tStep = new TimeMeasure();
    // cache document ids
    if (this.idxDocumentIds == null || this.idxDocumentIds.isEmpty()) {
      tStep.start();
      LOG.info("Cache warming: documents..");
      getDocumentIds(); // caches this.idxDocumentIds
      if (this.idxDocumentIds != null) {
        LOG.info("Cache warming: collecting {} documents took {}.",
                this.idxDocumentIds.size(), tStep.stop().getTimeString());
      }
    } else {
      LOG.info("Cache warming: {} documents already collected.",
              this.idxDocumentIds.size());
    }
  }

  /**
   * Pre-cache term-document frequencies.
   *
   * @throws Exception Overriding implementation may thrown an exception
   */
  protected abstract void warmUpDocumentFrequencies() throws Exception;

  /**
   * Default warmup method calling all warmUp* methods an tracks their running
   * time.
   *
   * @throws Exception Forwarded from dedicated warmUp methods
   */
  @Override
  @SuppressWarnings("checkstyle:designforextension")
  public void warmUp() throws Exception {
    if (this.warmed) {
      LOG.info("Caches are up to date.");
      return;
    }
    setStopwordsFromEnvironment();

    final TimeMeasure tOverAll = new TimeMeasure().start();

    // order matters!
    warmUpTerms(); // should be first
    warmUpIndexTermFrequencies(); // may need terms
    warmUpDocumentIds();
    warmUpDocumentFrequencies(); // may need terms and docIds

    LOG.info("Cache warmed. Took {}.", tOverAll.stop().getTimeString());
    this.warmed = true;
  }

  /**
   * Get the {@link Persistence} object to create a database.
   *
   * @param name Database name
   * @param createNew If true, a new database will be created. Throws an
   * error, if a database with the current name already exists.
   * @param createIfNotFound If true, a new database will be created, if none
   * with the given name exists
   * @return Tuple with Persistence object and flag indicating, if a new
   * database is created
   * @throws IOException Thrown on low-level I/O errors
   * @throws Environment.NoIndexException Thrown, if no index is provided in
   * the {@link Environment}
   */
  @SuppressWarnings("checkstyle:magicnumber")
  protected final Tuple.Tuple2<Persistence, Boolean> getPersistence(
          final String name,
          final boolean createNew,
          final boolean createIfNotFound) throws IOException,
          Environment.NoIndexException {
    final Persistence.Builder psb;
    final Tuple.Tuple2<Persistence, Boolean> ret;
    if (this.isTemporary) {
      LOG.warn("Caches are temporary!");
      psb = new Persistence.Builder(name + "_" + RandomValue.getString(6)).
              temporary();
    } else {
      psb = new Persistence.Builder(name);
    }
    psb.getMaker()
            .transactionDisable()
            .commitFileSyncDisable()
            .asyncWriteEnable()
            .asyncWriteFlushDelay(DB_ANSYNC_WRITEFLUSH_DELAY)
            .mmapFileEnableIfSupported()
            .closeOnJvmShutdown();
    if (createNew) {
      ret = Tuple.tuple2(psb.make(), true);
    } else if (!createIfNotFound) {
      ret = Tuple.tuple2(psb.get(), false);
    } else {
      if (!psb.exists()) {
        ret = Tuple.tuple2(psb.makeOrGet(), true);
      } else {
        ret = Tuple.tuple2(psb.makeOrGet(), false);
      }
    }
    this.db = ret.a.db;
    return ret;
  }

  /**
   * Get the id for a named field.
   *
   * @param fieldName Field name
   * @return Id of the field
   */
  protected final SerializableByte getFieldId(final String fieldName) {
    final SerializableByte fieldId = this.cachedFieldsMap.get(fieldName);
    if (fieldId == null) {
      throw new IllegalStateException("Unknown field '" + fieldName
              + "'. No id found.");
    }
    return fieldId;
  }

  /**
   * Get the term frequency including stop-words.
   *
   * @param term Term to lookup
   * @return Term frequency
   */
  @SuppressWarnings("checkstyle:methodname")
  protected final Long _getTermFrequency(final ByteArray term) {
    Long tf = 0L;
    for (String field : Environment.getFields()) {
      try {
        Long fieldTf = this.idxTermsMap.get(Fun.t2(getFieldId(field), term));
        if (fieldTf != null) {
          tf += fieldTf;
        }
      } catch (Exception ex) {
        LOG.error("EXCEPTION CAUGHT: f={} fId={} t={}", getFieldId(field),
                term, ex);
      }
    }
    return tf;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>0</tt>).
   */
  @Override
  public final Long getTermFrequency(final ByteArray term) {
    if (this.stopWords.contains(term)) {
      // skip stop-words
      return 0L;
    }
    return _getTermFrequency(term);
  }

  /**
   * {@inheritDoc} Stop-words will be skipped.
   */
  @Override
  public final long getTermFrequency() {
    if (this.idxTf == null) {
      LOG.info("Building term frequency index.");
      this.idxTf = 0L;

      SerializableByte fieldId;
      for (String field : Environment.getFields()) {
        fieldId = getFieldId(field);
        final Iterator<ByteArray> bytesIt = Fun.
                filter(this.idxTermsMap.keySet(), fieldId).iterator();
        while (bytesIt.hasNext()) {
          final ByteArray bytes = bytesIt.next();
          try {
            this.idxTf += this.idxTermsMap.get(Fun.t2(fieldId, bytes));
          } catch (NullPointerException ex) {
            LOG.error("EXCEPTION NULL: fId={} f={} b={}", fieldId, field,
                    bytes, ex);
          }
        }
      }

      // remove term frequencies of stop-words
      if (!this.stopWords.isEmpty()) {
        Long tf;
        for (ByteArray stopWord : this.stopWords) {
          tf = _getTermFrequency(stopWord);
          if (tf != null) {
            this.idxTf -= tf;
          }
        }
      }
      this.db.getAtomicLong(DbMakers.Caches.IDX_TF.name()).set(this.idxTf);
    }
    return this.idxTf;
  }

  /**
   * Collect and cache all index terms. Stop-words will be removed from the
   * list.
   *
   * @return Unique collection of all terms in index with stopwords removed
   */
  protected final Collection<ByteArray> getTerms() {
    if (this.idxTerms.isEmpty()) {
      LOG.info("Building transient index term cache.");

      final String[] fields = Environment.getFields();
      if (fields.length > 1) {
        new Processing(new Target.TargetFuncCall<>(
                new CollectionSource<>(Arrays.asList(fields)),
                new TermCollectorTarget(this.idxTermsMap.keySet(),
                        this.idxTerms)
        )).process(fields.length);
      } else {
        for (String field : fields) {
          Iterator<ByteArray> bytesIt = Fun.filter(this.idxTermsMap.keySet(),
                  getFieldId(field)).iterator();

          while (bytesIt.hasNext()) {
            this.idxTerms.add(bytesIt.next().clone());
          }
        }
      }
      this.idxTerms.removeAll(this.stopWords);
    }
    return Collections.unmodifiableCollection(this.idxTerms);
  }

  /**
   * Clear all dynamic caches. This must be called, if the fields or
   * stop-words have changed.
   */
  @SuppressWarnings("checkstyle:designforextension")
  protected void clearCache() {
    LOG.info("Clearing temporary caches.");
    // index terms cache (content depends on current fields & stopwords)
    if (this.db.exists(DbMakers.Caches.IDX_TERMS.name())) {
      this.db.delete(DbMakers.Caches.IDX_TERMS.name());
    }
    this.idxTerms = DbMakers.idxTermsMaker(this.db).make();

    // document term-frequency map
    // (content depends on current fields)
    if (this.db.exists(DbMakers.Caches.IDX_DFMAP.name())) {
      this.db.delete(DbMakers.Caches.IDX_DFMAP.name());
    }
    this.idxDfMap = DbMakers.idxDfMapMaker(this.db).make();

    this.idxTf = null;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>0</tt>).
   */
  @Override
  public final double getRelativeTermFrequency(final ByteArray term) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    if (this.stopWords.contains(term)) {
      // skip stop-words
      return 0d;
    }

    final double tf = getTermFrequency(term).doubleValue();
    if (tf == 0) {
      return 0d;
    }
    return tf / Long.valueOf(getTermFrequency()).doubleValue();
  }

  @Override
  @SuppressWarnings("checkstyle:designforextension")
  public void dispose() {
    if (this.db != null && !this.db.isClosed()) {
      LOG.info("Closing database.");
      this.db.commit();
      this.db.compact();
      this.db.close();
    }
  }

  @Override
  public final Iterator<ByteArray> getTermsIterator() {
    return getTerms().iterator();
  }

  @Override
  public final Source<ByteArray> getTermsSource() {
    return new CollectionSource<>(getTerms());
  }

  @Override
  public final Iterator<Integer> getDocumentIdIterator() {
    return getDocumentIds().iterator();
  }

  @Override
  public final Source<Integer> getDocumentIdSource() {
    return new CollectionSource<>(getDocumentIds());
  }

  @Override
  public final long getUniqueTermsCount() {
    return getTerms().size();
  }

  @Override
  public final boolean hasDocument(final Integer docId) {
    return this.idxDocumentIds.contains(docId);
  }

  @Override
  public final long getDocumentCount() {
    return getDocumentIds().size();
  }

  /**
   * {@link Processing} {@link Target} for collecting currently available
   * index terms.
   */
  private final class TermCollectorTarget extends Target.TargetFunc<String> {

    /**
     * Set to get terms from.
     */
    private final NavigableSet<Fun.Tuple2<SerializableByte, ByteArray>> terms;
    /**
     * Target set for results.
     */
    private final Set<ByteArray> target;

    /**
     * Create a new {@link Processing} {@link Target} for collecting index
     * terms.
     *
     * @param newTerms Set of all index terms
     * @param newTarget Target set to collect currently visible terms
     */
    private TermCollectorTarget(
            final NavigableSet<Fun.Tuple2<
          SerializableByte, ByteArray>> newTerms,
            final Set<ByteArray> newTarget) {
      this.terms = newTerms;
      this.target = newTarget;
    }

    @Override
    public void call(final String fieldName) {
      if (fieldName == null) {
        return;
      }
      Iterator<ByteArray> bytesIt = Fun.filter(this.terms, getFieldId(
              fieldName)).iterator();

      while (bytesIt.hasNext()) {
        this.target.add(bytesIt.next().clone());
      }
    }
  }
}
