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
package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.BytesRef;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Abstract implementation of {@link IndexDataProvider} supporting {@link
 * Persistence} storage.
 *
 * @author Jens Bertram
 */
abstract class AbstractIndexDataProvider
    implements IndexDataProvider {

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
  @SuppressWarnings({"ProtectedField", "checkstyle:visibilitymodifier"})
  boolean warmed = false;

  /**
   * Transient cached overall term frequency of the index.
   */
  private Long idxTf = null;

  /**
   * Transient cached collection of all (non deleted) document-ids.
   */
  @SuppressWarnings({"ProtectedField", "checkstyle:visibilitymodifier"})
  private Set<Integer> idxDocumentIds = null;

  /**
   * Transient cached document-frequency map for all terms in index.
   */
  @SuppressWarnings({"ProtectedField", "checkstyle:visibilitymodifier"})
  ConcurrentNavigableMap<ByteArray, Integer> idxDfMap = null;

  /**
   * List of stop-words to exclude from term frequency calculations.
   * Byte-encoded list for internal use.
   */
  private Set<ByteArray> stopwords = Collections.<ByteArray>emptySet();

  /**
   * List of stop-words to exclude from term frequency calculations. Cached
   * String set.
   */
  private Set<String> stopwordsStr = Collections.<String>emptySet();

  /**
   * Transient cached collection of all index terms.
   */
  @SuppressWarnings({"ProtectedField", "checkstyle:visibilitymodifier"})
  private Set<ByteArray> idxTerms = null;

  /**
   * Persistent cached collection of all index terms mapped by document field.
   * Mapping is <tt>(Field, Term)</tt> to <tt>Frequency</tt>. Fields are indexed
   * by {@link #cachedFieldsMap}.
   */
  @SuppressWarnings({"ProtectedField", "checkstyle:visibilitymodifier"})
  private ConcurrentNavigableMap<Fun.Tuple2<
      SerializableByte, ByteArray>, Long> idxTermsMap
      = null;

  /**
   * List of fields cached by this instance. Mapping of field name to id value.
   */
  private Map<String, SerializableByte> cachedFieldsMap;

  /**
   * Persistent disk backed storage backend.
   */
  @SuppressWarnings({"ProtectedField", "checkstyle:visibilitymodifier"})
  private DB db = null;

  /**
   * Flag indicating, if this instance is temporary (no data is hold
   * persistent).
   */
  private boolean isTemporary = false;

  /**
   * {@link IndexReader} to access the Lucene index.
   */
  private IndexReader idxReader;

  /**
   * List of document fields to operate on.
   */
  private Set<String> documentFields;

  /**
   * Last commit generation id of the Lucene index.
   */
  private Long indexLastCommitGeneration = null;

  /**
   * Flag indicating, if this instance is closed.
   */
  private transient boolean isDisposed = false;

  /**
   * Initializes the abstract instance.
   *
   * @param isTemp If true, all data will be deleted after terminating the JVM.
   */
  AbstractIndexDataProvider(final boolean isTemp) {
    this.isTemporary = isTemp;
    if (this.isTemporary) {
      LOG.info("Caches are temporary!");
    }
  }

  /**
   * Set the {@link IndexReader} to use for accessing the Lucene index.
   *
   * @param reader Reader to use
   */
  protected void setIndexReader(final IndexReader reader) {
    this.idxReader = reader;
  }

  @Override
  public IndexReader getIndexReader() {
    return this.idxReader;
  }

  /**
   * Set the list of document fields to operate on.
   *
   * @param fields List of document field names
   */
  protected void setDocumentFields(final Set<String> fields) {
    this.documentFields = fields;
  }

  @Override
  public Set<String> getDocumentFields() {
    return Collections.unmodifiableSet(this.documentFields);
  }

  /**
   * Set the (String) list of stopwords.
   *
   * @param words List of words to exclude
   * @throws java.io.UnsupportedEncodingException Thrown, if a term could not be
   * encoded into the target charset (usually UTF-8)
   */
  protected void setStopwords(final Set<String> words)
      throws UnsupportedEncodingException {
    this.stopwordsStr = words;
    this.stopwords = new HashSet<>();
    for (String word : words) {
      // terms in Lucene are UTF-8 encoded
      this.stopwords.add(new ByteArray(word.getBytes("UTF-8")));
    }
  }

  @Override
  public Set<String> getStopwords() {
    return this.stopwordsStr;
  }

  /**
   * Set the database to use for persistent storage.
   * @param newDb
   */
  protected void setDb(final DB newDb) {
    this.db = newDb;
  }

  /**
   * Get the database used for persistent storage.
   * @return
   */
  protected DB getDb() {
    return this.db;
  }

  /**
   * Set the index terms list.
   * @param newIdxTerms
   */
  protected void setIdxTerms(final Set<ByteArray> newIdxTerms) {
    if (newIdxTerms == null) {
      throw new IllegalArgumentException("Index terms set was null.");
    }
    this.idxTerms = newIdxTerms;
  }

  /**
   * Get the index terms list.
   * @return
   */
  protected Set<ByteArray> getIdxTerms() {
    return this.idxTerms;
  }

  protected void setCachedFieldsMap(final Map<String,
      SerializableByte> newFieldsMap) {
    if (newFieldsMap == null) {
      throw new IllegalArgumentException("Cached fields map was null.");
    }
    this.cachedFieldsMap = newFieldsMap;
  }

  protected Map<String, SerializableByte> getCachedFieldsMap() {
    return this.cachedFieldsMap;
  }

  protected void addFieldToCacheMap(final String field) {
    final Collection<SerializableByte> keys = new HashSet<>(
        this.cachedFieldsMap.values());
    for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
      @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
      final SerializableByte sByte = new SerializableByte(i);
      if (!keys.contains(sByte)) {
        getCachedFieldsMap().put(field, sByte);
        keys.add(sByte);
        break;
      }
    }
  }

  protected void setIdxTermsMap(ConcurrentNavigableMap<Fun.Tuple2<
      SerializableByte, ByteArray>, Long> newIdxTermsMap) {
    if (newIdxTermsMap == null) {
      throw new IllegalArgumentException("Index terms map was null.");
    }
    this.idxTermsMap = newIdxTermsMap;
  }

  protected ConcurrentNavigableMap<Fun.Tuple2<
      SerializableByte, ByteArray>, Long> getIdxTermsMap() {
    return this.idxTermsMap;
  }

  protected void setIdxDocumentIds(final Set<Integer> docIds) {
    if (docIds == null) {
      throw new IllegalArgumentException("Document id list was null.");
    }
    this.idxDocumentIds = docIds;
  }

  protected Set<Integer> getIdxDocumentIds() {
    return this.idxDocumentIds;
  }

  protected void setIdxTf(final Long tf) {
    this.idxTf = tf;
  }

  protected Long getIdxTf() {
    return this.idxTf;
  }

  /**
   * Checks, if the given {@link ByteArray} is flagged as stopword.
   *
   * @param word Word to check
   * @return True, if it's a stopword
   */
  protected boolean isStopword(final ByteArray word) {
    return this.stopwords.contains(word);
  }

  /**
   * Checks, if the given String is flagged as stopword.
   *
   * @param word Word to check
   * @return True, if it's a stopword
   */
  protected boolean isStopword(final String word)
      throws UnsupportedEncodingException {
    return isStopword(new ByteArray(word.getBytes("UTF-8")));
  }

  /**
   * Checks, if the given {@link BytesRef} is flagged as stopword.
   *
   * @param word Word to check
   * @return True, if it's a stopword
   */
  protected boolean isStopword(final BytesRef word) {
    return isStopword(BytesRefUtils.toByteArray(word));
  }

  /**
   * Set the last Lucene index commit generation id.
   *
   * @param cGen Generation id
   */
  protected void setLastIndexCommitGeneration(final Long cGen) {
    if (cGen == null) {
      // TODO: Really throw? Enough to go with zero?
      throw new IllegalArgumentException("Index generation was null.");
    }
    this.indexLastCommitGeneration = cGen;
  }

  @Override
  public long getLastIndexCommitGeneration() {
    return this.indexLastCommitGeneration;
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
  final boolean isTemporary() {
    return this.isTemporary;
  }

  /**
   * Pre-cache index terms.
   */
  final void warmUpTerms() {
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
  final void warmUpIndexTermFrequencies() {
    final TimeMeasure tStep = new TimeMeasure();
    // collect index term frequency
    if (this.idxTf == null || this.idxTf == 0) {
      tStep.start();
      LOG.info("Cache warming: index term frequencies..");
      getTermFrequency(); // caches this.idxTf
      LOG.info("Cache warming: index term frequency calculation "
              + "for {} terms took {}.", this.idxTerms.size(),
          tStep.stop().
              getTimeString()
      );
    } else {
      LOG.info("Cache warming: "
              + "{} index term frequency values already loaded.",
          this.idxTf
      );
    }
  }

  /**
   * Pre-cache document-ids.
   */
  final void warmUpDocumentIds() {
    final TimeMeasure tStep = new TimeMeasure();
    // cache document ids
    if (this.idxDocumentIds == null || this.idxDocumentIds.isEmpty()) {
      tStep.start();
      LOG.info("Cache warming: documents..");
      getDocumentIds(); // caches this.idxDocumentIds
      if (this.idxDocumentIds != null) {
        LOG.info("Cache warming: collecting {} documents took {}.",
            this.idxDocumentIds.size(),
            tStep.stop().getTimeString());
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
  protected abstract void warmUpDocumentFrequencies()
      throws Exception;

  /**
   * Default warmup method calling all warmUp* methods an tracks their running
   * time.
   *
   * @throws Exception Forwarded from dedicated warmUp methods
   */
  @Override
  @SuppressWarnings("checkstyle:designforextension")
  public void warmUp()
      throws Exception {
    if (this.warmed) {
      LOG.info("Caches are up to date.");
      return;
    }

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
   * Get the id for a named field.
   *
   * @param fieldName Field name
   * @return Id of the field
   */
  final SerializableByte getFieldId(final String fieldName) {
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
  final Long _getTermFrequency(final ByteArray term) {
    Long tf = 0L;
    for (String field : this.documentFields) {
      try {
        Long fieldTf =
            this.idxTermsMap.get(Fun.t2(getFieldId(field), term));
        if (fieldTf != null) {
          tf += fieldTf;
        }
      } catch (Exception ex) {
        LOG.error("EXCEPTION CAUGHT: f={} fId={} t={}",
            getFieldId(field),
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
    if (this.stopwords.contains(term)) {
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
      for (String field : this.documentFields) {
        fieldId = getFieldId(field);
        for (final ByteArray bytes : Fun.
            filter(this.idxTermsMap
                    .keySet(),
                fieldId
            )) {
          try {
            this.idxTf +=
                this.idxTermsMap.get(Fun.t2(fieldId, bytes));
          } catch (NullPointerException ex) {
            LOG.error("EXCEPTION NULL: fId={} f={} b={}", fieldId,
                field,
                bytes, ex);
          }
        }
      }

      // remove term frequencies of stop-words
      if (!this.stopwords.isEmpty()) {
        Long tf;
        for (ByteArray stopWord : this.stopwords) {
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
  final Collection<ByteArray> getTerms() {
    if (this.idxTerms.isEmpty()) {
      LOG.info("Building transient index term cache.");

      if (this.documentFields.size() > 1) {
        new Processing(new Target.TargetFuncCall<>(
            new CollectionSource<>(this.documentFields),
            new TermCollectorTarget(this.idxTermsMap.keySet(),
                this.idxTerms)
        )).process(this.documentFields.size());
      } else {
        for (String field : this.documentFields) {
          for (final ByteArray byteArray : Fun
              .filter(this.idxTermsMap.keySet(),
                  getFieldId(field))) {
            this.idxTerms.add(byteArray.clone());
          }
        }
      }
      this.idxTerms.removeAll(this.stopwords);
    }
    return Collections.unmodifiableCollection(this.idxTerms);
  }

  /**
   * Clear all dynamic caches. This must be called, if the fields or stop-words
   * have changed.
   */
  @SuppressWarnings("checkstyle:designforextension")
  void clearCache() {
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
    if (this.stopwords.contains(term)) {
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
    this.isDisposed = true;
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
    if (this.idxDocumentIds == null) {
      throw new IllegalStateException("No document ids set.");
    }
    return this.idxDocumentIds.contains(docId);
  }

  @Override
  public final long getDocumentCount() {
    return getDocumentIds().size();
  }

  @Override
  public boolean isDisposed() {
    return this.isDisposed;
  }

  /**
   * DBMaker helpers to create storage objects on the database.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class DbMakers {

    /**
     * Serializer to use for {@link #idxTermsMap}.
     */
    static final BTreeKeySerializer IDX_TERMSMAP_KEYSERIALIZER
        = new BTreeKeySerializer.Tuple2KeySerializer<>(
        SerializableByte.COMPARATOR, SerializableByte.SERIALIZER,
        ByteArray.SERIALIZER);

    /**
     * Private empty constructor for utility class.
     */
    private DbMakers() { // empty
    }

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
      final Collection<Stores> miss =
          new ArrayList<>(Stores.values().length);
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
      final Collection<Caches> miss =
          new ArrayList<>(Caches.values().length);
      for (Caches c : Caches.values()) {
        if (!db.exists(c.name())) {
          miss.add(c);
        }
      }
      return miss;
    }

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
  }

  /**
   * {@link Processing} {@link Target} for collecting currently available index
   * terms.
   */
  private final class TermCollectorTarget
      extends Target.TargetFunc<String> {

    /**
     * Set to get terms from.
     */
    private final NavigableSet<Fun.Tuple2<SerializableByte, ByteArray>>
        terms;
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

      for (final ByteArray byteArray : Fun.filter(this.terms, getFieldId(
          fieldName))) {
        this.target.add(byteArray.clone());
      }
    }
  }
}
