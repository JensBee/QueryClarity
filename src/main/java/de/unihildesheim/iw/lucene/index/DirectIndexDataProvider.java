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

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.GlobalConfiguration;
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.mapdb.DBMakerUtils;
import de.unihildesheim.iw.mapdb.TupleSerializer;
import de.unihildesheim.iw.util.FileUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetException;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import de.unihildesheim.iw.util.termFilter.TermFilter;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.mapdb.Atomic;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Pump;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.NumberFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * {@link IndexDataProvider} implementation directly accessing the Lucene index
 * and using some caching to speed-up data processing.
 *
 * @author Jens Bertram
 */
public final class DirectIndexDataProvider
    extends AbstractIndexDataProvider
    implements AutoCloseable {

  /**
   * Prefix used to store {@link GlobalConfiguration configuration} data.
   */
  private static final String IDENTIFIER = "DirectIDP";
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(DirectIndexDataProvider.class);
  /**
   * Data storage encapsulating class.
   */
  private final Cache cache;
  /**
   * Inverted keys for faster access to index document term frequency map.
   */
  private NavigableSet<
      Fun.Tuple3<Integer, SerializableByte, ByteArray>> invertedIdxDocTermsMap;
  /**
   * Wrapper for persistent data storage (static data).
   */
  private Persistence persistStatic;
  /**
   * Wrapper for persistent data storage (transient data).
   */
  private Persistence persistTransient;

  /**
   * Builder based constructor.
   *
   * @param builder Builder to use for constructing the instance
   * @throws Buildable.BuildException Thrown, if initializing the instance with
   * the provided builder has failed
   * @throws DataProviderException Thrown, if initializing the instance data
   * failed
   */
  DirectIndexDataProvider(final Builder builder)
      throws Buildable.BuildException, DataProviderException {
    super(Objects.requireNonNull(builder, "Builder was null.").isTemporary);

    // set configuration
    this.setIndexReader(builder.idxReader);
    this.setDocumentFields(builder.documentFields);
    this.setLastIndexCommitGeneration(builder.lastCommitGeneration);
    try {
      this.setStopwords(builder.stopwords);
    } catch (final UnsupportedEncodingException e) {
      throw new Buildable.BuildException("Error parsing stopwords.", e);
    }

    // initialize
    try {
      this.cache = new Cache(builder);
    } catch (final Buildable.BuildableException |
        ProcessingException | IOException e) {
      this.close();
      throw new DataProviderException.CacheException(
          "Failed to initialize cache.", e);
    }

    if (builder.doWarmUp) {
      try {
        this.warmUp();
      } catch (final DataProviderException e) {
        this.close();
        throw e;
      }
    }
  }

  @Override
  public void warmUp()
      throws DataProviderException {
    checkClosed();
    if (this.persistTransient == null) {
      throw new DataProviderException("Cache not initialized.");
    }

    if (!areCachesWarmed()) {
      super.warmUp();

      if (getIdxTf() == null || getIdxTf() == 0) {
        throw new IllegalStateException("Zero term frequency.");
      }

      if (this.persistTransient.getDb()
          .exists(CacheDbMakers.Caches.IDX_TF.name())) {
        this.persistTransient.getDb().getAtomicLong(
            CacheDbMakers.Caches.IDX_TF.name()).set(getIdxTf());
      } else {
        this.persistTransient.getDb().createAtomicLong(
            CacheDbMakers.Caches.IDX_TF.name(), getIdxTf());
      }


      if (getIdxDocumentIds().isEmpty()) {
        throw new IllegalStateException("Zero document ids.");
      }

      if (getIdxTerms().isEmpty()) {
        throw new IllegalStateException("Zero terms.");
      }

      this.persistTransient.updateMetaData(getDocumentFields(), getStopwords());
      this.cache.commit(CacheDB.ALL);
    }
  }

  @Override
  protected void warmUpTerms()
      throws DataProviderException {
    checkClosed();
    try {
      this.persistTransient.getDb().createAtomicBoolean(CacheDbMakers.Flags
          .IDX_TERMS_COLLECTING_RUN.name(), true);

      warmUpTerms_default();

      this.persistTransient.getDb().delete(
          CacheDbMakers.Flags.IDX_TERMS_COLLECTING_RUN.name());
      this.cache.commit(CacheDB.ALL);
    } catch (final ProcessingException e) {
      throw new DataProviderException("Failed to warm-up terms", e);
    }
  }

  @Override
  protected void warmUpIndexTermFrequencies() {
    checkClosed();
    warmUpIndexTermFrequencies_default();
  }

  @Override
  protected void warmUpDocumentIds() {
    checkClosed();
    warmUpDocumentIds_default();
  }

  @Override
  protected void warmUpDocumentFrequencies()
      throws DataProviderException {
    checkClosed();
    try {
      this.persistTransient.getDb().createAtomicBoolean(
          CacheDbMakers.Flags.IDX_DOC_FREQ_CALC_RUN.name(), true);
      warmUpDocumentFrequencies_default();
      this.persistTransient.getDb()
          .delete(CacheDbMakers.Flags.IDX_DOC_FREQ_CALC_RUN.name());
      this.cache.commit(CacheDB.TRANSIENT);
    } catch (final ProcessingException e) {
      throw new DataProviderException("Failed to warm-up document " +
          "frequencies", e);
    }
  }

  public static String getIdentifier() {
    return IDENTIFIER;
  }

  /**
   * Shared method for {@link IndexSegmentTermsCollectorTarget} and {@link
   * IndexFieldsTermsCollectorTarget} collecting all terms from a {@link
   * DocsEnum}. The results are stored to both the index terms map and document
   * term-frequency map.
   *
   * @param cacheMap Map to store calculated values
   * @param term Current term that is being collected
   * @param fieldId Id of the field that gets processed
   * @param docsEnum Enum initialized with the given term
   * @param ttf Total term frequency (in index) of the given term
   * @param docBase DocBase value {@see AtomicReaderContext#docBase} to
   * calculate the read document-id
   * @throws IOException Thrown on low-level i/o errors
   */
  void collectTerms(
      final ConcurrentNavigableMap<Fun.Tuple3<
          ByteArray, SerializableByte, Integer>, Integer> cacheMap,
      final ByteArray term, final SerializableByte fieldId,
      final DocsEnum docsEnum,
      final long ttf, final int docBase)
      throws IOException {
    // initialize the document iterator
    int docId = docsEnum.nextDoc();

    Integer oldDTFValue;

    if (ttf < 0L) {
      throw new IllegalArgumentException("Negative values for term " +
          "frequencies not allowed. Value=" + ttf + ".");
    }

    if (LOG.isDebugEnabled()) {
      Objects.requireNonNull(term);
      Objects.requireNonNull(fieldId);
      Objects.requireNonNull(docsEnum);
    }

    // build term frequency map for each document
    while (docId != DocIdSetIterator.NO_MORE_DOCS) {
      final Fun.Tuple3<ByteArray, SerializableByte, Integer>
          idxDocTfMapKey = Fun.t3(term, fieldId, docId + docBase);
      final int freq = docsEnum.freq();

      // replace value
      oldDTFValue = cacheMap.putIfAbsent(idxDocTfMapKey, freq);
      // retry, until really replaced
      if (oldDTFValue != null) {
        while (!cacheMap.replace(idxDocTfMapKey,
            oldDTFValue, oldDTFValue + freq)) {
          oldDTFValue = cacheMap.get(idxDocTfMapKey);
        }
      }

      docId = docsEnum.nextDoc();
    }

    // add whole index term frequency map
    final Fun.Tuple2<SerializableByte, ByteArray> idxTfMapKey =
        Fun.t2(fieldId, term);
    Long oldValue = getIdxTermsMap().putIfAbsent(idxTfMapKey, ttf);
    if (oldValue != null) {
      while (!getIdxTermsMap().replace(idxTfMapKey, oldValue, oldValue + ttf)) {
        oldValue = getIdxTermsMap().get(idxTfMapKey);
      }
    }
  }

  /**
   * Get the cache manager object.
   *
   * @return Cache manager
   */
  Cache getCache() {
    return this.cache;
  }

  /**
   * Get the persistent storage manager for the static database.
   *
   * @return Persistence instance (static data)
   */
  Persistence getPersistStatic() {
    return this.persistStatic;
  }

  /**
   * Set the persistent storage manager for the static database.
   *
   * @param newPersistStatic Persistent storage manager for the static database
   */
  void setPersistStatic(final Persistence newPersistStatic) {
    this.persistStatic = newPersistStatic;
  }

  /**
   * Get the persistent storage manager for the transient database.
   *
   * @return Persistence instance (transient data)
   */
  Persistence getPersistTransient() {
    return this.persistTransient;
  }

  /**
   * Set the persistent storage manager for the transient database.
   *
   * @param newPersistTransient Persistent storage manager for the transient
   * database
   */
  void setPersistTransient(final Persistence newPersistTransient) {
    this.persistTransient = newPersistTransient;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>0</tt>).
   *
   * @param term Term to lookup
   * @return Document frequency of the given term
   */
  @Override
  public int getDocumentFrequency(final ByteArray term) {
    checkClosed();
    Objects.requireNonNull(term, "Term was null.");
    if (isStopword(term)) {
      // skip stop-words
      return 0;
    }

    final Integer freq = getIdxDfMap().get(term);
    if (freq == null) {
      return 0;
    }
    return freq;
  }

  @Override
  public void close() {
    LOG.debug("Closing instance.");

    if (this.isClosed()) {
      LOG.debug("Instance already closed.");
    } else {
      LOG.info("Closing static information storage.");
      if (this.persistStatic == null) {
        LOG.warn("Persistence static was null.");
      } else {
        if (this.cache == null) {
          LOG.warn("Cache was null.");
        } else {
          this.cache.commit(CacheDB.PERSISTENT);
        }
        this.persistStatic.closeDb();
      }

      LOG.info("Closing transient information storage.");
      if (this.persistTransient == null) {
        LOG.warn("Persistence transient was null.");
      } else {
        if (this.cache == null) {
          LOG.warn("Cache was null.");
        } else {
          this.cache.commit(CacheDB.TRANSIENT);
        }
        this.persistTransient.closeDb();
      }

      setClosed();
    }
  }

  /**
   * {@inheritDoc} Deleted documents will be removed from the list.
   *
   * @return Unique collection of all (non-deleted) document ids
   */
  @Override
  public Iterator<Integer> getDocumentIds() {
    if (getIdxDocumentIds().isEmpty()) {
      // rebuild doc-ids set from inverted doc-terms map
      LOG.info("Building document-id index.");
      final TimeMeasure tm = new TimeMeasure().start();
      for (Fun.Tuple3<Integer, SerializableByte, ByteArray> t3 :
          this.invertedIdxDocTermsMap) {
        getIdxDocumentIds().add(t3.a);
      }
      LOG.info("Document-id index built with {} entries.",
          getIdxDocumentIds().size());
      LOG.debug("Document-id index built. Took {}.",
          tm.stop().getTimeString());
    }
    return Collections.unmodifiableSet(getIdxDocumentIds()).iterator();
  }

  /**
   * {@inheritDoc} Stop-words will be skipped while creating the model.
   *
   * @param docId Document-id to create the model from
   * @return Model for the given document
   */
  @SuppressWarnings("ReturnOfNull")
  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkClosed();
    checkDocId(docId);
    final DocumentModel.Builder dmBuilder = new DocumentModel.Builder(docId);

    for (final String field : getDocumentFields()) {
      final SerializableByte fieldId = getFieldId(field);
      final Fun.Tuple3<Integer, SerializableByte, ByteArray> t3Low =
          Fun.t3(docId, fieldId, (ByteArray) null);
      final Fun.Tuple3<Integer, SerializableByte, ByteArray> t3Hi =
          Fun.t3(docId, fieldId, ByteArray.MAX);

      for (final Fun.Tuple3<Integer, SerializableByte, ByteArray> k :
          this.invertedIdxDocTermsMap.subSet(t3Low, true, t3Hi, true)) {
        if (!isStopword(k.c)) {
          dmBuilder.setOrAddTermFrequency(k.c,
              (long) getIdxDocTermsMap().get(Fun.t3(k.c, fieldId, docId)));
        }
      }
    }
    return dmBuilder.getModel();
  }

  /**
   * Checks, if a document with the given id is known (in index).
   *
   * @param docId Document-id to check.
   */
  private void checkDocId(final int docId) {
    if (!hasDocument(docId)) {
      throw new IllegalArgumentException("No document with id '" + docId
          + "' found.");
    }
  }

  /**
   * {@inheritDoc} Stop-words will be skipped.
   *
   * @param docIds List of document ids to extract terms from
   * @return List of terms from all documents, with stopwords excluded
   */
  @Override
  public Iterator<ByteArray> getDocumentsTermsSet(
      final Collection<Integer> docIds) {
    return getDocumentsTerms(docIds, true).keySet().iterator();
//    if (Objects.requireNonNull(docIds, "Document ids were null.").isEmpty()) {
//      throw new IllegalArgumentException("Document id list was empty.");
//    }
//
//    final Iterable<Integer> uniqueDocIds = new HashSet<>(docIds);
//    // collection can get quite large, so get a disk backed one
//    final Set<ByteArray> terms = DBMakerUtils.newCompressedTempFileDB().make()
//        .createTreeSet("tmpDocTermSet")
//        .serializer(ByteArray.SERIALIZER_BTREE)
//        .make();
//
//    for (final Integer docId : uniqueDocIds) {
//      checkDocId(docId);
//      for (final String field : getDocumentFields()) {
//        final SerializableByte fieldId = getFieldId(field);
//        final Fun.Tuple3<Integer, SerializableByte, ByteArray> t3Low =
//            Fun.t3(docId, fieldId, (ByteArray) null);
//        final Fun.Tuple3<Integer, SerializableByte, ByteArray> t3Hi =
//            Fun.t3(docId, fieldId, ByteArray.MAX);
//        for (final Fun.Tuple3<Integer, SerializableByte, ByteArray> k :
//            this.invertedIdxDocTermsMap.subSet(t3Low, true, t3Hi, true)) {
//          if (!isStopword(k.c)) {
//            terms.add(k.c);
//          }
//        }
//      }
//    }
//    return terms.iterator();
  }

  @Override
  public Iterator<Map.Entry<ByteArray, Long>> getDocumentsTerms(
      final Collection<Integer> docIds) {
    if (Objects.requireNonNull(docIds, "Document ids were null.").isEmpty()) {
      throw new IllegalArgumentException("Document id list was empty.");
    }

    return getDocumentsTerms(docIds, false).entrySet().iterator();
  }

  private Map<ByteArray, Long> getDocumentsTerms(
      final Collection<Integer> docIds, final boolean asSet) {
    if (Objects.requireNonNull(docIds, "Document ids were null.").isEmpty()) {
      throw new IllegalArgumentException("Document id list was empty.");
    }

    final Long baseValue = 1L;

    // the final map can get quite large, so get a disk backed one
    final Map<ByteArray, Long> termsMap = DBMakerUtils.newCompressedTempFileDB()
        .make()
        .createTreeMap("tmpDocTermSet")
        .keySerializer(ByteArray.SERIALIZER_BTREE)
        .valueSerializer(Serializer.LONG)
        .make();

    final Iterable<Integer> uniqueDocIds = new HashSet<>(docIds);

    for (final Integer docId : uniqueDocIds) {
      checkDocId(docId);
      for (final String field : getDocumentFields()) {
        final SerializableByte fieldId = getFieldId(field);
        final Fun.Tuple3<Integer, SerializableByte, ByteArray> t3Low =
            Fun.t3(docId, fieldId, (ByteArray) null);
        final Fun.Tuple3<Integer, SerializableByte, ByteArray> t3Hi =
            Fun.t3(docId, fieldId, ByteArray.MAX);
        for (final Fun.Tuple3<Integer, SerializableByte, ByteArray> k :
            this.invertedIdxDocTermsMap.subSet(t3Low, true, t3Hi, true)) {
          if (!isStopword(k.c)) {
            if (termsMap.containsKey(k.c) && !asSet) {
              // only store updated values, if needed
              termsMap.put(k.c, termsMap.get(k.c) + 1L);
            } else {
              termsMap.put(k.c, baseValue); // 1L
            }
          }
        }
      }
    }
    return termsMap;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is always
   * <tt>false</tt>).
   *
   * @param documentId Id of the document to check
   * @param term Term to lookup
   * @return True, if it contains the term, false otherwise, or if term is a
   * stopword
   */
  @Override
  public boolean documentContains(final int documentId, final ByteArray term) {
    checkClosed();
    Objects.requireNonNull(term, "Term was null.");
    if (isStopword(term)) {
      // skip stop-words
      return false;
    }

    checkDocId(documentId);

    for (final String field : getDocumentFields()) {
      final Integer inDocFreq = getIdxDocTermsMap().get(Fun.t3(term,
          getFieldId(field), documentId));
      if (inDocFreq != null && inDocFreq > 0) {
        return true;
      }
    }

    return false;
  }

  /**
   * Database types used by this instance.
   */
  @SuppressWarnings("PackageVisibleInnerClass")
  enum CacheDB {
    /**
     * All used databases.
     */
    ALL,
    /**
     * Persistent data storage.
     */
    PERSISTENT,
    /**
     * Transient data storage.
     */
    TRANSIENT
  }

  /**
   * Builder for creating a new {@link DirectIndexDataProvider}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      extends AbstractIndexDataProviderBuilder<Builder> {

    /**
     * Builder used to create a proper caching backend.
     */
    @SuppressWarnings("PackageVisibleField")
    final Persistence.Builder persistenceBuilderTransient =
        new Persistence.Builder();
    /**
     * Do not re-open the persistent database read-only. Only meant for unit
     * testing.
     */
    @SuppressWarnings("PackageVisibleField")
    boolean noOpenReadOnly;

    TermFilter termFilter;

    /**
     * Default constructor.
     */
    public Builder() {
      super(IDENTIFIER);
    }

    /**
     * Stops the DataProvider from re-opening the database read-only after all
     * caches are verified. Meant for debugging only.
     *
     * @return Self reference
     */
    Builder noReadOnly() {
      this.noOpenReadOnly = true;
      return this;
    }

    public Builder termFilter(final TermFilter tf) {
      this.termFilter = tf;
      return this;
    }

    @Override
    protected Builder getThis() {
      return this;
    }

    @Override
    public void validate()
        throws ConfigurationException {
      super.validate();
      validatePersistenceBuilder();
    }

    @Override
    public DirectIndexDataProvider build()
        throws BuildableException {
      validate();

      // create transient cache db
      try {
        this.persistenceBuilderTransient
            .dataPath(FileUtils.getPath(this.dataPath));
      } catch (final IOException e) {
        throw new BuildException(e);
      }
      this.persistenceBuilderTransient.name(createCacheName(this.cacheName +
          "_transient"))
          .stopwords(this.stopwords)
          .documentFields(this.documentFields)
          .makeOrGet();

      try {
        return new DirectIndexDataProvider(this);
      } catch (final DataProviderException e) {
        throw new BuildException(e);
      }
    }
  }

  /**
   * DBMaker helpers to create storage objects on the database.
   */
  @SuppressWarnings({"PackageVisibleInnerClass", "PublicInnerClass"})
  public static final class CacheDbMakers {

    /**
     * Serializer to use for {@link #idxTermsMap}.
     */
    static final BTreeKeySerializer IDX_TERMSMAP_KEYSERIALIZER
        = new BTreeKeySerializer.Tuple2KeySerializer<>(
        SerializableByte.COMPARATOR,
        SerializableByte.SERIALIZER, ByteArray.SERIALIZER);

    /**
     * Serializer to use for {@link #idxDocTermsMap}.
     */
    static final BTreeKeySerializer IDX_DOCTERMSMAP_KEYSERIALIZER
        = new BTreeKeySerializer.Tuple3KeySerializer<>(
        ByteArray.COMPARATOR, SerializableByte.COMPARATOR,
        ByteArray.SERIALIZER, SerializableByte.SERIALIZER,
        Serializer.INTEGER);

    /**
     * Serializer to use for {@link
     * DirectIndexDataProvider#invertedIdxDocTermsMap}
     * - the inverted index to {@link #idxDocTermsMap}.
     */
    @SuppressWarnings({"ConstantNamingConvention", "unchecked"})
    static final BTreeKeySerializer IDX_INVERTED_DOCTERMSMAP_SERIALIZER
        = new BTreeKeySerializer.Tuple3KeySerializer<>(
        BTreeMap.COMPARABLE_COMPARATOR, SerializableByte.COMPARATOR,
        Serializer.INTEGER, SerializableByte.SERIALIZER, ByteArray.SERIALIZER);

    /**
     * Private empty constructor for utility class.
     */
    private CacheDbMakers() { // empty
    }

    /**
     * Get a maker for {@link #invertedIdxDocTermsMap}. Persistent.
     *
     * @param db Database reference
     * @return Maker for {@link #invertedIdxDocTermsMap}
     */
    public static DB.BTreeSetMaker idxTermsMapInvertedKeysMaker(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeSet(Stores.IDX_TERMS_MAP_INVERTED_KEYS.name())
          .serializer(IDX_INVERTED_DOCTERMSMAP_SERIALIZER)
          .counterEnable();
    }

    /**
     * Get a maker for {@link #getIdxDocumentIds()}. Persistent.
     *
     * @param db Database reference
     * @return Maker for {@link #getIdxDocumentIds()}
     */
    public static DB.BTreeSetMaker idxDocIdsMaker(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeSet(Stores.IDX_DOC_IDS.name())
          .serializer(BTreeKeySerializer.ZERO_OR_POSITIVE_INT)
          .counterEnable();
    }

    /**
     * Get a maker for {@link #idxTerms}. Transient.
     *
     * @param db Database reference
     * @return Maker for {@link #idxTerms}
     */
    public static DB.BTreeSetMaker idxTermsMaker(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeSet(Caches.IDX_TERMS.name())
          .serializer(ByteArray.SERIALIZER_BTREE)
          .nodeSize(32)
          .counterEnable();
    }

    /**
     * Get a maker for {@link #idxDfMap}. Transient.
     *
     * @param db Database reference
     * @return Maker for {@link #idxDfMap}
     */
    public static DB.BTreeMapMaker idxDfMapMaker(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeMap(Caches.IDX_DFMAP.name())
          .keySerializer(ByteArray.SERIALIZER_BTREE)
          .valueSerializer(Serializer.INTEGER)
          .nodeSize(18)
          .counterEnable();
    }

    /**
     * Get a maker for {@link #idxTermsMap}. Static.
     *
     * @param db Database reference
     * @return Maker for {@link #idxTermsMap}
     */
    public static DB.BTreeMapMaker idxTermsMapMkr(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeMap(Stores.IDX_TERMS_MAP.name())
          .keySerializer(IDX_TERMSMAP_KEYSERIALIZER)
          .valueSerializer(Serializer.LONG)
          .nodeSize(18)
          .counterEnable();
    }

    /**
     * Get a maker for {@link #idxTermsMap}. Static.
     *
     * @param db Database reference
     * @return Maker for {@link #idxTermsMap}
     */
    public static DB.BTreeMapMaker idxDocTermsMapMkr(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeMap(Stores.IDX_DOC_TERMS_MAP.name())
          .keySerializer(IDX_DOCTERMSMAP_KEYSERIALIZER)
          .valueSerializer(Serializer.INTEGER)
          .nodeSize(32);
    }

    /**
     * Get a maker for {@link #idxTermsMap}. Temporary map used for merging.
     * Static.
     *
     * @param db Database reference
     * @return Maker for {@link #idxTermsMap} temporary merge instance
     */
    static DB.BTreeMapMaker idxDocTermsMapMkr2(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeMap(Stores.IDX_DOC_TERMS_MAP2.name())
          .keySerializer(IDX_DOCTERMSMAP_KEYSERIALIZER)
          .valueSerializer(Serializer.INTEGER)
          .nodeSize(32);
    }

    /**
     * Get a maker for {@link #cachedFieldsMap}. Transient.
     *
     * @param db Database reference
     * @return Maker for {@link #cachedFieldsMap}
     */
    public static DB.BTreeMapMaker cachedFieldsMapMaker(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeMap(Stores.IDX_FIELDS.name())
          .valueSerializer(SerializableByte.SERIALIZER)
          .nodeSize(18)
          .counterEnable();
    }

    /**
     * Ids of flags being stored in the database. Those values are needed to
     * ensure data is consistent.
     */
    public enum Flags {
      /**
       * List of fields currently being indexed. Used to recover from crashes.
       */
      IDX_FIELDS_BEING_INDEXED,
      /**
       * Boolean flag indicating, if a document-frequency calculating process is
       * running.
       */
      IDX_DOC_FREQ_CALC_RUN,
      /**
       * Boolean flag indicating, if a term collecting process is running.
       */
      IDX_TERMS_COLLECTING_RUN
    }

    /**
     * Ids of persistent data held in the database.
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    public enum Stores {
      /**
       * List of document ids.
       */
      IDX_DOC_IDS,
      /**
       * Mapping of all document terms.
       */
      IDX_DOC_TERMS_MAP,
      /**
       * Same as {@link #IDX_DOC_TERMS_MAP}, temporary instance used for merging
       * maps
       */
      IDX_DOC_TERMS_MAP2,
      /**
       * Cached fields mapping.
       */
      IDX_FIELDS,
      /**
       * Mapping of all index terms.
       */
      IDX_TERMS_MAP,
      /**
       * Inverted keys to acces {@link #IDX_TERMS_MAP} entries.
       */
      IDX_TERMS_MAP_INVERTED_KEYS
    }

    /**
     * Ids of temporary data caches held in the database.
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    public enum Caches {
      /**
       * Filter used for selecting terms.
       */
      TERM_FILTER,
      /**
       * Set of all terms.
       */
      IDX_TERMS,
      /**
       * Document term-frequency map.
       */
      IDX_DFMAP,
      /**
       * Overall term frequency.
       */
      IDX_TF
    }
  }

  /**
   * Utility class managing disk based caches for this DataProvider.
   */
  private final class Cache {
    /**
     * True, if the static database is all new.
     */
    private boolean newStaticDb;
    /**
     * True, if the transient database is all new.
     */
    private boolean newTransientDb;
    /**
     * If true, the whole index needs to be rebuild. This happens, if the commit
     * generation has changed or any essential index value is missing.
     */
    private boolean flagCacheRebuild;
    /**
     * True, if the list of document fields have changed and needs to be
     * re-indexed.
     */
    private boolean flagFieldUpdate;
    /**
     * True, if there are incompletely indexed document fields.
     */
    private boolean flagFieldInvalid;
    /**
     * True, if the list of stopwords has changed.
     */
    private boolean flagStopwordsChanged;
    /**
     * True, if the inverted index needs to be rebuild.
     */
    private boolean flagRebuildInvertedIndex;
    /**
     * Termfilter in use.
     *
     * @see {@link #setTermFilter(TermFilter)}
     */
    private Atomic.Var<TermFilter> termFilter;

    /**
     * Initializes the cache from the supplied builder settings.
     *
     * @param builder Builder to get settings from
     * @throws Buildable.BuildException Thrown, if the {@link Persistence}
     * storage could not be initialized
     * @throws Buildable.ConfigurationException Thrown, if the {@link
     * Persistence} storage could not be initialized
     * @throws IOException Thrown on low-level I/O errors
     * @throws ProcessingException Thrown, if field indexing fails
     */
    Cache(final Builder builder)
        throws Buildable.BuildException, Buildable.ConfigurationException,
               IOException, ProcessingException {
      openStaticDb(builder.persistenceBuilder);
      openTransientDb(builder.persistenceBuilderTransient);

      if (!this.newStaticDb) {
        checkStaticDbState();
        if (this.flagCacheRebuild) {
          clearPersistentCaches();
        }
      }

      loadPersistentDb();

      if (this.newStaticDb) {
        checkStaticDbState();
      } else if (this.flagFieldInvalid) {
        removeIncompletelyIndexedFields();
      }

      if (this.flagFieldUpdate
          || this.flagFieldInvalid
          || this.flagCacheRebuild
          || this.newStaticDb) {
        indexFields();
        this.flagRebuildInvertedIndex = true;
      }

      if (this.flagRebuildInvertedIndex) {
        LOG.info("Building inverted document terms index with {} entries.",
            NumberFormat.getIntegerInstance()
                .format((long) getIdxDocTermsMap().size()));
        final TimeMeasure tm = new TimeMeasure().start();

        if (getPersistStatic().getDb().exists(
            CacheDbMakers.Stores.IDX_TERMS_MAP_INVERTED_KEYS.name())) {
          getPersistStatic().getDb()
              .delete(CacheDbMakers.Stores.IDX_TERMS_MAP_INVERTED_KEYS.name());
        }

        /**
         * Function 'translating' the key of {@link getIdxDocTermsMap()} to
         * an inverted form from Tuple3(ByteArray, SerializableByte,
         * Integer) to Tuple3(Integer, SerializableByte,
         * ByteArray)
         */
        final class Func1
            implements Fun.Function1<
            Fun.Tuple3<Integer, SerializableByte, ByteArray>,
            Fun.Tuple3<ByteArray, SerializableByte, Integer>> {

          @Override
          public Fun.Tuple3<Integer, SerializableByte, ByteArray> run(
              final Fun.Tuple3<ByteArray, SerializableByte, Integer> t3) {
            return Fun.t3(t3.c, t3.b, t3.a);
          }
        }

        /**
         * Iterator over the keySet from {@link getIdxDocTermsMap()} - in
         * reverse order. Order is according to the results from {@link Func1}.
         */
        final Iterator<Fun.Tuple3<ByteArray, SerializableByte,
            Integer>> invertedIt = Pump.sort(
            getIdxDocTermsMap().keySet().iterator(),
            false, 100000,
            Collections.reverseOrder(
                new Comparator<Fun.Tuple3<ByteArray, SerializableByte,
                    Integer>>() {
                  @Override
                  public int compare(
                      final Fun.Tuple3<ByteArray, SerializableByte, Integer> o1,
                      final Fun.Tuple3<ByteArray, SerializableByte,
                          Integer> o2) {
                    int result = o1.c.compareTo(o2.c);
                    if (result == 0) {
                      result = o1.b.compareTo(o2.b);
                      if (result == 0) {
                        result = o1.a.compareTo(o2.a);
                      }
                    }
                    return result;
                  }
                }),
            new TupleSerializer.Tuple3Serializer(ByteArray.SERIALIZER,
                SerializableByte.SERIALIZER, Serializer.INTEGER)
//            new Serializer<Fun.Tuple3<ByteArray, SerializableByte,
// Integer>>() {
//
//              @Override
//              public void serialize(final DataOutput out,
//                  final Fun.Tuple3<ByteArray, SerializableByte,
// Integer> value)
//                  throws IOException {
//                ByteArray.SERIALIZER.serialize(out, value.a);
//                SerializableByte.SERIALIZER.serialize(out, value.b);
//                Serializer.INTEGER.serialize(out, value.c);
//              }
//
//              @Override
//              public Fun.Tuple3<ByteArray, SerializableByte,
//                  Integer> deserialize(
//                  final DataInput in, final int available)
//                  throws IOException {
//                return Fun.t3(
//                    ByteArray.SERIALIZER.deserialize(in, available),
//                    SerializableByte.SERIALIZER.deserialize(in, available),
//                    Serializer.INTEGER.deserialize(in, available));
//              }
//
//              @Override
//              public int fixedSize() {
//                return -1;
//              }
//            }
        );
        final int nodeSize = 32;
        final long counterRecId = getPersistStatic().getDb().getEngine().put
            (0L, Serializer.LONG);

        @SuppressWarnings("unchecked")
        final long rootRecidRef = Pump.buildTreeMap(
            invertedIt, // pre-sorted values matching translation results
            getPersistStatic().getDb().getEngine(),
            new Func1(), // translate keys
            null, // no values = set
            false, // ignore dupes
            nodeSize, // node size
            false, // values outside nodes
            counterRecId, // counter record ref
            CacheDbMakers.IDX_INVERTED_DOCTERMSMAP_SERIALIZER, // serializer
            null, // value serializer (null for set)
            Fun.TUPLE3_COMPARATOR // comparator
        );
        // FIXME: manual update db catalog - any better way to to this?
        getPersistStatic().getDb().getCatalog().put( // serializer
            CacheDbMakers.Stores.IDX_TERMS_MAP_INVERTED_KEYS.name()
                + ".keySerializer",
            CacheDbMakers.IDX_INVERTED_DOCTERMSMAP_SERIALIZER);
        getPersistStatic().getDb().getCatalog().put( // comparator
            CacheDbMakers.Stores.IDX_TERMS_MAP_INVERTED_KEYS.name()
                + ".comparator", BTreeMap.COMPARABLE_COMPARATOR);
        getPersistStatic().getDb().getCatalog().put( // record ref
            CacheDbMakers.Stores.IDX_TERMS_MAP_INVERTED_KEYS.name()
                + ".rootRecidRef", rootRecidRef);
        getPersistStatic().getDb().getCatalog().put( // counter ref
            CacheDbMakers.Stores.IDX_TERMS_MAP_INVERTED_KEYS.name()
                + ".counterRecid", counterRecId);
        getPersistStatic().getDb().getCatalog().put( // node metas
            CacheDbMakers.Stores.IDX_TERMS_MAP_INVERTED_KEYS.name()
                + ".numberOfNodeMetas", 0);
        getPersistStatic().getDb().getCatalog().put( // type
            CacheDbMakers.Stores.IDX_TERMS_MAP_INVERTED_KEYS.name()
                + ".type", "TreeSet");

        commit(CacheDB.PERSISTENT);

        DirectIndexDataProvider.this.invertedIdxDocTermsMap = CacheDbMakers
            .idxTermsMapInvertedKeysMaker(getPersistStatic().getDb())
            .makeOrGet();

//
// https://groups.google.com/forum/#!topic/mapdb/yXs6Sw5nXVs
//
//        DirectIndexDataProvider.this.invertedIdxDocTermsMap = CacheDbMakers
//            .idxTermsMapInvertedKeysMaker(getPersistStatic().getDb())
//            .comparator(
//                new Comparator<Fun.Tuple3<ByteArray, SerializableByte,
//                                    Integer>>() {
//                  @Override
//                  public int compare(
//                      final Fun.Tuple3<ByteArray, SerializableByte,
// Integer> o1,
//                      final Fun.Tuple3<ByteArray, SerializableByte,
//                          Integer> o2) {
//                    int result = o1.c.compareTo(o2.c);
//                    if (result == 0) {
//                      result = o1.b.compareTo(o2.b);
//                      if (result == 0) {
//                        result = o1.a.compareTo(o2.a);
//                      }
//                    }
//                    return result;
//                  }
//                })
//            .pumpPresort((int)1e6)
//            .pumpSource(getIdxDocTermsMap().keySet().iterator())
//            .make();

        LOG.info("Inverted document terms index built. ({})",
            tm.stop().getTimeString());
      }

      LOG.debug("Inverted document terms index size={}.",
          NumberFormat.getIntegerInstance().format(
              (long) DirectIndexDataProvider.this.invertedIdxDocTermsMap
                  .size()));

      // use inverted index to get document ids, if ids are empty

//      if (!builder.noOpenReadOnly) {
//        LOG.info("Re-opening persistent database read-only.");
//        commit(CacheDB.PERSISTENT);
//        getPersistStatic().closeDb();
//        builder.persistenceBuilder.readOnly();
//        builder.persistenceBuilder.get();
//        openStaticDb(builder.persistenceBuilder);
//        loadPersistentDb();
//      }

      checkTransientDbState(builder);
      if (this.newTransientDb
          || this.flagStopwordsChanged
          || this.flagCacheRebuild
          || this.flagFieldUpdate
          || this.flagFieldInvalid) {
        clearTransientCaches();
        getPersistTransient().updateMetaData(getDocumentFields(),
            getStopwords());
        commit(CacheDB.ALL);
      }
      loadTransientDb();
    }

    /**
     * Opens or creates the main database.
     *
     * @param psb Database builder for static database
     * @throws Buildable.BuildException Thrown, if building the database manager
     * failed
     * @throws Buildable.ConfigurationException Thrown, if the database builder
     * was configured incorrectly
     */
    private void openStaticDb(final Persistence.Builder psb)
        throws Buildable.BuildException, Buildable.ConfigurationException {
      switch (psb.getCacheLoadInstruction()) {
        case MAKE:
          LOG.info("Creating new static database.");
          setPersistStatic(psb.make().build());
          this.newStaticDb = true;
          break;
        case GET:
          LOG.info("Opening static database.");
          setPersistStatic(psb.get().build());
          break;
        default: // make or get
          if (psb.dbExists()) {
            LOG.info("Creating new static database.");
          } else {
            // make
            LOG.info("Opening static database.");
            this.newStaticDb = true;
          }
          setPersistStatic(psb.makeOrGet().build());
          break;
      }
    }

    /**
     * Opens or creates the secondary database.
     *
     * @param psb Database builder for static database
     * @throws Buildable.BuildException Thrown, if building the database manager
     * failed
     * @throws Buildable.ConfigurationException Thrown, if the database builder
     * was configured incorrectly
     */
    private void openTransientDb(final Persistence.Builder psb)
        throws Buildable.ConfigurationException, Buildable.BuildException {
      LOG.info("Opening transient database.");
      switch (psb.getCacheLoadInstruction()) {
        case MAKE:
          LOG.info("Creating new transient database.");
          setPersistTransient(psb.make().build());
          this.newTransientDb = true;
          break;
        case GET:
          LOG.info("Opening transient database.");
          setPersistTransient(psb.get().build());
          break;
        default: // make or get
          if (psb.dbExists()) {
            LOG.info("Creating new transient database.");
          } else {
            // make
            LOG.info("Opening transient database.");
            this.newTransientDb = true;
          }
          setPersistTransient(psb.makeOrGet().build());
          break;
      }
    }

    /**
     * Check the state of the static database. The appropriate flags are set, if
     * some required data is missing.
     */
    private void checkStaticDbState() {
      LOG.info("Checking static database state.");
      // check, if index has changed since last caching
      if (getLastIndexCommitGeneration() == null ||
          !getPersistStatic().getMetaData().hasGenerationValue()) {
        LOG.warn("Index commit generation not available. Assuming an " +
            "unchanged index!");
      } else {
        if (!getPersistStatic().getMetaData()
            .isGenerationCurrent(getLastIndexCommitGeneration())) {
          LOG.warn("Invalid database state. " +
              "Index changed since last caching. " +
              "Database needs to be rebuild.");
          this.flagCacheRebuild = true;
        }
      }

      if (this.flagCacheRebuild) {
        return; // no more checks needed, we have to rebuild from scratch
      }

      // check all required caches
      for (final CacheDbMakers.Stores store : CacheDbMakers.Stores.values()) {
        if (!getPersistStatic().getDb().exists(store.name())) {
          this.flagCacheRebuild = true;
          switch (store) {
            case IDX_FIELDS:
              LOG.warn(
                  "Invalid database state. Cached fields list not found. " +
                      "Database needs to be rebuild.");
              break;
            case IDX_DOC_TERMS_MAP:
              LOG.warn(
                  "Invalid database state. Document terms map not found. " +
                      "Database needs to be rebuild.");
              break;
            case IDX_TERMS_MAP:
              LOG.warn("Invalid database state. Index terms map not found. " +
                  "Database needs to be rebuild.");
              break;
            case IDX_DOC_IDS:
              LOG.warn("Invalid database state. Document id list not found. " +
                  "Database needs to be rebuild.");
              break;
            default:
              // no rebuild needed, if unhandled item is missing
              this.flagCacheRebuild = false;
              break;
          }
        }
      }

      if (this.flagCacheRebuild) {
        return; // no more checks needed, we have to rebuild from scratch
      }

      // checks, if any fields are incompletely indexed
      if (getPersistStatic().getDb().exists(
          CacheDbMakers.Flags.IDX_FIELDS_BEING_INDEXED.name())) {
        LOG.warn("Invalid database state. Found incompletely indexed fields. " +
            "Those fields will be removed and will be re-indexed.");
        this.flagFieldInvalid = true;
      }

      // check, if fields have changed
//      if (!this.flagFieldInvalid && !getPersistStatic().getMetaData()
//          .areFieldsCurrent(getDocumentFields())) {
//        LOG.info("Fields changed. Caches needs to be rebuild.");
//        this.flagFieldUpdate = true;
//      }
    }

    /**
     * Clears the persistent caches.
     */
    private void clearPersistentCaches() {
      LOG.warn("Clearing persistent caches.");
      for (final CacheDbMakers.Stores store : CacheDbMakers.Stores.values()) {
        if (getPersistStatic().getDb().exists(store.name())) {
          getPersistStatic().getDb().delete(store.name());
        }
        switch (store) {
          case IDX_TERMS_MAP:
            CacheDbMakers.idxTermsMapMkr(getPersistStatic().getDb()).make();
            break;
          case IDX_DOC_TERMS_MAP:
            CacheDbMakers.idxDocTermsMapMkr(getPersistStatic().getDb())
                .counterEnable()
                .make();
            break;
          case IDX_FIELDS:
            CacheDbMakers.cachedFieldsMapMaker(getPersistStatic().getDb())
                .make();
            break;
        }
      }
    }

    /**
     * Tries to load cached values from the database. Creates the appropriate
     * storage objects on demand, if they do not already exist.
     */
    private void loadPersistentDb() {
      LOG.info("Loading persistent database.");

      // load mapping of cached fields
      if (LOG.isDebugEnabled()) {
        LOG.debug("hasCachedFieldsMap: {}", getPersistStatic().getDb().exists
            (CacheDbMakers.Stores.IDX_FIELDS.name()));
      }
      setCachedFieldsMap(CacheDbMakers.cachedFieldsMapMaker(
          getPersistStatic().getDb()).<String, SerializableByte>makeOrGet());
      // check, if some fields are missing in persistent cache
      if (!getCachedFieldsMap().keySet().containsAll(getDocumentFields())) {
        this.flagFieldUpdate = true;
      }

      // load document-term map
      if (LOG.isDebugEnabled()) {
        LOG.debug("hasIdxDocTermsMap: {}", getPersistStatic().getDb().exists
            (CacheDbMakers.Stores.IDX_DOC_TERMS_MAP.name()));
      }
      setIdxDocTermsMap(CacheDbMakers.idxDocTermsMapMkr(
          getPersistStatic().getDb())
          .counterEnable()
          .<Fun.Tuple3<ByteArray, SerializableByte, Integer>,
              Integer>makeOrGet());

      // load document-id list
      if (LOG.isDebugEnabled()) {
        LOG.debug("hasIdxDocIdList: {}", getPersistStatic().getDb().exists
            (CacheDbMakers.Stores.IDX_DOC_IDS.name()));
      }
      setIdxDocumentIds(CacheDbMakers.idxDocIdsMaker(
          getPersistStatic().getDb())
          .<Integer>makeOrGet());

      // load index term-map
      if (LOG.isDebugEnabled()) {
        LOG.debug("hasIdxTermsMap: {}", getPersistStatic().getDb().exists
            (CacheDbMakers.Stores.IDX_TERMS_MAP.name()));
      }
      setIdxTermsMap(CacheDbMakers.idxTermsMapMkr(
          getPersistStatic().getDb())
          .<Fun.Tuple2<SerializableByte, ByteArray>, Long>makeOrGet());

      // load/rebuild inverted index document term-map keys index
      this.flagRebuildInvertedIndex = !getPersistStatic().getDb().exists(
          CacheDbMakers.Stores.IDX_TERMS_MAP_INVERTED_KEYS.name());
      if (this.flagRebuildInvertedIndex) {
        LOG.debug("hasInvertedIdxDocTermsMap: false");
        LOG.info("Inverted document terms index needs to be build.");
      } else {
        LOG.debug("hasInvertedIdxDocTermsMap: true");
        DirectIndexDataProvider.this.invertedIdxDocTermsMap = CacheDbMakers
            .idxTermsMapInvertedKeysMaker(getPersistStatic().getDb())
            .makeOrGet();
      }
    }

    /**
     * Will remove any field data that is flagged as being incomplete (due to
     * crashes, interruption, etc.). <br> Cached data have to been loaded
     * already to allow modifications.
     */
    private void removeIncompletelyIndexedFields() {
      LOG.info("Removing incompletely indexed fields.");
      final Set<String> flaggedFields =
          getPersistTransient().getDb().createHashSet(
              CacheDbMakers.Flags.IDX_FIELDS_BEING_INDEXED.name()).makeOrGet();

      if (flaggedFields.isEmpty()) {
        return;
      }

      for (final String field : flaggedFields) {
        final SerializableByte fieldId = getFieldId(field);

        // remove all field contents from persistent index terms map
        final Iterator<ByteArray> idxTMapIt =
            Fun.filter(getIdxTermsMap().keySet(), fieldId).iterator();
        while (idxTMapIt.hasNext()) {
          idxTMapIt.next();
          idxTMapIt.remove();
        }

        // remove all field contents from persistent document terms map
        for (final ByteArray term : getIdxTerms()) {
          final Iterator<Integer> docTMapIt =
              Fun.filter(getIdxDocTermsMap().keySet(), term, fieldId)
                  .iterator();
          while (docTMapIt.hasNext()) {
            docTMapIt.next();
            docTMapIt.remove();
          }
        }
      }
      LOG.warn("Incompletely index fields found ({}). " +
              "This fields have been removed from cache. " +
              "Cache rebuilding is needed.",
          flaggedFields);
      commit(CacheDB.ALL);
    }

    /**
     * Build a cache of the current index.
     *
     * @throws ProcessingException Thrown, if any parallel data processing
     * method is failing
     * @throws IOException Thrown on low-level I/O errors
     */
    @SuppressWarnings(
        {"ObjectAllocationInLoop", "CollectionWithoutInitialCapacity"})
    private void indexFields()
        throws ProcessingException, IOException {
      final Set<String> updatingFields = new HashSet<>(getDocumentFields());

      updatingFields.removeAll(getCachedFieldsMap().keySet());

      // check, if there's anything to update
      if (updatingFields.isEmpty()) {
        return;
      }

      LOG.info("Building persistent index term cache. {}", updatingFields);

      // pre-check, if field has the information we need
      final FieldInfos fieldInfos = MultiFields.getMergedFieldInfos
          (getIndexReader());
      for (final String field : getDocumentFields()) {
        if (FieldInfo.IndexOptions.DOCS_ONLY == fieldInfos.fieldInfo(field)
            .getIndexOptions()) {
          throw new IllegalStateException(
              "Field '" + field + "' indexed with " +
                  "DOCS_ONLY option. No term frequencies and position " +
                  "information available.");
        }
      }

      // generate a field-id
      for (final String field : updatingFields) {
        addFieldToCacheMap(field);
      }

      // Store fields being update to database. The list will be emptied after
      // indexing. This will be used to identify incomplete indexed fields
      // caused by interruptions.
      final Set<String> flaggedFields =
          getPersistStatic().getDb()
              .createHashSet(
                  CacheDbMakers.Flags.IDX_FIELDS_BEING_INDEXED.name())
              .make();
      flaggedFields.addAll(updatingFields);
      commit(CacheDB.ALL); // store before processing

      final List<AtomicReaderContext> arContexts =
          getIndexReader().getContext().leaves();

      // collect terms in memory first and commit to disk later
      // this greatly improves performance
      final DB cacheDB = DBMakerUtils.newCompressedMemoryDirectDB().make();
      final BTreeMap<Fun.Tuple3<
          ByteArray, SerializableByte, Integer>, Integer> cacheMap =
          CacheDbMakers.idxDocTermsMapMkr(cacheDB).make();

      final Processing processing = new Processing();
      if (arContexts.size() == 1) {
        LOG.debug("Build strategy: concurrent field(s)");

        final AtomicReader reader = arContexts.get(0).reader();
        Terms terms;
        TermsEnum termsEnum = TermsEnum.EMPTY;
        BytesRef term;

        for (final String field : updatingFields) {
          final Set<ByteArray> termSet = new HashSet<>();
          terms = reader.terms(field);
          if (terms == null) {
            LOG.warn("No terms. field={}", field);
          } else {
            termsEnum = terms.iterator(termsEnum);
            term = termsEnum.next();
            while (term != null) {
              if (termsEnum.seekExact(term)) {
                try {
                  termSet.add(BytesRefUtils.toByteArray(term));
                } catch (final IllegalStateException e) {
                  LOG.error("Error collecting term. " +
                          "field={} bytes={} offset={} length={} str={}",
                      field, term.bytes, term.offset, term.length,
                      term.utf8ToString(), e);
                }
              }
              term = termsEnum.next();
            }
          }
          assert !termSet.isEmpty();

          LOG.info("Building persistent index term cache. field={}", field);
          processing.setSourceAndTarget(
              new TargetFuncCall<>(
                  new CollectionSource<>(termSet),
                  new IndexFieldsTermsCollectorTarget(cacheMap, field)
              )
          ).process(termSet.size());
        }
      } else {
        LOG.debug("Build strategy: segments ({})", arContexts.size());
        // we have multiple index segments, process every segment separately
        processing.setSourceAndTarget(
            new TargetFuncCall<>(
                new CollectionSource<>(arContexts),
                new IndexSegmentTermsCollectorTarget(cacheMap, updatingFields)
            )
        ).process(arContexts.size());
      }

      LOG.info("Storing {} items.",
          NumberFormat.getIntegerInstance().format((long) cacheMap.size()));
      final TimeMeasure tm = new TimeMeasure().start();
      if (getIdxDocTermsMap().isEmpty()) {
        LOG.debug("Streaming results using pump.");

        // delete old map, will be re-created using pump
        setIdxDocTermsMap(null);
        getPersistStatic().getDb().delete(
            CacheDbMakers.Stores.IDX_DOC_TERMS_MAP.name());
        setIdxDocTermsMap(
            CacheDbMakers.idxDocTermsMapMkr(getPersistStatic().getDb())
                .counterEnable()
                .keySerializer(CacheDbMakers.IDX_DOCTERMSMAP_KEYSERIALIZER)
                .pumpSource(
                    cacheMap.descendingKeySet().iterator(),
                    new Fun.Function1<Integer, Fun.Tuple3<
                        ByteArray, SerializableByte, Integer>>() {
                      @Override
                      public Integer run(
                          final Fun.Tuple3<
                              ByteArray, SerializableByte, Integer> t3) {
                        return cacheMap.get(t3);
                      }
                    })
                .<Fun.Tuple3<ByteArray, SerializableByte, Integer>,
                    Integer>make()
        );
      } else {
        LOG.debug("Merging results using pump.");
        // delete any existing temporary merge map
        if (getPersistStatic().getDb().exists(
            CacheDbMakers.Stores.IDX_DOC_TERMS_MAP2.name())) {
          getPersistStatic().getDb().delete(
              CacheDbMakers.Stores.IDX_DOC_TERMS_MAP2.name());
        }

        /**
         * Function extracting the value from the two document-term maps {@link
         * cacheMap} and {@link getIdxDocTermsMap()} by a given key.
         */
        final class Func1
            implements Fun.Function1<Integer, Fun.Tuple3<
            ByteArray, SerializableByte, Integer>> {

          @Override
          public Integer run(
              final Fun.Tuple3<ByteArray, SerializableByte, Integer> t3) {
            final Integer val = cacheMap.get(t3);
            if (val == null) {
              return getIdxDocTermsMap().get(t3);
            } else {
              if (!val.equals(getIdxDocTermsMap().get(t3))) {
                throw new IllegalStateException("Value clash! " +
                    "Got different values for document term frequency: " +
                    "cacheMap=" + val + " db=" + getIdxDocTermsMap().get(t3));
              }
              return val;
            }
          }
        }

        // create temporary merged map
        setIdxDocTermsMap(
            CacheDbMakers.idxDocTermsMapMkr2(
                getPersistStatic().getDb())
                .counterEnable()
                .pumpSource(
                    Pump.<Fun.Tuple3<ByteArray, SerializableByte,
                        Integer>>merge(
                        getIdxDocTermsMap().descendingKeySet().iterator(),
                        cacheMap.descendingKeySet().iterator()),
                    new Func1())
                .<Fun.Tuple3<ByteArray, SerializableByte, Integer>,
                    Integer>make()
        );

        commit(CacheDB.PERSISTENT);
        // delete old map
        getPersistStatic().getDb()
            .delete(CacheDbMakers.Stores.IDX_DOC_TERMS_MAP.name());
        // rename merged to new name
        getPersistStatic().getDb().rename(
            CacheDbMakers.Stores.IDX_DOC_TERMS_MAP2.name(),
            CacheDbMakers.Stores.IDX_DOC_TERMS_MAP.name()
        );
      }
      cacheDB.close();
      LOG.info("Storing results finished after {}.", tm.stop().getTimeString());

      // all fields successful updated
      getPersistStatic().getDb()
          .delete(CacheDbMakers.Flags.IDX_FIELDS_BEING_INDEXED.name());

      commit(CacheDB.ALL);
    }

    /**
     * Try to commit the database and optionally run compaction. Commits will
     * only be done, if transaction is supported.
     *
     * @param db Database type to commit
     */
    final synchronized void commit(final CacheDB db) {
//      final TimeMeasure tm = new TimeMeasure().start();

      // TODO: compaction is currently not supported

      switch (db) {
        case ALL:
          LOG.info("Updating all storages.");
          try {
            getPersistStatic().getDb().commit();
            getPersistTransient().getDb().commit();
          } catch (final Throwable t) {
            LOG.error("Commit error!", t);
          }
          break;
        case PERSISTENT:
          LOG.info("Updating static storage.");
          try {
            getPersistStatic().getDb().commit();
          } catch (final Throwable t) {
            LOG.error("Commit error!", t);
          }
          break;
        case TRANSIENT:
          LOG.info("Updating transient storage.");
          try {
            getPersistTransient().getDb().commit();
          } catch (final Throwable t) {
            LOG.error("Commit error!", t);
          }
          break;
      }
    }

    /**
     * Check for incomplete indexing tasks and if stopwords have changed. If
     * incomplete tasks are found those storage objects are removed.
     */
    private void checkTransientDbState(final Builder builder) {
      LOG.info("Checking transient database state.");

      // check filter
      // filter
      if (!getPersistTransient().getDb().exists(CacheDbMakers.Caches
          .TERM_FILTER.name())) {
        getPersistTransient().getDb().createAtomicVar(
            CacheDbMakers.Caches.TERM_FILTER.name(),
            null, Serializer.JAVA);
      }
      this.termFilter = getPersistTransient().getDb()
          .getAtomicVar(CacheDbMakers.Caches.TERM_FILTER.name());


      final boolean hasDbFilter = this.termFilter.get() != null;
      final boolean hasBuilderFilter = builder.termFilter != null;

      if (hasDbFilter) {
        if (hasBuilderFilter) {
          // filter in db and builder
          if (!getTermFilter().equals(
              builder.termFilter)) {
            this.termFilter.set(builder.termFilter);
            LOG.warn("Term filter changed. Caches needs to be rebuild.");
            this.flagCacheRebuild = true;
          }
        } else {
          // filter in db, not in builder
          LOG.warn("Index built using filter, but no filter is specified. " +
              "Caches needs to be rebuild.");
          this.termFilter.set(null);
          this.flagCacheRebuild = true;
        }
      } else if (hasBuilderFilter) {
        // filter not in db, but in builder
        LOG.warn("A new term filter is specified. " +
            "Caches needs to be rebuild.");
        this.termFilter.set(builder.termFilter);
        this.flagCacheRebuild = true;
      }
      setTermFilter(this.termFilter.get());

      // check flags
      for (final CacheDbMakers.Flags flag : CacheDbMakers.Flags.values()) {
        if (getPersistTransient().getDb()
            .exists(flag.name())) {
          switch (flag) {
            case IDX_TERMS_COLLECTING_RUN:
              LOG.warn("Found incomplete terms index. " +
                  "This index will be deleted and rebuilt on warm-up.");
              getPersistTransient().getDb()
                  .delete(CacheDbMakers.Flags.IDX_TERMS_COLLECTING_RUN.name());
              getPersistTransient().getDb()
                  .delete(CacheDbMakers.Caches.IDX_TERMS.name());
              break;
            case IDX_DOC_FREQ_CALC_RUN:
              LOG.warn("Found incomplete document-frequency map. " +
                  "This map will be deleted and rebuilt on warm-up.");
              getPersistTransient().getDb()
                  .delete(CacheDbMakers.Flags.IDX_DOC_FREQ_CALC_RUN.name());
              getPersistTransient().getDb()
                  .delete(CacheDbMakers.Caches.IDX_DFMAP.name());
              break;
          }
        }
      }

      // check, if stopwords have changed
      if (getPersistTransient()
          .getMetaData().areStopwordsCurrent(getStopwords())) {
        LOG.info("Stopwords ({}) unchanged.",
            NumberFormat.getIntegerInstance().format(
                (long) getStopwords().size()));
      } else {
        LOG.info("Stopwords changed. Caches needs to be rebuild.");
        this.flagStopwordsChanged = true;
      }
    }

    /**
     * Clears all transient caches.
     */
    private void clearTransientCaches() {
      LOG.info("Clearing temporary caches.");
      // index terms cache (content depends on current fields & stopwords)
      if (getPersistTransient().getDb().exists(
          CacheDbMakers.Caches.IDX_TERMS.name())) {
        getPersistTransient().getDb().delete(
            CacheDbMakers.Caches.IDX_TERMS.name());
      }

      // document term-frequency map (content depends on current fields)
      if (getPersistTransient().getDb().exists(
          CacheDbMakers.Caches.IDX_DFMAP.name())) {
        getPersistTransient().getDb().delete(
            CacheDbMakers.Caches.IDX_DFMAP.name());
      }

      // clear index term frequency value
      clearIdxTf();
    }

    /**
     * Tries to load cached values from the database. Creates the appropriate
     * storage objects on demand, if they do not already exist.
     */
    private void loadTransientDb() {
      LOG.info("Loading transient database.");
      // load terms index
      setIdxTerms(CacheDbMakers.idxTermsMaker(getPersistTransient().getDb())
          .<ByteArray>makeOrGet());
      final int idxTermsSize = getIdxTerms().size();
      if (idxTermsSize == 0) {
        LOG.info("Index terms cache is empty. Will be rebuild on warm-up.");
      } else {
        LOG.info("Loaded index terms cache with {} entries.",
            NumberFormat.getIntegerInstance().format((long) idxTermsSize));
      }

      // try load overall term frequency
      if (getPersistTransient().getDb()
          .exists(CacheDbMakers.Caches.IDX_TF.name())) {
        setIdxTf(getPersistTransient().getDb()
            .getAtomicLong(CacheDbMakers.Caches.IDX_TF.name()).get());
        LOG.info("Term frequency value loaded ({}).",
            NumberFormat.getNumberInstance().format(getIdxTf()));
      } else {
        LOG.info("No term frequency value found. " +
            "Will be re-calculated on warm-up.");
        clearIdxTf();
      }

      // try load document-frequency map
      setIdxDfMap(CacheDbMakers.idxDfMapMaker(
          getPersistTransient().getDb()).<ByteArray,
          Integer>makeOrGet());
      if (getIdxDfMap().isEmpty()) {
        LOG.info("Document-frequency cache is empty. " +
            "Will be rebuild on warm-up.");
      } else {
        LOG.info("Loaded document-frequency cache with {} entries.",
            NumberFormat.getIntegerInstance().format(
                (long) getIdxDfMap().size()));
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for collecting all terms from the current
   * Lucene index. Based on index fields. This should only be used, if only one
   * leave is available from the IndexReader. If there are more than one, only
   * the first will be used.
   */
  private final class IndexFieldsTermsCollectorTarget
      extends TargetFuncCall.TargetFuncFactory<ByteArray> {

    /**
     * Field to collect terms from.
     */
    private final String field;
    /**
     * AtomicReader for the single context.
     */
    private final AtomicReader reader;
    /**
     * DocBase value for the single context.
     */
    private final int docBase;
    /**
     * Id of the current field.
     */
    private final SerializableByte fieldId;
    /**
     * Live documents retrieved from reader.
     */
    private final Bits liveDocBits;
    /**
     * Map to cache results from all threads.
     */
    private final ConcurrentNavigableMap<Fun.Tuple3<
        ByteArray, SerializableByte, Integer>, Integer> cacheMap;
    /**
     * Local reusable terms enumerator instance.
     */
    private TermsEnum termsEnum = TermsEnum.EMPTY;
    /**
     * Local reusable document enumerator instance.
     */
    private DocsEnum docsEnum;

    /**
     * Create a new instance with the provided field as target.
     *
     * @param newCacheMap Target map to store results
     * @param fieldName Target field
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    IndexFieldsTermsCollectorTarget(
        final ConcurrentNavigableMap<Fun.Tuple3<
            ByteArray, SerializableByte, Integer>, Integer> newCacheMap,
        final String fieldName) {
      this.field = fieldName;
      this.reader = getIndexReader().getContext().leaves().get(0).reader();
      this.docBase = getIndexReader().getContext().leaves().get(0).docBase;
      this.fieldId = getFieldId(fieldName);
      this.liveDocBits = this.reader.getLiveDocs();
      this.cacheMap = newCacheMap;
    }

    /**
     * Private constructor to create new instances.
     *
     * @param theCacheMap Map to put results in
     * @param theField Current target field
     * @param theReader Current index reader
     * @param theDocBase Current docBase value
     * @param theFieldId Current field id
     * @param theLiveDocs Live docs from current reader
     * @throws IOException Thrown on low-level I/O errors
     */
    private IndexFieldsTermsCollectorTarget(
        final ConcurrentNavigableMap<Fun.Tuple3<
            ByteArray, SerializableByte, Integer>, Integer> theCacheMap,
        final String theField,
        final AtomicReader theReader,
        final int theDocBase,
        final SerializableByte theFieldId,
        final Bits theLiveDocs)
        throws IOException {
      this.field = theField;
      this.reader = theReader;
      this.docBase = theDocBase;
      this.fieldId = theFieldId;
      this.termsEnum = this.reader.terms(this.field).iterator(this.termsEnum);
      this.liveDocBits = theLiveDocs;
      this.cacheMap = theCacheMap;
    }

    @Override
    public IndexFieldsTermsCollectorTarget newInstance()
        throws TargetException {
      try {
        return new IndexFieldsTermsCollectorTarget(this.cacheMap, this.field,
            this.reader, this.docBase, this.fieldId, this.liveDocBits);
      } catch (final IOException e) {
        throw new TargetException("Failed to create a new instance.", e);
      }
    }

    @Override
    public void call(final ByteArray term)
        throws Exception {
      if (term != null) {
        if (this.termsEnum.seekExact(BytesRefUtils.refFromByteArray(term))) {
          this.docsEnum = this.termsEnum.docs(this.liveDocBits, this.docsEnum);
          collectTerms(this.cacheMap, term, this.fieldId, this.docsEnum,
              this.termsEnum.totalTermFreq(), this.docBase);
        }
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for collecting index terms on a per
   * segment basis. Each Lucene index segment will be accessed by a separate
   * {@link AtomicReader}.
   */
  private final class IndexSegmentTermsCollectorTarget
      extends TargetFuncCall.TargetFunc<AtomicReaderContext> {

    /**
     * List of fields to collect terms from.
     */
    private final Collection<String> fields;
    /**
     * Map to cache results.
     */
    private final ConcurrentNavigableMap<Fun.Tuple3<
        ByteArray, SerializableByte, Integer>, Integer> cacheMap;

    /**
     * Create a new collector for index terms.
     *
     * @param newCacheMap Map to store results
     * @param newFields Lucene index segment provider
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    IndexSegmentTermsCollectorTarget(
        final ConcurrentNavigableMap<Fun.Tuple3<
            ByteArray, SerializableByte, Integer>, Integer> newCacheMap,
        final Collection<String> newFields) {
      assert newFields != null && !newFields.isEmpty();
      this.fields = newFields;
      this.cacheMap = newCacheMap;
    }

    @Override
    public void call(final AtomicReaderContext rContext)
        throws IOException {
      if (rContext == null) {
        return;
      }

      TermsEnum termsEnum = TermsEnum.EMPTY;
      DocsEnum docsEnum = null;
      Terms terms;
      final int docBase = rContext.docBase;
      final AtomicReader reader = rContext.reader();
      BytesRef term;

      for (final String field : this.fields) {
        final SerializableByte fieldId = getFieldId(field);
        terms = reader.terms(field);

        if (terms == null) {
          LOG.warn("No terms. field={}", field);
        } else {
          termsEnum = terms.iterator(termsEnum);
          term = termsEnum.next();

          while (term != null) {
            if (termsEnum.seekExact(term)) {
              docsEnum = termsEnum.docs(reader.getLiveDocs(), docsEnum);
              collectTerms(this.cacheMap, BytesRefUtils.toByteArray(term),
                  fieldId, docsEnum, termsEnum.totalTermFreq(), docBase);
            }
            term = termsEnum.next();
          }
        }
      }
    }
  }
}
