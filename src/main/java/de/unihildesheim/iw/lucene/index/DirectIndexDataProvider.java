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
import de.unihildesheim.iw.util.FileUtils;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetException;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
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
import org.mapdb.BTreeKeySerializer;
import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
    extends AbstractIndexDataProvider {

  /**
   * Prefix used to store {@link GlobalConfiguration configuration} data.
   */
  static final String IDENTIFIER = "DirectIDP";

  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(DirectIndexDataProvider.class);
  /**
   * Data storage encapsulating class.
   */
  private final Cache cache;
  /**
   * Inverted keys for faster access to index document term frequency map.
   */
  NavigableSet<
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

  /**
   * {@inheritDoc} Deleted documents will be removed from the list.
   *
   * @return Unique collection of all (non-deleted) document ids
   */
  @Override
  protected Collection<Integer> getDocumentIds() {
    final Collection<Integer> ret;

    // cache initially, if needed
    if (getIdxDocumentIds() == null) {
      final int maxDoc = getIndexReader().maxDoc();
      setIdxDocumentIds(new HashSet<Integer>(maxDoc));

      final Bits liveDocs = MultiFields.getLiveDocs(getIndexReader());
      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs != null && !liveDocs.get(i)) {
          continue;
        }
        getIdxDocumentIds().add(i);
      }
      ret = Collections.unmodifiableCollection(getIdxDocumentIds());
    } else {
      ret = Collections.unmodifiableCollection(getIdxDocumentIds());
    }
    return ret;
  }

  /**
   * Shared method for {@link IndexSegmentTermsCollectorTarget} and {@link
   * IndexFieldsTermsCollectorTarget} collecting all terms from a {@link
   * DocsEnum}. The results are stored to both the index terms map and document
   * term-frequency map.
   *
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

    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Map<ByteArray, Long> tfMap = new HashMap<>();

    for (final String field : getDocumentFields()) {
      this.invertedIdxDocTermsMap.subSet(
          Fun.t3(docId, getFieldId(field), (ByteArray) null), true,
          Fun.t3(docId, getFieldId(field), ByteArray.MAX), true);
      for (final Fun.Tuple3<Integer, SerializableByte, ByteArray> k :
          this.invertedIdxDocTermsMap.subSet(
              Fun.t3(docId, getFieldId(field), (ByteArray) null), true,
              Fun.t3(docId, getFieldId(field), ByteArray.MAX), true)
          ) {
        if (!isStopword(k.c)) {
          if (tfMap.containsKey(k.c)) {
            tfMap.put(k.c, tfMap.get(k.c) +
                getIdxDocTermsMap().get(Fun.t3(k.c, getFieldId(field), docId)));
            //dftEnum.getTotalTermFreq());
          } else {
            tfMap.put(k.c, (long) getIdxDocTermsMap().get(
                Fun.t3(k.c, getFieldId(field), docId)));
          }
        }
      }
    }

    for (final Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
      dmBuilder.setTermFrequency(tfEntry.getKey(), tfEntry.getValue());
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
  public Set<ByteArray> getDocumentsTermSet(
      final Collection<Integer> docIds) {
    if (Objects.requireNonNull(docIds, "Document ids were null.").isEmpty()) {
      throw new IllegalArgumentException("Document id list was empty.");
    }

    final Iterable<Integer> uniqueDocIds = new HashSet<>(docIds);
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Set<ByteArray> terms = new HashSet<>();

    for (final Integer docId : uniqueDocIds) {
      checkDocId(docId);
      for (final String field : getDocumentFields()) {
        for (final Fun.Tuple3<Integer, SerializableByte, ByteArray> k :
            this.invertedIdxDocTermsMap.subSet(
                Fun.t3(docId, getFieldId(field), (ByteArray) null), true,
                Fun.t3(docId, getFieldId(field), ByteArray.MAX), true)
            ) {
          if (!isStopword(k.c)) {
            terms.add(k.c);
          }
        }
      }
    }
    return terms;
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
    static DB.BTreeSetMaker idxTermsMapInvertedKeysMaker(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeSet(Stores.IDX_TERMS_MAP_INVERTED_KEYS.name())
          .serializer(BTreeKeySerializer.TUPLE3)
          .counterEnable();
    }

    /**
     * Get a maker for {@link #idxTerms}. Transient.
     *
     * @param db Database reference
     * @return Maker for {@link #idxTerms}
     */
    static DB.BTreeSetMaker idxTermsMaker(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeSet(Caches.IDX_TERMS.name())
          .serializer(ByteArray.SERIALIZER_KEY)
          .nodeSize(32)
          .counterEnable();
    }

    /**
     * Get a maker for {@link #idxDfMap}. Transient.
     *
     * @param db Database reference
     * @return Maker for {@link #idxDfMap}
     */
    static DB.BTreeMapMaker idxDfMapMaker(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeMap(Caches.IDX_DFMAP.name())
          .keySerializer(ByteArray.SERIALIZER_KEY)
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
    static DB.BTreeMapMaker idxTermsMapMkr(final DB db) {
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
     * Get a maker for {@link #cachedFieldsMap}. Transient.
     *
     * @param db Database reference
     * @return Maker for {@link #cachedFieldsMap}
     */
    static DB.BTreeMapMaker cachedFieldsMapMaker(final DB db) {
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
    private enum Flags {
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
    enum Stores {
      /**
       * Mapping of all document terms.
       */
      IDX_DOC_TERMS_MAP,
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
    enum Caches {

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
        commit(CacheDB.ALL);
      }

//      if (!builder.noOpenReadOnly) {
//        LOG.info("Re-opening persistent database read-only.");
//        commit(false);
//        getPersistStatic().closeDb();
//        builder.persistenceBuilder.readOnly();
//        builder.persistenceBuilder.get();
//        openStaticDb(builder.persistenceBuilder);
//        loadPersistentDb();
//      }

      checkTransientDbState();
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

      // load index term-map
      if (LOG.isDebugEnabled()) {
        LOG.debug("hasIdxTermsMap: {}", getPersistStatic().getDb().exists
            (CacheDbMakers.Stores.IDX_TERMS_MAP.name()));
      }
      setIdxTermsMap(CacheDbMakers.idxTermsMapMkr(
          getPersistStatic().getDb())
          .<Fun.Tuple2<SerializableByte, ByteArray>, Long>makeOrGet());

      // load/rebuild inverted index document term-map keys index
      final boolean hasInvertedIndex = getPersistStatic().getDb().exists(
          CacheDbMakers.Stores.IDX_TERMS_MAP_INVERTED_KEYS.name());
      if (hasInvertedIndex) {
        LOG.debug("hasInvertedIdxDocTermsMap: true");
      } else {
        LOG.debug("hasInvertedIdxDocTermsMap: false");
        LOG.info("Building inverted document terms index.");
      }
      DirectIndexDataProvider.this.invertedIdxDocTermsMap = CacheDbMakers
          .idxTermsMapInvertedKeysMaker(getPersistStatic().getDb()).makeOrGet();
      bindInvertedIndex(
          (Bind.MapWithModificationListener<Fun.Tuple3<
              ByteArray, SerializableByte, Integer>,
              Integer>) getIdxDocTermsMap(),
          DirectIndexDataProvider.this.invertedIdxDocTermsMap);
      if (!hasInvertedIndex) {
        LOG.info("Inverted document terms index built. {} entries.",
            DirectIndexDataProvider.this.invertedIdxDocTermsMap.size());
        LOG.info("Updating database.");
        commit(CacheDB.PERSISTENT);
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
      DB cacheDB = DBMaker.newMemoryDirectDB()
          .transactionDisable()
          .compressionEnable()
          .make();
      ConcurrentNavigableMap<Fun.Tuple3<
          ByteArray, SerializableByte, Integer>, Integer> cacheMap =
          CacheDbMakers.idxDocTermsMapMkr(cacheDB).makeOrGet();

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

      LOG.info("Storing results.");
      Integer oldDTFValue;
      for (Entry<Fun.Tuple3<ByteArray, SerializableByte, Integer>, Integer> e :
          cacheMap.entrySet()) {
        // replace value
        oldDTFValue = getIdxDocTermsMap().putIfAbsent(e.getKey(), e.getValue());
        // retry, until really replaced
        if (oldDTFValue != null) {
          while (!getIdxDocTermsMap().replace(e.getKey(),
              oldDTFValue, oldDTFValue + e.getValue())) {
            oldDTFValue = cacheMap.get(e.getKey());
          }
        }
      }
      cacheDB.close();

      // all fields successful updated
      getPersistStatic().getDb()
          .delete(CacheDbMakers.Flags.IDX_FIELDS_BEING_INDEXED.name());

      LOG.info("Inverted document terms index size={}.",
          DirectIndexDataProvider.this.invertedIdxDocTermsMap.size());
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
    private void checkTransientDbState() {
      LOG.info("Checking transient database state.");
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
        LOG.info("Stopwords ({}) unchanged.", getStopwords().size());
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
        LOG.info("Loaded index terms cache with {} entries.", idxTermsSize);
      }

      // try load overall term frequency
      if (getPersistTransient().getDb()
          .exists(CacheDbMakers.Caches.IDX_TF.name())) {
        setIdxTf(getPersistTransient().getDb()
            .getAtomicLong(CacheDbMakers.Caches.IDX_TF.name()).get());
        LOG.info("Term frequency value loaded ({}).", getIdxTf());
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
            getIdxDfMap().size());
      }
    }

    /**
     * Dynamically bind the {@link #invertedIdxDocTermsMap} to the main map.
     * Updates are reflected automatically. Based on {@link
     * Bind#secondaryKey(Bind.MapWithModificationListener, Set,
     * Fun.Function2)}.
     *
     * @param map Primary Map
     * @param secondary Secondary Set
     * @param <V> Value in Primary Map (unused)
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private <V> void bindInvertedIndex(
        final Bind.MapWithModificationListener<
            Fun.Tuple3<ByteArray, SerializableByte, Integer>, V> map,
        final Set<Fun.Tuple3<Integer, SerializableByte, ByteArray>> secondary) {

      //fill if empty
      if (secondary.isEmpty()) {
        for (final Fun.Tuple3<ByteArray, SerializableByte, Integer>
            t3 : map.keySet()) {
          secondary.add(Fun.t3(t3.c, t3.b, t3.a));
        }
      }

      //hook listener
      map.modificationListenerAdd(
          new Bind.MapListener<Fun.Tuple3<ByteArray, SerializableByte,
              Integer>, V>() {
            @SuppressWarnings("ObjectEquality")
            @Override
            public void update(
                final Fun.Tuple3<ByteArray, SerializableByte, Integer> key,
                final V oldVal, final V newVal) {
              if (newVal == null) {
                //removal
                secondary.remove(Fun.t3(key.c, key.b, key.a));
              } else if (oldVal == null) {
                //insert
                secondary.add(Fun.t3(key.c, key.b, key.a));
              }
            }
          });
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
            this.reader,
            this.docBase, this.fieldId, this.liveDocBits);
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
