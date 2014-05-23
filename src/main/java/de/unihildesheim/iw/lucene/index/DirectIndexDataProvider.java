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
import de.unihildesheim.iw.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Source;
import de.unihildesheim.iw.util.concurrent.processing.SourceException;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link IndexDataProvider} implementation directly accessing the Lucene index
 * and using some caching to speed-up data processing.
 *
 * @author Jens Bertram
 */
public final class DirectIndexDataProvider
    extends AbstractIndexDataProvider {

  /**
   * Prefix used to store configuration.
   */
  public static final String IDENTIFIER = "DirectIDP";

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      DirectIndexDataProvider.class);

  /**
   * Persistent disk backed storage backend.
   */
  private DB db;

  /**
   * Wrapper for persistent data storage.
   */
  private Persistence persistence;

  /**
   * Default constructor.
   *
   * @param isTemporary If true, data is held temporary only.
   */
  protected DirectIndexDataProvider(final boolean isTemporary) {
    super(isTemporary);
  }

  /**
   * Builder method to create a new instance.
   *
   * @param builder Builder to use for constructing the instance
   * @return New instance
   * @throws Buildable.BuildException Thrown, if initializing the instance with
   * the provided builder has failed
   * @throws DataProviderException Thrown, if initializing the instance data
   * failed
   */
  protected static DirectIndexDataProvider build(final Builder builder)
      throws DataProviderException, Buildable.BuildException {
    Objects.requireNonNull(builder, "Builder was null.");

    final DirectIndexDataProvider instance = new DirectIndexDataProvider
        (builder.isTemporary);

    // set configuration
    instance.setIndexReader(builder.idxReader);
    instance.setDocumentFields(builder.documentFields);
    instance.setLastIndexCommitGeneration(builder.lastCommitGeneration);
    try {
      instance.setStopwords(builder.stopwords);
    } catch (UnsupportedEncodingException e) {
      throw new Buildable.BuildException("Error parsing stopwords.", e);
    }

    // initialize
    try {
      instance.initCache(builder);
    } catch (Buildable.BuildableException | IOException e) {
      throw new DataProviderException("Failed to initialize cache.", e);
    }

    if (builder.doWarmUp) {
      instance.warmUp();
    }

    return instance;
  }

  /**
   * Initialize the database with a default configuration.
   *
   * @param builder Builder instance
   * @throws IOException Thrown on low-level I/O errors
   * @throws Buildable.BuildableException Thrown if {@link Persistence}
   * initialization fails
   * @throws DataProviderException Thrown, if the cache building failed
   */
  private void initCache(final Builder builder)
      throws IOException, Buildable.BuildableException, DataProviderException {
    boolean clearCache = false;

    final Persistence.Builder psb = builder.persistenceBuilder;

    switch (psb.getCacheLoadInstruction()) {
      case MAKE:
        this.persistence = psb.make().build();
        clearCache = true;
        break;
      case GET:
        this.persistence = psb.get().build();
        break;
      default:
        if (!psb.dbExists()) {
          clearCache = true;
        }
        this.persistence = psb.makeOrGet().build();
        break;
    }

    this.db = persistence.db;

    if (!clearCache) {
      if (!this.db.exists(DbMakers.Stores.IDX_TERMS_MAP.name())) {
        throw new IllegalStateException(
            "Invalid database state. 'idxTermsMap' does not exist.");
      }

      if (getLastIndexCommitGeneration() == null || !persistence
          .getMetaData().hasGenerationValue()) {
        LOG.warn("Index commit generation not available. Assuming an " +
            "unchanged index!");
      } else {
        if (!this.persistence.getMetaData().generationCurrent(
            getLastIndexCommitGeneration())) {
          throw new IllegalStateException("Invalid database state. " +
              "Index changed since last caching.");
        }
      }
      if (!this.persistence.getMetaData().fieldsCurrent(getDocumentFields())) {
        LOG.info("Fields changed. Caches needs to be rebuild.");
        clearCache = true;
      }
    }

    if (isTemporary()) {
      setCachedFieldsMap(
          new HashMap<String, SerializableByte>(getDocumentFields().size()));
    } else {
      setCachedFieldsMap(DbMakers.cachedFieldsMapMaker(this.db).<String,
          SerializableByte>makeOrGet());
    }

    LOG.debug("Fields: cached={} active={}", getCachedFieldsMap().keySet(),
        getDocumentFields());

    setIdxDocTermsMap(DbMakers.idxDocTermsMapMkr(this.db)
        .<Fun.Tuple3<ByteArray, SerializableByte, Integer>,
            Integer>makeOrGet());
    setIdxTermsMap(DbMakers.idxTermsMapMkr(this.db)
        .<Fun.Tuple2<SerializableByte, ByteArray>, Long>makeOrGet());

    // caches are current
    if (!clearCache) {
      // read cached values
      readDbCaches();

      // finally check if any fields failed to index, returns true,
      // if any fields have been removed
      clearCache = checkForIncompleteIndexedFields();
    }

    // caches are not current, or their have been incomplete indexed fields
    if (clearCache) {
      // re-create cached values
      clearDbCaches();
      try {
        buildCache(getDocumentFields());
      } catch (ProcessingException e) {
        throw new DataProviderException("Cache building failed.", e);
      }
    }

    this.persistence.updateMetaData(getDocumentFields(), getStopwords());
    commitDb(false);
  }

  @Override
  public void warmUp()
      throws DataProviderException {
    if (!cachesWarmed()) {
      super.warmUp();

      if (getIdxTf() == null || getIdxTf() == 0) {
        throw new IllegalStateException("Zero term frequency.");
      }

      if (this.db.exists(DbMakers.Caches.IDX_TF.name())) {
        this.db.getAtomicLong(DbMakers.Caches.IDX_TF.name()).set(getIdxTf());
      } else {
        this.db.createAtomicLong(DbMakers.Caches.IDX_TF.name(), getIdxTf());
      }


      if (getIdxDocumentIds().isEmpty()) {
        throw new IllegalStateException("Zero document ids.");
      }

      if (getIdxTerms().isEmpty()) {
        throw new IllegalStateException("Zero terms.");
      }

      this.persistence.updateMetaData(getDocumentFields(), getStopwords());
      commitDb(false);
    }
  }

  /**
   * Read dynamic caches from the database and validate their state.
   */
  private void readDbCaches() {
    if (this.persistence.getMetaData().stopWordsCurrent(getStopwords())) {
      LOG.info("Stopwords unchanged ({} terms).", getStopwords().size());

      // check if terms collector task has finished completely
      if (this.db.exists(DbMakers.Flags.IDX_TERMS_COLLECTING_RUN.name())) {
        LOG.warn("Found incomplete terms index. This index will " +
            "be deleted and rebuilt on warm-up.");
        this.db.delete(DbMakers.Flags.IDX_TERMS_COLLECTING_RUN.name());
        this.db.delete(DbMakers.Caches.IDX_TERMS.name());
      }

      // load terms index
      setIdxTerms(DbMakers.idxTermsMaker(this.db).<ByteArray>makeOrGet());
      final int idxTermsSize = getIdxTerms().size();
      if (idxTermsSize == 0) {
        LOG.info("Index terms cache is empty. Will be rebuild on warm-up.");
      } else {
        LOG.info("Loaded index terms cache with {} entries.", idxTermsSize);
      }

      // try load overall term frequency
      if (this.db.exists(DbMakers.Caches.IDX_TF.name())) {
        setIdxTf(this.db.getAtomicLong(DbMakers.Caches.IDX_TF.name()).get());
        LOG.info("Term frequency value loaded ({}).", getIdxTf());
      } else {
        LOG.info("No term frequency value found. Will be re-calculated on " +
            "warm-up.");
        clearIdxTf();
      }
    } else {
      // reset terms index
      LOG.info("Stopwords changed. Deleting index terms cache.");
      this.db.delete(DbMakers.Caches.IDX_TERMS.name());
      setIdxTerms(DbMakers.idxTermsMaker(this.db).<ByteArray>make());

      // reset overall term frequency
      setIdxTf(null);
    }

    // check if document-frequency calculation task has finished completely
    if (this.db.exists(DbMakers.Flags.IDX_DOC_FREQ_CALC_RUN.name())) {
      LOG.warn("Found incomplete document-frequency map. This map will " +
          "be deleted and rebuilt on warm-up.");
      this.db.delete(DbMakers.Flags.IDX_DOC_FREQ_CALC_RUN.name());
      this.db.delete(DbMakers.Caches.IDX_DFMAP.name());
    }
    setIdxDfMap(DbMakers.idxDfMapMaker(this.db).<ByteArray,
        Integer>makeOrGet());
    if (getIdxDfMap().isEmpty()) {
      LOG.info(
          "Document-frequency cache is empty. Will be rebuild on warm-up.");
    } else {
      LOG.info("Loaded document-frequency cache with {} entries.",
          getIdxDfMap().size());
    }
  }

  /**
   * Check, if field indexing was not completed in last run. Will remove any
   * field data that is flagged as being incomplete (due to crashes,
   * interruption, etc.).
   * <p/>
   * Cached data have to been loaded already to allow modifications.
   *
   * @return True, if indexed fields have changed
   */
  private boolean checkForIncompleteIndexedFields() {
    if (!this.db.exists(DbMakers.Flags.IDX_FIELDS_BEING_INDEXED.name())) {
      return false; // no changes
    }

    final Set<String> flaggedFields = this.db.createHashSet(DbMakers
        .Flags.IDX_FIELDS_BEING_INDEXED.name()).<String>makeOrGet();

    if (flaggedFields.isEmpty()) {
      return false; // no changes
    } else {
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
        for (ByteArray term : getIdxTerms()) {
          final Iterator<Integer> docTMapIt =
              Fun.filter(getIdxDocTermsMap().keySet(), term, fieldId)
                  .iterator();
          while (docTMapIt.hasNext()) {
            docTMapIt.next();
            docTMapIt.remove();
          }
        }
      }
      LOG.warn("Incompletely index fields found ({}). This fields have been " +
          "removed from cache. Cache rebuilding is needed.", flaggedFields);
      commitDb(false);
      return true; // fields were removed from cache
    }
  }

  /**
   * Clear all dynamic caches. This must be called, if the fields or stop-words
   * have changed.
   */
  void clearDbCaches() {
    LOG.info("Clearing temporary caches.");
    // index terms cache (content depends on current fields & stopwords)
    if (this.db.exists(DbMakers.Caches.IDX_TERMS.name())) {
      this.db.delete(DbMakers.Caches.IDX_TERMS.name());
    }
    setIdxTerms(DbMakers.idxTermsMaker(this.db).<ByteArray>make());

    // document term-frequency map
    // (content depends on current fields)
    if (this.db.exists(DbMakers.Caches.IDX_DFMAP.name())) {
      this.db.delete(DbMakers.Caches.IDX_DFMAP.name());
    }
    setIdxDfMap(DbMakers.idxDfMapMaker(this.db).<ByteArray, Integer>make());

    clearIdxTf();
  }

  /**
   * Build a cache of the current index.
   *
   * @param fields List of fields to cache
   * @throws ProcessingException Thrown, if any parallel data processing method
   * is failing
   * @throws IOException Thrown on low-level I/O errors
   */
  private void buildCache(final Set<String> fields)
      throws ProcessingException, IOException {
    final Set<String> updatingFields = new HashSet<>(fields);

    final boolean update = new HashSet<>(getCachedFieldsMap().keySet())
        .removeAll(updatingFields);
    updatingFields.removeAll(getCachedFieldsMap().keySet());

    // check, if there's anything to update
    if (!update && updatingFields.isEmpty()) {
      return;
    }

    if (updatingFields.isEmpty()) {
      LOG.info("Updating persistent index term cache. {} -> {}",
          getCachedFieldsMap().keySet(), fields);
      return;
    } else {
      LOG.info("Building persistent index term cache. {}",
          updatingFields);
    }

    // pre-check, if field has the information we need
    final FieldInfos fieldInfos = MultiFields.getMergedFieldInfos
        (getIndexReader());
    for (final String field : getDocumentFields()) {
      if (FieldInfo.IndexOptions.DOCS_ONLY.equals(fieldInfos.fieldInfo(field)
          .getIndexOptions())) {
        throw new IllegalStateException("Field '" + field + "' indexed with " +
            "DOCS_ONLY option. No term frequencies and position information " +
            "available.");
      }
    }

    // generate a field-id
    if (!updatingFields.isEmpty()) {
      for (final String field : updatingFields) {
        addFieldToCacheMap(field);
      }
    }

    // Store fields being update to database. The list will be emptied after
    // indexing. This will be used to identify incomplete indexed fields
    // caused by interruptions.
    final Set<String> flaggedFields = this.db.createHashSet(DbMakers
        .Flags.IDX_FIELDS_BEING_INDEXED.name()).<String>make();
    flaggedFields.addAll(updatingFields);
    commitDb(false); // store before processing

    final List<AtomicReaderContext> arContexts =
        getIndexReader().getContext().leaves();

    final Processing processing = new Processing();
    final TimedCommit autoCommit = new TimedCommit(UserConf.AUTOCOMMIT_DELAY);
    if (arContexts.size() == 1 && updatingFields.size() > 1) {
      LOG.debug("Build strategy: concurrent fields");

      final AtomicReader reader = arContexts.get(0).reader();
      Set<ByteArray> termSet;
      Terms terms;
      TermsEnum termsEnum = TermsEnum.EMPTY;
      BytesRef term;
      SerializableByte fieldId;

      for (final String field : updatingFields) {
        fieldId = getFieldId(field);
        termSet = new HashSet<>();
        terms = reader.terms(field);
        if (terms == null) {
          LOG.warn("No terms. field={}", field);
        } else {
          termsEnum = terms.iterator(termsEnum);
          term = termsEnum.next();
          while (term != null) {
            if (termsEnum.seekExact(term)) {
              termSet.add(BytesRefUtils.toByteArray(term));
            }
            term = termsEnum.next();
          }
        }
        assert !termSet.isEmpty();

        LOG.info("Building persistent index term cache. field={}", field);
        autoCommit.start();
        processing.setSourceAndTarget(
            new IndexTermsCollectorTarget(new CollectionSource<>(termSet),
                field, fieldId, arContexts.get(0), getIdxTermsMap(),
                getIdxDocTermsMap())
        ).process(termSet.size());
        autoCommit.stop();
      }
    } else {
      LOG.debug("Build strategy: segments ({})", arContexts.size());
      // we have multiple index segments, process every segment separately
      autoCommit.start();
      processing.setSourceAndTarget(
          new TargetFuncCall<>(
              new CollectionSource<>(arContexts),
              new IndexSegmentTermsCollectorTarget(updatingFields)
          )
      ).process(arContexts.size());
      autoCommit.stop();
    }
    autoCommit.stop();

    // all fields successful updated
    this.db.delete(DbMakers.Flags.IDX_FIELDS_BEING_INDEXED.name());

    commitDb(true);
  }

  /**
   * Try to commit the database and optinally run compaction. Commits will only
   * be done, if transaction is supported.
   *
   * @param compact If true, compaction will be run after committing
   */
  void commitDb(final boolean compact) {
    TimeMeasure tm = null;

    if (persistence.supportsTransaction()) {
      tm = new TimeMeasure().start();
      LOG.info("Updating database.");
      this.db.commit();
      LOG.info("Database updated. ({})", tm.stop().getTimeString());
    }

    if (compact) {
      if (tm == null) {
        tm = new TimeMeasure();
      }
      tm.start();
      LOG.info("Compacting database.");
      this.db.compact();
      LOG.info("Database compacted. ({})", tm.stop().getTimeString());
    }
  }

  @Override
  protected void warmUpTerms()
      throws DataProviderException {
    try {
      this.db.createAtomicBoolean(DbMakers.Flags.IDX_TERMS_COLLECTING_RUN
          .name(), true);

      super.warmUpTerms_default();

      this.db.delete(DbMakers.Flags.IDX_TERMS_COLLECTING_RUN.name());
      commitDb(false);
    } catch (ProcessingException e) {
      throw new DataProviderException(
          "Failed to warm-up terms", e);
    }
  }

  @Override
  protected void warmUpIndexTermFrequencies() {
    super.warmUpIndexTermFrequencies_default();
  }

  @Override
  protected void warmUpDocumentIds() {
    super.warmUpDocumentIds_default();
  }

  @Override
  protected void warmUpDocumentFrequencies()
      throws DataProviderException {
    try {
      this.db.createAtomicBoolean(DbMakers.Flags.IDX_DOC_FREQ_CALC_RUN.name()
          , true);
      super.warmUpDocumentFrequencies_default();
      this.db.delete(DbMakers.Flags.IDX_DOC_FREQ_CALC_RUN.name());
      commitDb(false);
    } catch (ProcessingException e) {
      throw new DataProviderException(
          "Failed to warm-up document frequencies", e);
    }
  }

  /**
   * {@inheritDoc} Deleted documents will be removed from the list.
   *
   * @return Unique collection of all (non-deleted) document ids
   */
  @Override
  protected Collection<Integer> getDocumentIds() {
    Collection<Integer> ret;

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
   * IndexTermsCollectorTarget} collecting all terms from a {@link DocsEnum}.
   * The results are stored to both the index terms map and document
   * term-frequency map.
   *
   * @param term Current term that is being collected
   * @param fieldId Id of the field that gets processed
   * @param targetIdxTerms Target map for storing index term frequency values
   * @param targetDocTerms Target map to store document term-frequency values
   * @param docsEnum Enum initialized with the given term
   * @param ttf Total term frequency (in index) of the given term
   * @param docBase DocBase value {@see AtomicReaderContext#docBase} to
   * calculate the read document-id
   * @throws IOException Thrown on low-level i/o errors
   */
  static void collectTerms(
      final ByteArray term, final SerializableByte fieldId,
      final ConcurrentMap<Fun.Tuple2<SerializableByte,
          ByteArray>, Long> targetIdxTerms,
      final ConcurrentMap<Fun.Tuple3<ByteArray, SerializableByte, Integer>,
          Integer> targetDocTerms,
      final DocsEnum docsEnum,
      final long ttf, final int docBase)
      throws IOException {
    // initialize the document iterator
    int docId = docsEnum.nextDoc();

    // build term frequency map for each document
    Integer oldDTFValue;
    while (docId != DocsEnum.NO_MORE_DOCS) {
      docId += docBase;
      final int docTermFreq = docsEnum.freq();

      oldDTFValue =
          targetDocTerms.putIfAbsent(Fun.t3(term, fieldId, docId),
              docTermFreq);
      if (oldDTFValue != null) {
        final Fun.Tuple3<ByteArray, SerializableByte, Integer>
            idxDocTfMapKey = Fun.t3(term, fieldId, docId);
        while (!targetDocTerms
            .replace(idxDocTfMapKey, oldDTFValue, oldDTFValue + docTermFreq)) {
          oldDTFValue = targetDocTerms.get(idxDocTfMapKey);
        }
      }

      docId = docsEnum.nextDoc();
    }

    // add whole index term frequency map
    Long oldValue = targetIdxTerms.putIfAbsent(Fun.t2(fieldId, term), ttf);
    if (oldValue != null) {
      final Fun.Tuple2<SerializableByte, ByteArray> idxTfMapKey =
          Fun.t2(fieldId, term);
      while (!targetIdxTerms.replace(idxTfMapKey, oldValue, oldValue + ttf)) {
        oldValue = targetIdxTerms.get(idxTfMapKey);
      }
    }
  }

  /**
   * Get the persistent storage manager.
   *
   * @return Persistence instance
   */
  Persistence getPersistence() {
    return persistence;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>0</tt>).
   *
   * @param term Term to lookup
   * @return Document frequency of the given term
   */
  @Override
  public int getDocumentFrequency(final ByteArray term) {
    Objects.requireNonNull(term, "Term was null.");
    if (isStopword(term)) {
      // skip stop-words
      return 0;
    }

    Integer freq = getIdxDfMap().get(term);
    if (freq == null) {
      return 0;
    }
    return freq;
  }

  @Override
  public void dispose() {
    if (this.db != null && !this.db.isClosed()) {
      commitDb(false);
      LOG.info("Closing database.");
      this.db.close();
    }
    setDisposed();
  }

  /**
   * {@inheritDoc} Stop-words will be skipped while creating the model.
   *
   * @param docId Document-id to create the model from
   * @return Model for the given document
   */
  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkDocId(docId);
    final DocumentModel.Builder dmBuilder
        = new DocumentModel.Builder(docId);

    try {
      final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum
          (getIndexReader(), getDocumentFields()).setDocument(docId);
      @SuppressWarnings("CollectionWithoutInitialCapacity")
      final Map<ByteArray, Long> tfMap = new HashMap<>();

      BytesRef bytesRef = dftEnum.next();
      while (bytesRef != null) {
        final ByteArray byteArray = BytesRefUtils.toByteArray(bytesRef);
        // skip stop-words
        if (!isStopword(byteArray)) {
          if (tfMap.containsKey(byteArray)) {
            tfMap.put(byteArray, tfMap.get(byteArray) + dftEnum.
                getTotalTermFreq());
          } else {
            tfMap.put(byteArray, dftEnum.getTotalTermFreq());
          }
        }
        bytesRef = dftEnum.next();
      }
      for (final Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
        dmBuilder
            .setTermFrequency(tfEntry.getKey(), tfEntry.getValue());
      }
      return dmBuilder.getModel();
    } catch (IOException ex) {
      LOG.error("Caught exception while iterating document terms. "
          + "docId=" + docId + ".", ex);
    }
    return null;
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
      final Collection<Integer> docIds)
      throws IOException {
    if (Objects.requireNonNull(docIds, "Document ids were null.").isEmpty()) {
      throw new IllegalArgumentException("Document id list was empty.");
    }

    final Set<Integer> uniqueDocIds = new HashSet<>(docIds);
    final Set<ByteArray> terms = new HashSet<>();
    final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(getIndexReader(),
        getDocumentFields());

    for (final Integer docId : uniqueDocIds) {
      checkDocId(docId);
      try {
        dftEnum.setDocument(docId);
        BytesRef br = dftEnum.next();
        ByteArray termBytes;
        while (br != null) {
          termBytes = BytesRefUtils.toByteArray(br);
          // skip adding stop-words
          if (!isStopword(termBytes)) {
            terms.add(termBytes);
          }
          br = dftEnum.next();
        }
      } catch (IOException ex) {
        LOG.error("Caught exception while iterating document terms. "
            + "docId=" + docId + ".", ex);
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
    Objects.requireNonNull(term, "Term was null.");

    if (isStopword(term)) {
      // skip stop-words
      return false;
    }

    checkDocId(documentId);
    try {
      final DocFieldsTermsEnum dftEnum =
          new DocFieldsTermsEnum(getIndexReader(), getDocumentFields())
              .setDocument
                  (documentId);
      BytesRef br = dftEnum.next();
      while (br != null) {
        if (BytesRefUtils.bytesEquals(br, term)) {
          return true;
        }
        br = dftEnum.next();
      }
    } catch (IOException ex) {
      LOG.error("Caught exception while iterating document terms. "
          + "docId=" + documentId + ".", ex);
    }
    return false;
  }

  /**
   * Wrapper for externally configurable options.
   */
  private static final class UserConf {
    static final int AUTOCOMMIT_DELAY = GlobalConfiguration.conf()
        .getAndAddInteger(key("db-autocommit-delay"), 60000);

    private static String key(final String name) {
      return IDENTIFIER + "_" + name;
    }
  }

  private static final class IndexTermsCollectorTarget
      extends Target<ByteArray> {

    private static ConcurrentMap<Fun.Tuple2<SerializableByte,
        ByteArray>, Long> targetIdxTerms;
    private static ConcurrentMap<Fun.Tuple3<ByteArray, SerializableByte,
        Integer>, Integer> targetDocTerms;
    private static AtomicReader reader;
    private static String field;
    private static int docBase;
    private static SerializableByte fieldId;

    IndexTermsCollectorTarget(
        final Source<ByteArray> newSource,
        final String newField, final SerializableByte newFieldId,
        final AtomicReaderContext context,
        final ConcurrentMap<Fun.Tuple2<SerializableByte, ByteArray>,
            Long> idxTermsMap,
        final ConcurrentMap<Fun.Tuple3<ByteArray, SerializableByte, Integer>,
            Integer> idxDocTermsMap)
        throws IOException {
      super(newSource);
      targetIdxTerms = idxTermsMap;
      targetDocTerms = idxDocTermsMap;
      field = newField;
      fieldId = newFieldId;
      reader = context.reader();
      docBase = context.docBase;
    }

    private IndexTermsCollectorTarget(final Source<ByteArray> source)
        throws IOException {
      super(source);
    }

    @Override
    public Target<ByteArray> newInstance()
        throws IOException {
      return new IndexTermsCollectorTarget(getSource());
    }

    @Override
    public void runProcess()
        throws Exception {
      ByteArray term;
      DocsEnum docsEnum = null;
      final TermsEnum termsEnum = reader.terms(field).iterator(TermsEnum.EMPTY);
      BytesRef termBr;

      while (!isTerminating()) {
        try {
          term = getSource().next();
        } catch (SourceException.SourceHasFinishedException ex) {
          break;
        }

        if (term != null) {
          termBr = new BytesRef(term.bytes);

          if (termsEnum.seekExact(termBr)) {
            docsEnum = reader.termDocsEnum(new Term(field, termBr));

            if (docsEnum == null) {
              // field or term does not exist
              LOG.warn("Field or term does not exist. f={} t={}", field,
                  ByteArrayUtils.utf8ToString(term));
            } else {
              collectTerms(term, fieldId, targetIdxTerms, targetDocTerms,
                  docsEnum, termsEnum.totalTermFreq(), docBase);
            }
          }
        }
      }
    }
  }

  public final static class Builder
      extends AbstractIndexDataProviderBuilder<Builder> {

    /**
     * Default constructor.
     */
    public Builder() {
      super(IDENTIFIER);
    }

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
      try {
        return DirectIndexDataProvider.build(this);
      } catch (DataProviderException e) {
        throw new BuildException(e);
      }
    }
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
     * Serializer to use for {@link #idxDocTermsMap}.
     */
    static final BTreeKeySerializer IDX_DOCTERMSMAP_KEYSERIALIZER
        = new BTreeKeySerializer.Tuple3KeySerializer<>(
        null, null,
        ByteArray.SERIALIZER, SerializableByte.SERIALIZER, Serializer.INTEGER);

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
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeSet(Caches.IDX_TERMS.name())
          .serializer(new BTreeKeySerializer.BasicKeySerializer(
              ByteArray.SERIALIZER))
          .nodeSize(18)
          .counterEnable();
    }

    /**
     * Get a maker for {@link #idxDfMap}.
     *
     * @param db Database reference
     * @return Maker for {@link #idxDfMap}
     */
    static DB.BTreeMapMaker idxDfMapMaker(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeMap(Caches.IDX_DFMAP.name())
          .keySerializerWrap(ByteArray.SERIALIZER)
          .valueSerializer(Serializer.INTEGER)
          .nodeSize(18)
          .counterEnable();
    }

    /**
     * Get a maker for {@link #idxTermsMap}.
     *
     * @param db Database reference
     * @return Maker for {@link #idxTermsMap}
     */
    static DB.BTreeMapMaker idxTermsMapMkr(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeMap(Stores.IDX_TERMS_MAP.name())
          .keySerializer(DbMakers.IDX_TERMSMAP_KEYSERIALIZER)
          .valueSerializer(Serializer.LONG)
          .counterEnable()
          .nodeSize(16);
    }

    /**
     * Get a maker for {@link #idxTermsMap}.
     *
     * @param db Database reference
     * @return Maker for {@link #idxTermsMap}
     */
    static DB.BTreeMapMaker idxDocTermsMapMkr(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeMap(Stores.IDX_DOC_TERMS_MAP.name())
          .keySerializer(DbMakers.IDX_DOCTERMSMAP_KEYSERIALIZER)
          .valueSerializer(Serializer.INTEGER)
          .counterEnable()
          .nodeSize(8);
    }

    /**
     * Get a maker for {@link #cachedFieldsMap}.
     *
     * @param db Database reference
     * @return Maker for {@link #cachedFieldsMap}
     */
    static DB.BTreeMapMaker cachedFieldsMapMaker(final DB db) {
      return Objects.requireNonNull(db, "DB was null.")
          .createTreeMap(Caches.IDX_FIELDS.name())
          .keySerializer(BTreeKeySerializer.STRING)
          .valueSerializer(SerializableByte.SERIALIZER)
          .nodeSize(8)
          .counterEnable();
    }

    /**
     * Checks all {@link Stores} against the DB, collecting missing ones.
     *
     * @param db Database reference
     * @return List of missing {@link Stores}
     */
    static Collection<Stores> checkStores(final DB db) {
      Objects.requireNonNull(db, "DB was null.");

      final Collection<Stores> miss = new ArrayList<>(Stores.values().length);
      for (final Stores s : Stores.values()) {
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
      Objects.requireNonNull(db, "DB was null.");

      final Collection<Caches> miss =
          new ArrayList<>(Caches.values().length);
      for (final Caches c : Caches.values()) {
        if (!db.exists(c.name())) {
          miss.add(c);
        }
      }
      return miss;
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
    public enum Stores {
      /**
       * Mapping of all document terms.
       */
      IDX_DOC_TERMS_MAP,
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
     * Create a new collector for index terms.
     *
     * @param newFields Lucene index segment provider
     */
    IndexSegmentTermsCollectorTarget(final Collection<String> newFields) {
      super();
      assert newFields != null && !newFields.isEmpty();
      this.fields = newFields;
    }

    @Override
    public void call(final AtomicReaderContext rContext)
        throws IOException {
      if (rContext == null) {
        return;
      }

      TermsEnum termsEnum = TermsEnum.EMPTY;
      DocsEnum docsEnum;
      Terms terms;
      final int docBase = rContext.docBase;
      final AtomicReader reader = rContext.reader();
      SerializableByte fieldId;
      BytesRef term;

      for (final String field : this.fields) {
        fieldId = getFieldId(field);
        terms = reader.terms(field);

        if (terms == null) {
          LOG.warn("No terms. field={}", field);
        } else {
          termsEnum = terms.iterator(termsEnum);
          term = termsEnum.next();

          while (term != null) {
            if (termsEnum.seekExact(term)) {
              docsEnum = reader.termDocsEnum(new Term(field, term));

              if (docsEnum == null) {
                // field or term does not exist
                LOG.warn("Field or term does not exist. f={} t={}", field,
                    term.utf8ToString());
              } else {
                collectTerms(BytesRefUtils.toByteArray(term), fieldId,
                    getIdxTermsMap(), getIdxDocTermsMap(),
                    docsEnum, termsEnum.totalTermFreq(), docBase);
              }
            }
            term = termsEnum.next();
          }
        }
      }
    }
  }

  private final class TimedCommitRunner
      extends TimerTask {
    private final TimeMeasure tm = new TimeMeasure().start();
    private final int period;
    private boolean finished = true;

    TimedCommitRunner(final int newPeriod) {
      super();
      this.period = newPeriod;
    }

    @Override
    public void run() {
      if (this.finished && tm.getElapsedMillis() >= this.period) {
        this.finished = false;
        commitDb(false);
        this.finished = true;
        tm.start();
      }
    }
  }

  /**
   * Auto-commit database on long running processes.
   */
  private final class TimedCommit {

    private Timer timer;
    private int period;
    private TimedCommitRunner task;

    /**
     * Initializes the timer and starts it after the given delay, repeating the
     * commit in the given period.
     *
     * @param delay delay in milliseconds before task is to be executed.
     * @param newPeriod time in milliseconds between successive task executions
     */
    TimedCommit(final int delay, final int newPeriod) {
      if (getPersistence().supportsTransaction()) {
        this.period = newPeriod;
        this.timer = new Timer();
        this.task = new TimedCommitRunner(this.period);
        this.timer.schedule(task, delay, this.period);
      }
    }

    /**
     * Initialize the timer. This will not start the timer. You need to call
     * {@link #start(int)} separately.
     *
     * @param newPeriod time in milliseconds between successive task executions
     */
    TimedCommit(final int newPeriod) {
      if (getPersistence().supportsTransaction()) {
        this.period = newPeriod;
        this.timer = new Timer();
        this.task = new TimedCommitRunner(this.period);
      }
    }

    /**
     * Stop the timer.
     */
    void stop() {
      if (getPersistence().supportsTransaction()) {
        this.timer.cancel();
      }
    }

    /**
     * Start the timer using a new period time.
     *
     * @param newPeriod time in milliseconds between successive task executions
     */
    void start(final int newPeriod) {
      if (getPersistence().supportsTransaction()) {
        this.timer.cancel();
        this.timer = new Timer();
        this.task = new TimedCommitRunner(this.period);
        this.timer.schedule(task, 0, newPeriod);
      }
    }

    /**
     * Start the timer again.
     */
    void start() {
      if (getPersistence().supportsTransaction()) {
        this.timer.cancel();
        this.timer = new Timer();
        this.timer.schedule(task, 0, this.period);
      }
    }
  }
}
