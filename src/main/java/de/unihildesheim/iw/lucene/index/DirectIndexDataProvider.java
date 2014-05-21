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
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
  DB db;

  /**
   * Wrapper for persistent data storage.
   */
  private Persistence persistence;

  /**
   * Wrapper for externally configurable options.
   */
  private static final class UserConf {
    private static final String key(final String name) {
      return IDENTIFIER + "_" + name;
    }

    static final int AUTOCOMMIT_DELAY = GlobalConfiguration.conf()
        .getAndAddInteger(key("db-autocommit-delay"), 60000);
  }

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
   * @throws IOException Thrown on low-level I/O errors
   */
  protected static DirectIndexDataProvider build(final Builder builder)
      throws IOException, Buildable.BuildableException, DataProviderException,
             ProcessingException {
    Objects.requireNonNull(builder, "Builder was null.");

    final DirectIndexDataProvider instance = new DirectIndexDataProvider
        (builder.isTemporary);

    // set configuration
    instance.setIndexReader(builder.idxReader);
    instance.setDocumentFields(builder.documentFields);
    instance.setLastIndexCommitGeneration(builder.lastCommitGeneration);
    instance.setStopwords(builder.stopwords);

    // initialize
    instance.initCache(builder);

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
   */
  private void initCache(final Builder builder)
      throws IOException, Buildable.BuildableException, ProcessingException {
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
    final Persistence.StorageMeta dbMeta = this.persistence.getMetaData();

    if (!clearCache) {
      if (!this.db.exists(DbMakers.Stores.IDX_TERMS_MAP.name())) {
        throw new IllegalStateException(
            "Invalid database state. 'idxTermsMap' does not exist.");
      }

      if (getLastIndexCommitGeneration() == null) {
        LOG.warn("Index commit generation not available. Assuming an " +
            "unchanged index!");
      } else {
        if (!dbMeta.generationCurrent(getLastIndexCommitGeneration())) {
          throw new IllegalStateException("Invalid database state. " +
              "Index changed since last caching.");
        }
      }
      if (!dbMeta.fieldsCurrent(getDocumentFields())) {
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
        .<Fun.Tuple3<SerializableByte, Integer, ByteArray>,
            Integer>makeOrGet());
    setIdxTermsMap(DbMakers.idxTermsMapMkr(this.db)
        .<Fun.Tuple2<SerializableByte, ByteArray>, Long>makeOrGet());

    if (clearCache) {
      clearCache();
      buildCache(getDocumentFields());
    } else {
      if (dbMeta.stopWordsCurrent(getStopwords())) {
        LOG.info("Stopwords unchanged.");

        // load terms index
        setIdxTerms(DbMakers.idxTermsMaker(this.db).<ByteArray>makeOrGet());
        final int idxTermsSize = getIdxTerms().size();
        if (idxTermsSize == 0) {
          LOG.info("Index terms cache is empty. Will be rebuild on warm-up.");
        } else {
          LOG.info("Loaded index terms cache with {} entries.",
              idxTermsSize);
        }

        // try load overall term frequency
        setIdxTf(this.db.getAtomicLong(DbMakers.Caches.IDX_TF.name()).get());
        if (getIdxTf() == 0L) {
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

      setIdxDfMap(DbMakers.idxDfMapMaker(this.db).<ByteArray,
          Integer>makeOrGet());
    }

    this.persistence.updateMetaData(getDocumentFields(), getStopwords());
    this.db.commit();
  }

  @Override
  public void warmUp()
      throws DataProviderException {
    if (!cachesWarmed()) {
      super.warmUp();

      if (getIdxTf() == null || getIdxTf() == 0) {
        throw new IllegalStateException("Zero term frequency.");
      }
      this.db.getAtomicLong(DbMakers.Caches.IDX_TF.name()).set(getIdxTf());

      if (getIdxDocumentIds().isEmpty()) {
        throw new IllegalStateException("Zero document ids.");
      }

      if (getIdxTerms().isEmpty()) {
        throw new IllegalStateException("Zero terms.");
      }

      LOG.info("Writing data.");
      this.persistence.updateMetaData(getDocumentFields(), getStopwords());
      this.db.commit();
      LOG.debug("Writing data - done.");
    }
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
   * Clear all dynamic caches. This must be called, if the fields or stop-words
   * have changed.
   */
  void clearCache() {
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
   */
  private void buildCache(final Set<String> fields)
      throws ProcessingException, IOException {
    final Set<String> updatingFields = new HashSet<>(fields);

    final boolean update = new HashSet<>(getCachedFieldsMap().keySet())
        .removeAll(updatingFields);
    updatingFields.removeAll(getCachedFieldsMap().keySet());

    if (!updatingFields.isEmpty()) {
      for (final String field : updatingFields) {
        addFieldToCacheMap(field);
      }
    }

    if (!update && updatingFields.isEmpty()) {
      return;
    }

    if (updatingFields.isEmpty()) {
      LOG.info("Updating persistent index term cache. {} -> {}",
          getCachedFieldsMap().keySet(), fields);
    } else {
      LOG.info("Building persistent index term cache. {}",
          updatingFields);
    }

    // pre-check, if field has the information we need
    for (String field : getDocumentFields()) {
      if (FieldInfo.IndexOptions.DOCS_ONLY.equals(MultiFields
          .getMergedFieldInfos(getIndexReader()).fieldInfo(field)
          .getIndexOptions())) {
        throw new IllegalStateException("Field '" + field + "' indexed with " +
            "DOCS_ONLY option. No term frequencies and position information " +
            "available.");
      }
    }

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
              new IndexSegmentTermsCollectorTarget(
                  getIdxTermsMap(), getIdxDocTermsMap(), updatingFields)
          )
      ).process(arContexts.size());
      autoCommit.stop();
    }
    autoCommit.stop();

    LOG.info("Updating database.");
    this.db.commit();
    LOG.info("Compacting database.");
    this.db.compact();
  }

  /**
   * {@inheritDoc} Deleted documents will be removed from the list.
   *
   * @return Unique collection of all (non-deleted) document ids
   */
  @Override
  protected Collection<Integer> getDocumentIds() {
    Collection<Integer> ret;
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

    final Integer freq;

    // if map is not pre-cached, try to calculate the value on-the-fly
    if (getIdxDfMap().get(term) == null) {
      final List<AtomicReaderContext> arContexts;
      arContexts = getIndexReader().leaves();

      final Collection<Integer> matchedDocs;
      if (getIdxDocumentIds() == null) {
        matchedDocs = new HashSet<>();
      } else {
        matchedDocs = new HashSet<>(getIdxDocumentIds().size());
      }

      final BytesRef termBr = new BytesRef(term.bytes);
      AtomicReader reader;
      DocsEnum docsEnum;
      int docId;
      for (final AtomicReaderContext aReader : arContexts) {
        reader = aReader.reader();
        for (final String field : getDocumentFields()) {
          try {
            final Term termObj = new Term(field, termBr);
            docsEnum = reader.termDocsEnum(termObj);
            if (docsEnum == null) {
              LOG.trace("Field or term does not exist. field={}" +
                      " term={}", field, termBr.utf8ToString()
              );
            } else {
              docId = docsEnum.nextDoc();
              while (docId != DocsEnum.NO_MORE_DOCS) {
                if (docsEnum.freq() > 0) {
                  matchedDocs.add(docId + aReader.docBase);
                }
                docId = docsEnum.nextDoc();
              }
            }
          } catch (IOException ex) {
            LOG.error(
                "Error enumerating documents. field={} term={}", field,
                termBr.utf8ToString(), ex);
          }
        }
      }
      getIdxDfMap().put(term.clone(), matchedDocs.size());
      freq = matchedDocs.size();
    } else {
      freq = getIdxDfMap().get(term);
    }

    return freq;
  }

  @Override
  public void dispose() {
    if (this.db != null && !this.db.isClosed()) {
      LOG.info("Closing database.");
      this.db.commit();
      this.db.compact();
      this.db.close();
    }
    setDisposed();
  }

  /**
   * Collect the terms from an {@link TermsEnum} instance.
   */
  private void targetFuncCollectTerms(
      final AtomicReaderContext rContext,
      final String fieldName,
      final ConcurrentMap<Fun.Tuple2<SerializableByte, ByteArray>,
          Long> targetIdxTerms,
      final ConcurrentMap<Fun.Tuple3<SerializableByte, Integer, ByteArray>,
          Integer> targetDocTerms)
      throws IOException {


    TermsEnum termsEnum = TermsEnum.EMPTY;
    DocsEnum docsEnum;
    Terms terms;
    final SerializableByte fieldId = getFieldId(fieldName);
    final AtomicReader reader = rContext.reader();
    terms = reader.terms(fieldName);

    if (terms == null) {
      LOG.warn("No terms. field={}", fieldName);
    } else {
      termsEnum = terms.iterator(termsEnum);
      BytesRef term = termsEnum.next();

      while (term != null) {
        if (termsEnum.seekExact(term)) {
          docsEnum = reader.termDocsEnum(new Term(fieldName, term));

          // this should never happen (values are pre-checked already)
          if (docsEnum == null) {
            throw new IllegalStateException("Term or field does not exist.");
          }

          // get the total term frequency of the current term
          // across all documents and the current field
          final long ttf = termsEnum.totalTermFreq();

          // initialize the document iterator
          int docId = docsEnum.nextDoc();

          // build term frequency map for each document
          while (docId != DocsEnum.NO_MORE_DOCS) {
            docId += rContext.docBase;
            final int docTermFreq = docsEnum.freq();

            final Fun.Tuple3<SerializableByte, Integer, ByteArray>
                idxDocTfMapKey =
                Fun.t3(fieldId, docId, BytesRefUtils.toByteArray(term));
            Integer oldValue = targetDocTerms.putIfAbsent(idxDocTfMapKey,
                docTermFreq);
            if (oldValue != null) {
              Integer newValue = oldValue + docTermFreq;
              while (!targetDocTerms.replace(idxDocTfMapKey, oldValue,
                  newValue)) {
                oldValue = targetDocTerms.get(idxDocTfMapKey);
                newValue = oldValue + docTermFreq;
              }
            }

            docId = docsEnum.nextDoc();
          }

          // add whole index term frequency map
          final Fun.Tuple2<SerializableByte, ByteArray> idxTfMapKey =
              Fun.t2(fieldId, BytesRefUtils.toByteArray(term));
          Long oldValue = targetIdxTerms.putIfAbsent(idxTfMapKey, ttf);
          if (oldValue != null) {
            Long newValue = oldValue + ttf;
            while (!targetIdxTerms.replace(idxTfMapKey, oldValue, newValue)) {
              oldValue = targetIdxTerms.get(idxTfMapKey);
              newValue = oldValue + ttf;
            }
          }
        }
        term = termsEnum.next();
      }
    }
  }

  private static void collectTerms(
      final ByteArray term, final SerializableByte fieldId,
      final ConcurrentMap<Fun.Tuple2<SerializableByte,
          ByteArray>, Long> targetIdxTerms,
      final ConcurrentMap<Fun.Tuple3<SerializableByte, Integer,
          ByteArray>, Integer> targetDocTerms, final DocsEnum docsEnum,
      final long ttf, final int docBase)
      throws IOException {
    // initialize the document iterator
    int docId = docsEnum.nextDoc();

    // build term frequency map for each document
    while (docId != DocsEnum.NO_MORE_DOCS) {
      docId += docBase;
      final int docTermFreq = docsEnum.freq();

      final Fun.Tuple3<SerializableByte, Integer, ByteArray>
          idxDocTfMapKey = Fun.t3(fieldId, docId, term);
      Integer oldValue = targetDocTerms.putIfAbsent(idxDocTfMapKey,
          docTermFreq);
      if (oldValue != null) {
        Integer newValue = oldValue + docTermFreq;
        while (!targetDocTerms.replace(idxDocTfMapKey, oldValue, newValue)) {
          oldValue = targetDocTerms.get(idxDocTfMapKey);
          newValue = oldValue + docTermFreq;
        }
      }

      docId = docsEnum.nextDoc();
    }

    // add whole index term frequency map
    final Fun.Tuple2<SerializableByte, ByteArray> idxTfMapKey =
        Fun.t2(fieldId, term);
    Long oldValue = targetIdxTerms.putIfAbsent(idxTfMapKey, ttf);
    if (oldValue != null) {
      Long newValue = oldValue + ttf;
      while (!targetIdxTerms.replace(idxTfMapKey, oldValue, newValue)) {
        oldValue = targetIdxTerms.get(idxTfMapKey);
        newValue = oldValue + ttf;
      }
    }
  }

  private static final class IndexTermsCollectorTarget
      extends Target<ByteArray> {

    private static ConcurrentMap<Fun.Tuple2<SerializableByte,
        ByteArray>, Long> targetIdxTerms;
    private static ConcurrentMap<Fun.Tuple3<SerializableByte, Integer,
        ByteArray>, Integer> targetDocTerms;
    private static AtomicReader reader;
    private static String field;
    private TermsEnum termsEnum;
    private static int docBase;
    private static SerializableByte fieldId;
    private DocsEnum docsEnum;
    private Bits bits;

    IndexTermsCollectorTarget(
        final Source<ByteArray> newSource,
        final String newField, final SerializableByte newFieldId,
        final AtomicReaderContext context,
        final ConcurrentMap<Fun.Tuple2<SerializableByte, ByteArray>,
            Long> idxTermsMap,
        final ConcurrentMap<Fun.Tuple3<SerializableByte, Integer, ByteArray>,
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
      this.termsEnum = reader.terms(field).iterator(TermsEnum.EMPTY);
      this.bits = reader.getDocsWithField(field);
      this.docsEnum = null;
    }

    @Override
    public Target<ByteArray> newInstance()
        throws IOException {
      return new IndexTermsCollectorTarget(getSource());
    }

    @Override
    public void runProcess()
        throws Exception {
      while (!isTerminating()) {
        final ByteArray term;
        try {
          term = getSource().next();
        } catch (ProcessingException.SourceHasFinishedException ex) {
          break;
        }

        if (term != null) {
          if (!this.termsEnum.seekExact(new BytesRef(term.bytes))) {
            return;
          }

          this.docsEnum = this.termsEnum.docs(bits, this.docsEnum);

          if (this.docsEnum == null) {
            // field or term does not exist
            LOG.warn("Field or term does not exist. f={} t={}", field,
                ByteArrayUtils.utf8ToString(term));
          } else {
            collectTerms(term, fieldId, targetIdxTerms, targetDocTerms,
                this.docsEnum, this.termsEnum.totalTermFreq(), docBase);
          }
        }
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for collecting index terms on a per field
   * basis. Each Lucene document field will be accessed by a separate {@link
   * TermsEnum}.
   */
  private final class IndexFieldTermsCollectorTarget
      extends TargetFuncCall.TargetFunc<String> {

    private final AtomicReaderContext arContext;
    private final ConcurrentMap<Fun.Tuple2<SerializableByte,
        ByteArray>, Long> targetIdxTerms;
    private final ConcurrentMap<Fun.Tuple3<SerializableByte, Integer,
        ByteArray>, Integer> targetDocTerms;

    public IndexFieldTermsCollectorTarget(
        final AtomicReaderContext newArContext,
        final ConcurrentMap<Fun.Tuple2<SerializableByte, ByteArray>,
            Long> idxTermsMap,
        final ConcurrentMap<Fun.Tuple3<SerializableByte, Integer, ByteArray>,
            Integer> idxDocTermsMap) {
      super();
      this.arContext = newArContext;
      this.targetIdxTerms = idxTermsMap;
      this.targetDocTerms = idxDocTermsMap;
    }

    @Override
    public void call(final String fieldName)
        throws IOException {

      if (fieldName != null) {
        targetFuncCollectTerms(this.arContext, fieldName, this.targetIdxTerms,
            this.targetDocTerms);
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
     * Target map to store results.
     */
    private final ConcurrentMap<Fun.Tuple2<SerializableByte, ByteArray>,
        Long> targetIdxTerms;
    private final ConcurrentMap<Fun.Tuple3<SerializableByte, Integer,
        ByteArray>, Integer> targetDocTerms;

    /**
     * Create a new collector for index terms.
     *
     * @param newFields Lucene index segment provider
     */
    IndexSegmentTermsCollectorTarget(
        final ConcurrentMap<Fun.Tuple2<SerializableByte, ByteArray>,
            Long> idxTermsMap,
        final ConcurrentMap<Fun.Tuple3<SerializableByte, Integer, ByteArray>,
            Integer> idxDocTermsMap,
        final Collection<String> newFields) {
      super();
      assert idxDocTermsMap != null;
      assert newFields != null && !newFields.isEmpty();

      this.fields = newFields;
      this.targetIdxTerms = idxTermsMap;
      this.targetDocTerms = idxDocTermsMap;
    }

    @Override
    public void call(final AtomicReaderContext rContext)
        throws IOException {
      if (rContext == null) {
        return;
      }

      for (final String field : this.fields) {
        targetFuncCollectTerms(rContext, field, this.targetIdxTerms,
            this.targetDocTerms);
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
    public DirectIndexDataProvider build()
        throws BuildableException {
      validate();
      try {
        return DirectIndexDataProvider.build(this);
      } catch (IOException | DataProviderException | ProcessingException e) {
        throw new BuildException(e);
      }
    }

    @Override
    public void validate()
        throws ConfigurationException {
      super.validate();
      validatePersistenceBuilder();
    }
  }

  private final class TimedCommitRunner
      extends TimerTask {
    private boolean finished = true;
    private TimeMeasure tm = new TimeMeasure().start();
    private final int period;

    TimedCommitRunner(final int newPeriod) {
      this.period = newPeriod;
    }

    @Override
    public void run() {
      if (this.finished && tm.getElapsedMillis() >= this.period) {
        this.finished = false;
        tm.stop().start();
        LOG.info("Updating database.");
        DirectIndexDataProvider.this.db.commit();
        LOG.info("Database updated. ({})", tm.stop().getTimeString());
        this.finished = true;
        tm.start();
      }
    }
  }

  /**
   * Auto-commit database on long running processes.
   */
  private final class TimedCommit {

    private Timer timer = new Timer();
    private final int period;
    private TimedCommitRunner task;

    /**
     * Initializes the timer and starts it after the given delay, repeating the
     * commit in the given period.
     *
     * @param delay delay in milliseconds before task is to be executed.
     * @param newPeriod time in milliseconds between successive task executions
     */
    TimedCommit(final int delay, final int newPeriod) {
      this.period = newPeriod;
      this.timer = new Timer();
      this.task = new TimedCommitRunner(this.period);
      this.timer.schedule(task, delay, this.period);
    }

    /**
     * Initialize the timer. This will not start the timer. You need to call
     * {@link #start(int)} separately.
     *
     * @param newPeriod time in milliseconds between successive task executions
     */
    TimedCommit(final int newPeriod) {
      this.period = newPeriod;
      this.task = new TimedCommitRunner(this.period);
    }

    /**
     * Stop the timer.
     */
    void stop() {
      this.timer.cancel();
    }

    /**
     * Start the timer using a new period time.
     *
     * @param newPeriod time in milliseconds between successive task executions
     */
    void start(final int newPeriod) {
      this.timer.cancel();
      this.timer = new Timer();
      this.task = new TimedCommitRunner(this.period);
      this.timer.schedule(task, 0, newPeriod);
    }

    /**
     * Start the timer again.
     */
    void start() {
      this.timer.cancel();
      this.timer = new Timer();
      this.timer.schedule(task, 0, this.period);
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
        SerializableByte.COMPARATOR, null, SerializableByte.SERIALIZER,
        Serializer.INTEGER, ByteArray.SERIALIZER);

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
          .nodeSize(100);
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
          .nodeSize(100);
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
}
