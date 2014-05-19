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
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.util.BytesRefUtils;
import de.unihildesheim.iw.util.TimeMeasure;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.Target;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
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
   * Prefix used to store configuration.
   */
  public static final String IDENTIFIER = "DirectIDP";
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      DirectIndexDataProvider.class);

  /**
   * Wrapper for persistent data storage.
   */
  private Persistence persistence;

  /**
   * List of current stopwords.
   */
  private Collection<String> stopwords;

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
    Objects.requireNonNull(builder);

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


    setDb(this.persistence.db);

    final Persistence.StorageMeta dbMeta = this.persistence.getMetaData();

    if (!clearCache) {
      if (!getDb().exists(DbMakers.Stores.IDX_TERMS_MAP.name())) {
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
      setCachedFieldsMap(DbMakers.cachedFieldsMapMaker(getDb()).<String,
          SerializableByte>makeOrGet());
    }

    LOG.debug("Fields: cached={} active={}", getCachedFieldsMap().keySet(),
        getDocumentFields());

    setIdxTermsMap(DbMakers.idxTermsMapMkr(getDb())
        .<Fun.Tuple2<SerializableByte, ByteArray>, Long>makeOrGet());

    if (clearCache) {
      clearCache();
      buildCache(getDocumentFields());
    } else {
      if (dbMeta.stopWordsCurrent(getStopwords())) {
        // load terms index
        setIdxTerms(DbMakers.idxTermsMaker(getDb()).<ByteArray>makeOrGet());
        LOG.info("Stopwords unchanged. Loaded index terms cache with {} " +
                "entries.", getIdxTerms().size()
        );

        // try load overall term frequency
        setIdxTf(getDb().getAtomicLong(DbMakers.Caches.IDX_TF.name())
            .get());
        if (getIdxTf() == 0L) {
          setIdxTf(null);
        }
      } else {
        // reset terms index
        LOG.info("Stopwords changed. Deleting index terms cache.");
        getDb().delete(DbMakers.Caches.IDX_TERMS.name());
        setIdxTerms(DbMakers.idxTermsMaker(getDb()).<ByteArray>make());

        // reset overall term frequency
        setIdxTf(null);
      }

      setIdxDfMap(DbMakers.idxDfMapMaker(getDb()).<ByteArray,
          Integer>makeOrGet());
    }

    this.persistence.updateMetaData(getDocumentFields(), getStopwords());
    getDb().commit();
  }

  @Override
  public void warmUp()
      throws DataProviderException {
    if (!cachesWarmed()) {
      super.warmUp();

      if (getIdxTf() == null || getIdxTf() == 0) {
        throw new IllegalStateException("Zero term frequency.");
      }
      if (getIdxDocumentIds().isEmpty()) {
        throw new IllegalStateException("Zero document ids.");
      }
      if (getIdxTerms().isEmpty()) {
        throw new IllegalStateException("Zero terms.");
      }

      LOG.info("Writing data.");
      this.persistence.updateMetaData(getDocumentFields(), getStopwords());
      getDb().commit();
      LOG.debug("Writing data - done.");
    }
  }

  @Override
  protected void warmUpDocumentFrequencies()
      throws DataProviderException.CacheException {
    // cache document frequency values for each term
    if (getIdxDfMap().isEmpty()) {
      final TimeMeasure tStep = new TimeMeasure().start();
      LOG.info("Cache warming: document frequencies..");
      final List<AtomicReaderContext> arContexts = getIndexReader().getContext()
          .leaves();

      try {
        if (arContexts.size() == 1 && getDocumentFields().size() > 1) {
          LOG.debug("WarmUp strategy: concurrent fields");
          // we have only one segment, process each field concurrently
          new Processing(
              new TargetFuncCall<>(
                  new CollectionSource<>(getDocumentFields()),
                  new IndexFieldDocFreqCalcTarget(arContexts
                      .get(0).reader(), getIdxDfMap(),
                      getIdxDocumentIds().size(), getIdxTerms()
                  )
              )
          ).process(getDocumentFields().size());
        } else {
          new Processing(
              new TargetFuncCall<>(
                  new CollectionSource<>(arContexts),
                  new IndexSegmentDocFreqCalcTarget(getIdxDfMap(),
                      getIdxDocumentIds().size(),
                      getIdxTerms(),
                      getDocumentFields())
              )
          ).process(arContexts.size());
        }
      } catch (ProcessingException e) {
        throw new DataProviderException.CacheException("Failed to warmUp " +
            "document frequencies.", e);
      }
      LOG.info("Cache warming: calculating document frequencies "
              + "for {} documents and {} terms took {}.",
          getIdxDocumentIds().size(), getIdxTerms().size(),
          tStep.stop().getTimeString()
      );
    } else {
      LOG.info("Cache warming: Document frequencies "
              + "for {} documents and {} terms already loaded.",
          getIdxDocumentIds().size(), getIdxTerms().size()
      );
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

    final List<AtomicReaderContext> arContexts =
        getIndexReader().getContext().leaves();

    if (arContexts.size() == 1 && updatingFields.size() > 1) {
      LOG.debug("Build strategy: concurrent fields");
      // we have only one segment, process each field concurrently
      new Processing(
          new TargetFuncCall<>(
              new CollectionSource<>(updatingFields),
              new IndexFieldTermsCollectorTarget(arContexts
                  .get(0).reader(), getIdxTermsMap())
          )
      ).process(updatingFields.size());
    } else {
      LOG.debug("Build strategy: segment");
      // we have multiple index segments, process every segment separately
      new Processing(
          new TargetFuncCall<>(
              new CollectionSource<>(arContexts),
              new IndexSegmentTermsCollectorTarget(
                  getIdxTermsMap(), updatingFields)
          )
      ).process(arContexts.size());
    }

    LOG.info("Writing data.");

    getDb().commit();
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
    if (Objects.requireNonNull(docIds).isEmpty()) {
      throw new IllegalArgumentException("Document id list was empty.");
    }

    final Set<Integer> uniqueDocIds = new HashSet<>(docIds);
    @SuppressWarnings("CollectionWithoutInitialCapacity")
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
    Objects.requireNonNull(term);

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
    Objects.requireNonNull(term);
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
                      " term={}", field,
                  termBr.utf8ToString()
              );
            } else {
              docId = docsEnum.nextDoc();
              while (docId != DocsEnum.NO_MORE_DOCS) {
                if (docsEnum.freq() > 0) {
                  matchedDocs.add(docId);
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

  /**
   * Shared function for {@link Processing} {@link TargetFuncCall} classes.
   */
  private static final class TargetFuncShared {
    /**
     * Collect the terms from an {@link TermsEnum} instance.
     *
     * @param fieldId Id of the fields the terms belong to
     * @param termsEnum The enum proiding field terms
     * @param targetMap Target to store results
     * @throws IOException Thrown on low-level I/O errors
     */
    static void collectTerms(final SerializableByte fieldId,
        final TermsEnum termsEnum,
        final ConcurrentMap<Fun.Tuple2<SerializableByte, ByteArray>,
            Long> targetMap)
        throws IOException {
      BytesRef br = termsEnum.next();
      while (br != null) {
        if (termsEnum.seekExact(br)) {
          // get the total term frequency of the current term
          // across all documents and the current field
          final long ttf = termsEnum.totalTermFreq();
          // add value up for all fields
          final Fun.Tuple2<SerializableByte, ByteArray> fieldTerm =
              Fun.t2(fieldId, BytesRefUtils.toByteArray(br));

          Long oldValue = targetMap.putIfAbsent(fieldTerm, ttf);
          if (oldValue != null) {
            for (; ; ) {
              if (targetMap.replace(fieldTerm, oldValue,
                  oldValue + ttf)) {
                break;
              }
              oldValue = targetMap.get(fieldTerm);
            }
          }
        }
        br = termsEnum.next();
      }
    }

    /**
     * @param reader Reader to access the Lucene index
     * @param idxTerms Set of index terms
     * @param docCount Number of documents in index
     * @param field Field to index
     * @param map Target map for storing results
     */
    static void calcDocFreq(final AtomicReader reader, final Set<ByteArray>
        idxTerms, int docCount, final String field,
        ConcurrentMap<ByteArray, Integer> map) {
      // step through all terms in index
      for (final ByteArray term : idxTerms) {
        final BytesRef termBr = new BytesRef(term.bytes);
        final Set<Integer> matchedDocs = new HashSet<>(docCount);
        // step through all document fields using the current term
        try {
          // get all documents containing
          // the current term in the current field
          final Term termObj = new Term(field, termBr);
          final DocsEnum docsEnum = reader.termDocsEnum(termObj);
          if (docsEnum == null) {
            LOG.trace(
                "Field or term does not exist. field={} term={}",
                field, termBr.utf8ToString());
          } else {
            int docId = docsEnum.nextDoc();
            while (docId != DocsEnum.NO_MORE_DOCS) {
              if (docsEnum.freq() > 0) {
                matchedDocs.add(docId);
              }
              docId = docsEnum.nextDoc();
            }
          }
        } catch (IOException ex) {
          LOG.error(
              "Error enumerating documents. field={} term={}",
              field, termBr.utf8ToString(), ex);
        }
        // now all documents containing the current term are collected
        // store/update documents count for the current term, if any
        if (!matchedDocs.isEmpty()) {
          Integer oldValue =
              map.putIfAbsent(term.clone(), matchedDocs.size());
          if (oldValue != null) {
            final int newValue = matchedDocs.size();
            for (; ; ) {
              if (map.replace(term, oldValue,
                  oldValue + newValue)) {
                break;
              }
              oldValue = map.get(term);
            }
          }
        }
      }
    }
  }

  private static final class IndexFieldDocFreqCalcTarget
      extends TargetFuncCall.TargetFunc<String> {

    /**
     * Target map to put results into.
     */
    private final ConcurrentMap<ByteArray, Integer> map;
    /**
     * Set of terms available in the index.
     */
    private final Set<ByteArray> idxTerms;
    /**
     * Number of documents in index.
     */
    private final int docCount;

    private final AtomicReader reader;

    /**
     * Create a new document frequency calculation target.
     *
     * @param targetMap Map to put results into
     * @param idxDocCount Number of documents in index
     * @param idxTermSet Set of terms in index
     */
    private IndexFieldDocFreqCalcTarget(
        final AtomicReader newReader,
        final ConcurrentNavigableMap<ByteArray, Integer> targetMap,
        final int idxDocCount,
        final Set<ByteArray> idxTermSet) {
      super();
      this.map = targetMap;
      this.docCount = idxDocCount;
      this.idxTerms = idxTermSet;
      this.reader = newReader;
    }

    @Override
    public void call(final String fieldName)
        throws Exception {
      if (fieldName != null) {
        TargetFuncShared.calcDocFreq(this.reader, this.idxTerms,
            this.docCount, fieldName, this.map);
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for document term-frequency calculation.
   */
  private static final class IndexSegmentDocFreqCalcTarget
      extends TargetFuncCall.TargetFunc<AtomicReaderContext> {

    /**
     * Fields to index.
     */
    private final Set<String> currentFields;
    /**
     * Target map to put results into.
     */
    private final ConcurrentMap<ByteArray, Integer> map;
    /**
     * Set of terms available in the index.
     */
    private final Set<ByteArray> idxTerms;
    /**
     * Number of documents in index.
     */
    private final int docCount;

    /**
     * Create a new document frequency calculation target.
     *
     * @param targetMap Map to put results into
     * @param idxDocCount Number of documents in index
     * @param idxTermSet Set of terms in index
     * @param fields List of document fields to iterate over
     */
    private IndexSegmentDocFreqCalcTarget(
        final ConcurrentNavigableMap<ByteArray, Integer> targetMap,
        final int idxDocCount,
        final Set<ByteArray> idxTermSet,
        final Set<String> fields) {
      super();
      this.currentFields = fields;
      this.map = targetMap;
      this.docCount = idxDocCount;
      this.idxTerms = idxTermSet;
    }

    @Override
    public void call(final AtomicReaderContext rContext) {
      if (rContext == null) {
        return;
      }

//      final AtomicReader reader = rContext.reader();
//      DocsEnum docsEnum;
//      BytesRef termBr;
//      Set<Integer> matchedDocs;
//      int docId;

      for (final String field : this.currentFields) {
        TargetFuncShared.calcDocFreq(rContext.reader(), this.idxTerms,
            this.docCount, field, this.map);
      }

//      // step through all terms in index
//      for (final ByteArray term : this.idxTerms) {
//        termBr = new BytesRef(term.bytes);
//        matchedDocs = new HashSet<>(this.docCount);
//        // step through all document fields using the current term
//        for (final String field : this.currentFields) {
//          try {
//            // get all documents containing
//            // the current term in the current field
//            final Term termObj = new Term(field, termBr);
//            docsEnum = reader.termDocsEnum(termObj);
//            if (docsEnum == null) {
//              LOG.trace(
//                  "Field or term does not exist. field={} term={}",
//                  field, termBr.utf8ToString());
//            } else {
//              docId = docsEnum.nextDoc();
//              while (docId != DocsEnum.NO_MORE_DOCS) {
//                if (docsEnum.freq() > 0) {
//                  matchedDocs.add(docId);
//                }
//                docId = docsEnum.nextDoc();
//              }
//            }
//          } catch (IOException ex) {
//            LOG.error(
//                "Error enumerating documents. field={} term={}",
//                field,
//                termBr.utf8ToString(), ex);
//          }
//        }
//        // now all documents containing the current term are collected
//        // store/update documents count for the current term, if any
//        if (!matchedDocs.isEmpty()) {
//          Integer oldValue =
//              this.map.putIfAbsent(term.clone(), matchedDocs.size());
//          if (oldValue != null) {
//            final int newValue = matchedDocs.size();
//            for (; ; ) {
//              if (this.map.replace(term, oldValue,
//                  oldValue + newValue)) {
//                break;
//              }
//              oldValue = this.map.get(term);
//            }
//          }
//        }
//      }
    }
  }

  /**
   * {@link Processing} {@link Target} for collecting index terms on a per field
   * basis. Each Lucene document field will be accessed by a separate {@link
   * TermsEnum}.
   */
  private final class IndexFieldTermsCollectorTarget
      extends TargetFuncCall.TargetFunc<String> {

    private final AtomicReader reader;
    private final ConcurrentNavigableMap<Fun.Tuple2<SerializableByte,
        ByteArray>, Long> target;
    private final TermsEnum termsEnum = TermsEnum.EMPTY;


    public IndexFieldTermsCollectorTarget(final AtomicReader newReader,
        final ConcurrentNavigableMap<Fun.Tuple2<SerializableByte, ByteArray>,
            Long> idxTermsMap) {
      super();
      this.reader = newReader;
      this.target = idxTermsMap;
    }

    @Override
    public void call(final String fieldName)
        throws IOException {
      if (fieldName != null) {
        final SerializableByte fieldId = getFieldId(fieldName);
        final Terms terms = this.reader.terms(fieldName);
        if (terms == null) {
          LOG.warn("({}) No terms. field={}", getName(), fieldName);
        } else {
          try {
            TargetFuncShared.collectTerms(fieldId, terms.iterator(termsEnum),
                this.target);
          } catch (IOException ex) {
            LOG.error("({}) Failed to parse terms. field={}.", getName(),
                fieldName);
            throw ex;
          }
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
     * Target map to store results.
     */
    private final ConcurrentNavigableMap<Fun.Tuple2<
        SerializableByte, ByteArray>, Long> map;

    /**
     * Create a new collector for index terms.
     *
     * @param targetMap Map to put results into
     * @param newFields Lucene index segment provider
     */
    IndexSegmentTermsCollectorTarget(final ConcurrentNavigableMap<Fun.Tuple2<
        SerializableByte, ByteArray>, Long> targetMap,
        final Collection<String> newFields) {
      super();
      assert targetMap != null;
      assert newFields != null && !newFields.isEmpty();

      this.fields = newFields;
      this.map = targetMap;
    }

    @Override
    public void call(final AtomicReaderContext rContext)
        throws IOException {
      if (rContext == null) {
        return;
      }

      TermsEnum termsEnum = TermsEnum.EMPTY;
      final String name = Integer.toString(rContext.hashCode());

      Terms terms;
      BytesRef br;
      for (final String field : this.fields) {
        final SerializableByte fieldId = getFieldId(field);
        terms = rContext.reader().terms(field);
        if (terms == null) {
          LOG.warn("({}) No terms. field={}", getName(), field);
        } else {
          try {
            TargetFuncShared
                .collectTerms(fieldId, terms.iterator(termsEnum), this.map);
          } catch (IOException ex) {
            LOG.error("({}) Failed to parse terms. field={}.", getName(),
                field);
            throw ex;
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
}
