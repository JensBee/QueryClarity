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
import de.unihildesheim.iw.util.concurrent.processing.Target;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;
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
      throws Exception {
    final DirectIndexDataProvider instance = new DirectIndexDataProvider
        (builder.isTemporary);

    // set configuration
    instance.setIndexReader(builder.idxReader);
    instance.setDocumentFields(builder.documentFields);
    instance.setLastIndexCommitGeneration(builder.lastCommitGeneration);
    instance.setStopwords(builder.stopwords);

    // initialize
    try {
      instance.initCache(builder);
    } catch (Buildable.BuilderConfigurationException e) {
      LOG.error("Failed to initialize cache.", e);
      throw new IllegalStateException("Failed to initialize cache.");
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
   */
  private void initCache(final Builder builder)
      throws IOException, Buildable.BuilderConfigurationException {
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

      if (!dbMeta.generationCurrent(getLastIndexCommitGeneration())) {
        throw new IllegalStateException("Invalid database state. " +
            "Index changed since last caching.");
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

      super.idxDfMap = DbMakers.idxDfMapMaker(getDb()).makeOrGet();
    }

    this.persistence.updateMetaData(getDocumentFields(), getStopwords());
    getDb().commit();
  }

  @Override
  public void warmUp()
      throws Exception {
    if (!super.warmed) {
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
  protected void warmUpDocumentFrequencies() {
    // cache document frequency values for each term
    if (super.idxDfMap.isEmpty()) {
      final TimeMeasure tStep = new TimeMeasure().start();
      LOG.info("Cache warming: document frequencies..");
      final List<AtomicReaderContext> arContexts = getIndexReader().getContext()
          .leaves();
      new Processing(
          new Target.TargetFuncCall<>(
              new CollectionSource<>(arContexts),
              new DocFreqCalcTarget(super.idxDfMap,
                  getIdxDocumentIds().size(),
                  getIdxTerms(),
                  getDocumentFields())
          )
      ).process(arContexts.size());
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
  private void buildCache(final Set<String> fields) {
    final Set<String> updatingFields = new HashSet<>(fields);

    boolean update = new HashSet<>(getCachedFieldsMap().keySet()).removeAll(
        updatingFields);
    updatingFields.removeAll(getCachedFieldsMap().keySet());

    if (!updatingFields.isEmpty()) {
      for (String field : updatingFields) {
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
    final IndexTermsCollectorTarget target = new IndexTermsCollectorTarget(
        getIdxTermsMap(), updatingFields);
    final List<AtomicReaderContext> arContexts =
        getIndexReader().getContext().leaves();
    new Processing(
        new Target.TargetFuncCall<>(
            new CollectionSource<>(arContexts),
            target
        )
    ).process(arContexts.size());
    target.closeLocalDb();

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
    final DocumentModel.DocumentModelBuilder dmBuilder
        = new DocumentModel.DocumentModelBuilder(docId);

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
      for (Entry<ByteArray, Long> tfEntry : tfMap.entrySet()) {
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
  public Collection<ByteArray> getDocumentsTermSet(
      final Collection<Integer> docIds)
      throws IOException {
    final Set<Integer> uniqueDocIds = new HashSet<>(docIds);
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<ByteArray> terms = new HashSet<>();
    final DocFieldsTermsEnum dftEnum =
        new DocFieldsTermsEnum(getIndexReader(), getDocumentFields());

    for (Integer docId : uniqueDocIds) {
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
  public boolean documentContains(final int documentId,
      final ByteArray term) {
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
    if (isStopword(term)) {
      // skip stop-words
      return 0;
    }

    final Integer freq;

    if (super.idxDfMap.get(term) == null) {
      final List<AtomicReaderContext> arContexts;
      arContexts = getIndexReader().leaves();

      final Collection<Integer> matchedDocs;
      if (getIdxDocumentIds() == null) {
        matchedDocs = new HashSet<>();
      } else {
        matchedDocs = new HashSet<>(getIdxDocumentIds().size());
      }

      final BytesRef termBref = new BytesRef(term.bytes);
      AtomicReader reader;
      DocsEnum docsEnum;
      int docId;
      for (AtomicReaderContext aReader : arContexts) {
        reader = aReader.reader();
        for (String field : getDocumentFields()) {
          try {
            docsEnum =
                reader.termDocsEnum(new Term(field, termBref));
            if (docsEnum == null) {
              LOG.trace("Field or term does not exist. field={}" +
                      " term={}", field,
                  termBref.utf8ToString()
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
                "Error enumerating documents. field={} term={}",
                field,
                termBref.utf8ToString(), ex);
          }
        }
      }
      super.idxDfMap.put(term.clone(), matchedDocs.size());
      freq = matchedDocs.size();
    } else {
      freq = super.idxDfMap.get(term);
    }

    return freq;
  }

  /**
   * {@link Processing} {@link Target} for document term-frequency calculation.
   */
  private static final class DocFreqCalcTarget
      extends
      Target.TargetFunc<AtomicReaderContext> {

    /**
     * Fields to index.
     */
    private final Set<String> currentFields;
    /**
     * Target map to put results into.
     */
    private final ConcurrentNavigableMap<ByteArray, Integer> map;
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
    private DocFreqCalcTarget(
        final ConcurrentNavigableMap<ByteArray, Integer> targetMap,
        final int idxDocCount,
        final Set<ByteArray> idxTermSet,
        final Set<String> fields) {
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

      AtomicReader reader = rContext.reader();
      DocsEnum docsEnum;
      BytesRef termBref;
      Set<Integer> matchedDocs;
      int docId;

      // step through all terms in index
      for (ByteArray term : this.idxTerms) {
        termBref = new BytesRef(term.bytes);
        matchedDocs = new HashSet<>(this.docCount);
        // step through all document fields using the current term
        for (String field : this.currentFields) {
          try {
            // get all documents containing
            // the current term in the current field
            docsEnum =
                reader.termDocsEnum(new Term(field, termBref));
            if (docsEnum == null) {
              LOG.trace(
                  "Field or term does not exist. field={} term={}",
                  field, termBref.utf8ToString());
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
                "Error enumerating documents. field={} term={}",
                field,
                termBref.utf8ToString(), ex);
          }
        }
        // now all documents containing the current term are collected
        // store/update documents count for the current term, if any
        if (!matchedDocs.isEmpty()) {
          Integer oldValue =
              this.map.putIfAbsent(term.clone(), matchedDocs.
                  size());
          if (oldValue != null) {
            final int newValue = matchedDocs.size();
            for (; ; ) {
              if (this.map.replace(term, oldValue,
                  oldValue + newValue)) {
                break;
              }
              oldValue = this.map.get(term);
            }
          }
        }
      }
    }
  }

  /**
   * {@link Processing} {@link Target} for collecting index terms.
   */
  private final class IndexTermsCollectorTarget
      extends Target.TargetFunc<AtomicReaderContext> {

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
     * Local memory db instance used for caching results.
     */
    private final DB localDb;

    /**
     * Create a new collector for index terms.
     *
     * @param targetMap Map to put results into
     * @param newFields Lucene index segment provider
     */
    IndexTermsCollectorTarget(
        final ConcurrentNavigableMap<Fun.Tuple2<
            SerializableByte, ByteArray>, Long> targetMap,
        final Collection<String> newFields) {
      super();
      this.fields = newFields;
      this.localDb = DBMaker
          .newMemoryDirectDB()
          .compressionEnable()
          .transactionDisable()
          .make();
      this.map = targetMap;
    }

    /**
     * Close the memory db. This should be called after all processes have
     * finished.
     */
    private void closeLocalDb() {
      this.localDb.close();
    }

    /**
     * Push locally cached data to the global map.
     *
     * @param map Local map
     */
    private void commitLocalData(
        final ConcurrentNavigableMap<
            Fun.Tuple2<SerializableByte, ByteArray>, Long> map) {
      for (Entry<Fun.Tuple2<SerializableByte, ByteArray>, Long> entry
          : map.entrySet()) {
        final Fun.Tuple2<SerializableByte, ByteArray> t2 = Fun.t2(
            entry.getKey().a, entry.getKey().b);
        Long oldValue = this.map.putIfAbsent(t2, entry.getValue());

        if (oldValue != null) {
          for (; ; ) {
            final long newValue = entry.getValue();
            if (this.map.replace(
                t2, oldValue, oldValue + newValue)) {
              break;
            }
            oldValue = this.map.get(t2);
          }
        }
      }
    }

    @Override
    public void call(final AtomicReaderContext rContext) {
      if (rContext == null) {
        return;
      }

      TermsEnum termsEnum = TermsEnum.EMPTY;
      final String name = Integer.toString(rContext.hashCode());
      final ConcurrentNavigableMap<
          Fun.Tuple2<SerializableByte, ByteArray>, Long>
          localIdxTermsMap
          = localDb.createTreeMap(name)
          .keySerializer(DbMakers.IDX_TERMSMAP_KEYSERIALIZER)
          .valueSerializer(Serializer.LONG)
          .make();

      try {
        Terms terms;
        BytesRef br;
        for (String field : this.fields) {
          final SerializableByte fieldId
              = getCachedFieldsMap().get(field);
          if (fieldId == null) {
            throw new IllegalStateException("(" + getName()
                + ") Unknown field '" +
                field +
                "' without any id."
            );
          }
          terms = rContext.reader().terms(field);
          try {
            if (terms == null) {
              LOG.warn("({}) No terms. field={}", getName(),
                  field);
            } else {
              // termsEnum is bound to the current field
              termsEnum = terms.iterator(termsEnum);
              br = termsEnum.next();
              while (br != null) {
                if (termsEnum.seekExact(br)) {
                  // get the total term frequency of the current term
                  // across all documents and the current field
                  final long ttf = termsEnum.totalTermFreq();
                  // add value up for all fields
                  final Fun.Tuple2<SerializableByte, ByteArray>
                      fieldTerm
                      = Fun.t2(fieldId, BytesRefUtils
                      .toByteArray(br));

                  try {
                    Long oldValue =
                        localIdxTermsMap.putIfAbsent(
                            fieldTerm, ttf);
                    if (oldValue != null) {
                      for (; ; ) {
                        if (localIdxTermsMap
                            .replace(fieldTerm,
                                oldValue,
                                oldValue +
                                    ttf
                            )) {
                          break;
                        }
                        oldValue = localIdxTermsMap
                            .get(fieldTerm);
                      }
                    }
                  } catch (Exception ex) {
                    LOG.error("({}) Error: f={} b={} v={}",
                        getName(),
                        field, br.bytes, ttf, ex);
                    throw ex;
                  }
                }
                br = termsEnum.next();
              }

              commitLocalData(localIdxTermsMap);
              localIdxTermsMap.clear();
            }
          } catch (IOException ex) {
            LOG.error("({}) Failed to parse terms. field={}.",
                getName(),
                field, ex);
          }
        }
      } catch (IOException ex) {
        LOG.error("Index error.", ex);
      } finally {
        this.localDb.delete(name);
      }
    }
  }

  public final static class Builder
      extends AbstractIndexDataProviderBuilder<Builder,
      DirectIndexDataProvider> {

    /**
     * Default constructor.
     */
    public Builder() {
      super(IDENTIFIER);
    }

    @Override
    public DirectIndexDataProvider build()
        throws Exception {
      validate();
      return DirectIndexDataProvider.build(this);
    }

    @Override
    public void validate()
        throws BuilderConfigurationException, IOException {
      super.validate();
      validatePersistenceBuilder();
    }
  }
}
