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
package de.unihildesheim.lucene.index;

import de.unihildesheim.ByteArray;
import de.unihildesheim.Persistence;
import de.unihildesheim.SerializableByte;
import de.unihildesheim.Tuple;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.util.BytesRefUtil;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
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

/**
 * {@link IndexDataProvider} implementation directly accessing the Lucene
 * index.
 *
 * @author Jens Bertram
 */
public final class DirectIndexDataProvider extends AbstractIndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DirectIndexDataProvider.class);

  /**
   * Wrapper for persistent data storage.
   */
  private Persistence pData;

  /**
   * Prefix used to store configuration.
   */
  public static final String IDENTIFIER = "DirectIDP";

  /**
   * Default constructor using the parameters set by {@link Environment}.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  public DirectIndexDataProvider() throws IOException {
    this(false);
  }

  /**
   * Custom constructor allowing to set the parameters manually and optionally
   * creating a temporary instance.
   *
   * @param temporaray If true, all persistent data will be temporary
   * @throws IOException Thrown on low-level I/O errors
   */
  protected DirectIndexDataProvider(final boolean temporaray) throws
          IOException {
    super(IDENTIFIER, temporaray);
    setStopwordsFromEnvironment();
  }

  @Override
  public void createCache(final String name) throws
          IOException, Environment.NoIndexException {
    initCache(name, true, true);
  }

  @Override
  public void loadOrCreateCache(final String name) throws
          IOException, Environment.NoIndexException {
    initCache(name, false, true);
  }

  @Override
  public void loadCache(final String name) throws
          IOException, Environment.NoIndexException {
    initCache(name, false, false);
  }

  /**
   * Initialize the database with a default configuration.
   *
   * @param name Database name
   * @param createNew If true, a new database will be created. Throws an
   * error, if a database with the current name already exists.
   * @param createIfNotFound If true, a new database will be created, if none
   * with the given name exists
   * @throws IOException Thrown on low-level I/O errors
   * @throws Environment.NoIndexException Thrown, if no index is provided in
   * the {@link Environment}
   */
  @SuppressWarnings("CollectionWithoutInitialCapacity")
  private void initCache(final String name, final boolean createNew,
          final boolean createIfNotFound) throws
          IOException, Environment.NoIndexException {

    boolean clearCache = false;

    final Tuple.Tuple2<Persistence, Boolean> pSetup = super.getPersistence(
            IDENTIFIER + "_" + name, createNew, createIfNotFound);
    final boolean create = pSetup.b;

    this.pData = pSetup.a;
    final Persistence.StorageMeta dbMeta = this.pData.getMetaData();

    if (create) {
      clearCache = true;
    } else {
      if (!super.db.exists(DbMakers.Stores.IDX_TERMS_MAP.name())) {
        throw new IllegalStateException(
                "Invalid database state. 'idxTermsMap' does not exist.");
      }

      if (!dbMeta.generationCurrent()) {
        throw new IllegalStateException("Invalid database state. "
                + "Index changed since last caching.");
      }
      if (!dbMeta.fieldsCurrent()) {
        LOG.info("Fields changed. Caches needs to be rebuild.");
        clearCache = true;
      }
    }

    if (super.isTemporary()) {
      super.cachedFieldsMap = new HashMap<>();
    } else {
      super.cachedFieldsMap = DbMakers.cachedFieldsMapMaker(super.db).
              makeOrGet();
    }

    LOG.debug("Fields: cached={} active={}", super.cachedFieldsMap.keySet(),
            Arrays.toString(Environment.getFields()));

    super.idxTermsMap = DbMakers.idxTermsMapMkr(super.db).makeOrGet();

    if (clearCache) {
      clearCache();
      buildCache(Environment.getFields());
    } else {
      if (dbMeta.stopWordsCurrent()) {
        // load terms index
        super.idxTerms = DbMakers.idxTermsMaker(super.db).makeOrGet();
        LOG.info("Stopwords unchanged. "
                + "Loaded index terms cache with {} entries.", super.idxTerms.
                size());

        // try load overall term frequency
        super.idxTf = super.db.getAtomicLong(DbMakers.Caches.IDX_TF.name()).
                get();
        if (super.idxTf == 0L) {
          super.idxTf = null;
        }
      } else {
        // reset terms index
        LOG.info("Stopwords changed. Deleting index terms cache.");
        super.db.delete(DbMakers.Caches.IDX_TERMS.name());
        super.idxTerms = DbMakers.idxTermsMaker(super.db).make();

        // reset overall term frequency
        super.idxTf = null;
      }

      super.idxDfMap = DbMakers.idxDfMapMaker(super.db).makeOrGet();
    }

    this.pData.updateMetaData();
    super.db.commit();
  }

  @Override
  public void warmUp() throws Exception {
    if (!super.warmed) {
      super.warmUp();

      if (super.idxTf == null || super.idxTf == 0) {
        throw new IllegalStateException("Zero term frequency.");
      }
      if (super.idxDocumentIds.isEmpty()) {
        throw new IllegalStateException("Zero document ids.");
      }
      if (super.idxTerms.isEmpty()) {
        throw new IllegalStateException("Zero terms.");
      }

      LOG.info("Writing data.");
      this.pData.updateMetaData();
      super.db.commit();
      LOG.debug("Writing data - done.");
    }
  }

  @Override
  protected void warmUpDocumentFrequencies() throws
          Environment.NoIndexException {
    // cache document frequency values for each term
    if (super.idxDfMap.isEmpty()) {
      final TimeMeasure tStep = new TimeMeasure().start();
      LOG.info("Cache warming: document frequencies..");
      final List<AtomicReaderContext> arContexts = Environment.
              getIndexReader().getContext().leaves();
      new Processing(
              new Target.TargetFuncCall<>(
                      new CollectionSource<>(arContexts),
                      new DocFreqCalcTarget(super.idxDfMap,
                              super.idxDocumentIds.size(), super.idxTerms,
                              Environment.getFields())
              )).process(arContexts.size());
      LOG.info("Cache warming: calculating document frequencies "
              + "for {} documents and {} terms took {}.",
              super.idxDocumentIds.size(), super.idxTerms.size(),
              tStep.stop().getTimeString());
    } else {
      LOG.info("Cache warming: Document frequencies "
              + "for {} documents and {} terms already loaded.",
              super.idxDocumentIds.size(), super.idxTerms.size());
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
  private void buildCache(final String[] fields) throws
          Environment.NoIndexException {
    final Set<String> updatingFields = new HashSet<>(Arrays.asList(fields));

    boolean update = new HashSet<>(this.cachedFieldsMap.keySet()).removeAll(
            updatingFields);
    updatingFields.removeAll(this.cachedFieldsMap.keySet());

    if (!updatingFields.isEmpty()) {
      final Collection<SerializableByte> keys = new HashSet<>(
              this.cachedFieldsMap.values());
      for (String field : updatingFields) {
        for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
          final SerializableByte sByte = new SerializableByte(i);
          if (!keys.contains(sByte)) {
            this.cachedFieldsMap.put(field, sByte);
            keys.add(sByte);
            break;
          }
        }
      }
    }

    if (!update && updatingFields.isEmpty()) {
      return;
    }

    if (updatingFields.isEmpty()) {
      LOG.info("Updating persistent index term cache. {} -> {}",
              this.cachedFieldsMap.keySet(), fields);
    } else {
      LOG.info("Building persistent index term cache. {}", updatingFields);
    }
    final IndexTermsCollectorTarget target = new IndexTermsCollectorTarget(
            this.idxTermsMap, updatingFields);
    final List<AtomicReaderContext> arContexts = Environment.getIndexReader().
            getContext().leaves();
    new Processing(
            new Target.TargetFuncCall<>(
                    new CollectionSource<>(arContexts),
                    target
            )).process(arContexts.size());
    target.closeLocalDb();

    LOG.info("Writing data.");

    this.db.commit();
  }

  /**
   * {@inheritDoc} Deleted documents will be removed from the list.
   *
   * @return Unique collection of all (non-deleted) document ids
   */
  @Override
  protected Collection<Integer> getDocumentIds() {
    Collection<Integer> ret;
    if (super.idxDocumentIds == null) {
      try {
        final int maxDoc = Environment.getIndexReader().maxDoc();
        super.idxDocumentIds = new ArrayList<>(maxDoc);

        final Bits liveDocs = MultiFields.getLiveDocs(Environment.
                getIndexReader());
        for (int i = 0; i < maxDoc; i++) {
          if (liveDocs != null && !liveDocs.get(i)) {
            continue;
          }
          super.idxDocumentIds.add(i);
        }
        ret = Collections.unmodifiableCollection(super.idxDocumentIds);
      } catch (Environment.NoIndexException ex) {
        LOG.error("Failed to get document-ids.", ex);
        ret = Collections.<Integer>emptyList();
      }
    } else {
      ret = Collections.unmodifiableCollection(super.idxDocumentIds);
    }
    return ret;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped while creating the model.
   *
   * @param docId Document id to create the model from
   * @return Model for the given document
   */
  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkDocId(docId);
    final DocumentModel.DocumentModelBuilder dmBuilder
            = new DocumentModel.DocumentModelBuilder(docId);

    try {
      final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(docId);
      @SuppressWarnings("CollectionWithoutInitialCapacity")
      final Map<ByteArray, Long> tfMap = new HashMap<>();

      BytesRef bytesRef = dftEnum.next();
      while (bytesRef != null) {
        final ByteArray byteArray = BytesRefUtil.toByteArray(bytesRef);
        // skip stop-words
        if (!super.stopWords.contains(byteArray)) {
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
        dmBuilder.setTermFrequency(tfEntry.getKey(), tfEntry.getValue());
      }
      return dmBuilder.getModel();
    } catch (IOException ex) {
      LOG.error("Caught exception while iterating document terms. "
              + "docId=" + docId + ".", ex);
    } catch (Environment.NoIndexException ex) {
      LOG.error("Failed to create document-model.", ex);
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
          final Collection<Integer> docIds) {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<ByteArray> terms = new HashSet<>();
    try {
      for (Integer docId : new HashSet<>(docIds)) {
        checkDocId(docId);
        try {
          final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(docId);
          BytesRef br = dftEnum.next();
          ByteArray termBytes;
          while (br != null) {
            termBytes = BytesRefUtil.toByteArray(br);
            // skip adding stop-words
            if (!super.stopWords.contains(termBytes)) {
              terms.add(termBytes);
            }
            br = dftEnum.next();
          }
        } catch (IOException ex) {
          LOG.error("Caught exception while iterating document terms. "
                  + "docId=" + docId + ".", ex);
        }
      }
    } catch (Environment.NoIndexException ex) {
      LOG.error("Failed to get document terms.", ex);
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
    if (super.stopWords.contains(term)) {
      // skip stop-words
      return false;
    }

    checkDocId(documentId);
    try {
      final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(documentId);
      BytesRef br = dftEnum.next();
      while (br != null) {
        if (BytesRefUtil.bytesEquals(br, term)) {
          return true;
        }
        br = dftEnum.next();
      }
    } catch (IOException ex) {
      LOG.error("Caught exception while iterating document terms. "
              + "docId=" + documentId + ".", ex);
    } catch (Environment.NoIndexException ex) {
      LOG.error("Failed to get document terms.", ex);
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
    if (super.stopWords.contains(term)) {
      // skip stop-words
      return 0;
    }

    final Integer freq;

    if (super.idxDfMap.get(term) == null) {
      final List<AtomicReaderContext> arContexts;
      try {
        arContexts = Environment.getIndexReader().leaves();
      } catch (Environment.NoIndexException ex) {
        LOG.error("Failed to get index reader.", ex);
        return 0;
      }

      final Collection<Integer> matchedDocs = new HashSet<>(
              super.idxDocumentIds.size());
      final BytesRef termBref = new BytesRef(term.bytes);
      AtomicReader reader;
      DocsEnum docsEnum;
      int docId;
      for (AtomicReaderContext aReader : arContexts) {
        reader = aReader.reader();
        for (String field : Environment.getFields()) {
          try {
            docsEnum = reader.termDocsEnum(new Term(field, termBref));
            if (docsEnum == null) {
              LOG.trace("Field or term does not exist. field={} term={}",
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
            LOG.error("Error enumerating documents. field={} term={}", field,
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
          for (;;) {
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
              Fun.Tuple2<SerializableByte, ByteArray>, Long> localIdxTermsMap
              = localDb.createTreeMap(name)
              .keySerializer(DbMakers.idxTermsMapKeySerializer)
              .valueSerializer(Serializer.LONG)
              .make();

      try {
        Terms terms;
        BytesRef br;
        for (String field : this.fields) {
          final SerializableByte fieldId
                  = DirectIndexDataProvider.this.cachedFieldsMap.get(field);
          if (fieldId == null) {
            throw new IllegalStateException("(" + getName()
                    + ") Unknown field '" + field + "' without any id."
            );
          }
          terms = rContext.reader().terms(field);
          try {
            if (terms == null) {
              LOG.warn("({}) No terms. field={}", getName(), field);
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
                  final Fun.Tuple2<SerializableByte, ByteArray> fieldTerm
                          = Fun.t2(fieldId, BytesRefUtil.toByteArray(br));

                  try {
                    Long oldValue = localIdxTermsMap.putIfAbsent(
                            fieldTerm, ttf);
                    if (oldValue != null) {
                      for (;;) {
                        if (localIdxTermsMap.replace(fieldTerm,
                                oldValue, oldValue + ttf)) {
                          break;
                        }
                        oldValue = localIdxTermsMap.get(fieldTerm);
                      }
                    }
                  } catch (Exception ex) {
                    LOG.error("({}) Error: f={} b={} v={}", getName(),
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
            LOG.error("({}) Failed to parse terms. field={}.", getName(),
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

  /**
   * {@link Processing} {@link Target} for document term-frequency
   * calculation.
   */
  private static final class DocFreqCalcTarget extends
          Target.TargetFunc<AtomicReaderContext> {

    /**
     * Fields to index.
     */
    private final String[] currentFields;
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
            final String[] fields) {
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
            docsEnum = reader.termDocsEnum(new Term(field, termBref));
            if (docsEnum == null) {
              LOG.trace("Field or term does not exist. field={} term={}",
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
            LOG.error("Error enumerating documents. field={} term={}", field,
                    termBref.utf8ToString(), ex);
          }
        }
        // now all documents containing the current term are collected
        // store/update documents count for the current term, if any
        if (!matchedDocs.isEmpty()) {
          Integer oldValue = this.map.putIfAbsent(term.clone(), matchedDocs.
                  size());
          if (oldValue != null) {
            final int newValue = matchedDocs.size();
            for (;;) {
              if (this.map.replace(term, oldValue, oldValue + newValue)) {
                break;
              }
              oldValue = this.map.get(term);
            }
          }
        }
      }
    }
  }
}
