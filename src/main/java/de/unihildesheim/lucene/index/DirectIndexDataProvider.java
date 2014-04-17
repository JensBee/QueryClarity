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
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.util.BytesRefUtil;
import de.unihildesheim.util.ByteArrayUtil;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DB.BTreeMapMaker;
import org.mapdb.DB.BTreeSetMaker;
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
public final class DirectIndexDataProvider implements IndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DirectIndexDataProvider.class);

  /**
   * Persistent cached collection of all index terms mapped by document field.
   * Mapping is <tt>(Field, Term)</tt> to <tt>Frequency</tt>. Fields are
   * indexed by {@link #cachedFieldsMap}.
   */
  private ConcurrentNavigableMap<Fun.Tuple2<
          SerializableByte, ByteArray>, Long> idxTermsMap
          = null;
  /**
   * Serializer to use for {@link idxTermsMap}.
   */
  final BTreeKeySerializer idxTermsMapKeySerializer;

  /**
   * Transient cached collection of all index terms.
   */
  private Set<ByteArray> idxTerms = null;
  /**
   * Configuration for {@link #idxTerms}.
   */
  private BTreeSetMaker idxTermsMaker;

  /**
   * Transient cached collection of all (non deleted) document-ids.
   */
  private Collection<Integer> idxDocumentIds = null;

  /**
   * Transient cached document-frequency map for all terms in index.
   */
  private ConcurrentNavigableMap<ByteArray, Integer> idxDfMap = null;
  /**
   * Configuration for {@link #idxDfMap}.
   */
  private BTreeMapMaker idxDfMapMaker;

  /**
   * Transient cached overall term frequency of the index.
   */
  private Long idxTf = null;
  /**
   * Persistent disk backed storage backend.
   */
  DB db;
  /**
   * Flag indicating, if this instance is temporary (no data is hold
   * persistent).
   */
  private boolean isTemporary = false;
  /**
   * Wrapper for persistent data storage.
   */
  private Persistence pData;

  /**
   * Prefix used to store configuration.
   */
  private static final String IDENTIFIER = "DirectIDP";

  /**
   * List of stop-words to exclude from term frequency calculations.
   */
  private static Collection<ByteArray> stopWords = Collections.
          <ByteArray>emptySet();

  /**
   * List of fields cached by this instance. Mapping of field name to id
   * value.
   */
  Map<String, SerializableByte> cachedFieldsMap;

  /**
   * Local copy of fields currently set by {@link Environment}.
   */
  private String[] currentFields;

  /**
   * Flag indicating, if caches are filled (warmed).
   */
  private boolean warmed = false;

  /**
   * Ids of persistent data held in the database.
   */
  private enum Stores {

    /**
     * Mapping of all index terms.
     */
    IDX_TERMS_MAP,
    /**
     * Stopwords used to build last transient cache.
     */
    STOPWORDS,
    /**
     * Fields used to build last transient cache.
     */
    FIELDS,
  }

  /**
   * Ids of temporary data caches held in the database.
   */
  private enum Caches {

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
   * Keys to properties stored in the {@link Environment}.
   */
  private enum Properties {

    /**
     * Number of caches created by this instance.
     */
    numberOfCaches("numberOfCaches"),
    /**
     * Prefix for cache related informations.
     */
    cachePrefix("cache_"),
    /**
     * Suffix for cache path.
     */
    cachePathSuffix("_path"),
    /**
     * Suffix for cache generation.
     */
    cacheGenSuffix("_gen");

    /**
     * Final string written to properties.
     */
    private final String key;

    /**
     * Create a new property item.
     *
     * @param str String representation of the property
     */
    private Properties(final String str) {
      this.key = str;
    }

    @Override
    public String toString() {
      return key;
    }
  }

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
    if (!Environment.isTestRun()) {
      this.isTemporary = temporaray;
    } else {
      this.isTemporary = true;
    }

    this.idxTermsMapKeySerializer
            = new BTreeKeySerializer.Tuple2KeySerializer<>(
                    SerializableByte.COMPARATOR, SerializableByte.SERIALIZER,
                    ByteArray.SERIALIZER);

    setStopwordsFromEnvironment();
  }

  @Override
  public void createCache(final String name) throws
          IOException {
    initCache(name, true, true);
  }

  @Override
  public void loadOrCreateCache(final String name) throws
          IOException {
    initCache(name, false, true);
  }

  @Override
  public void loadCache(final String name) throws
          IOException {
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
   */
  @SuppressWarnings("CollectionWithoutInitialCapacity")
  private void initCache(final String name, final boolean createNew,
          final boolean createIfNotFound) throws IOException {

    boolean create = createNew;
    boolean clearCache = false;
    final Persistence.Builder psb;
    this.currentFields = Environment.getFields();

    if (this.isTemporary) {
      LOG.warn("Caches are temporary!");
      psb = new Persistence.Builder(IDENTIFIER
              + "_" + name + "_" + RandomValue.getString(6)).temporary();
    } else {
      psb = new Persistence.Builder(IDENTIFIER + "_" + name);
    }

    psb.getMaker()
            .transactionDisable()
            .commitFileSyncDisable()
            .asyncWriteEnable()
            .asyncWriteFlushDelay(100)
            .mmapFileEnableIfSupported()
            .closeOnJvmShutdown();
    if (createNew) {
      this.pData = psb.make();
      create = true;
    } else if (!createIfNotFound) {
      this.pData = psb.get();
    } else {
      if (!psb.exists()) {
        create = true;
      }
      this.pData = psb.makeOrGet();
    }
    this.db = this.pData.db;

    final Persistence.StorageMeta dbMeta = this.pData.getMetaData();

    final DB.BTreeMapMaker idxTermsMapMkr = this.db
            .createTreeMap(Stores.IDX_TERMS_MAP.name())
            .keySerializer(this.idxTermsMapKeySerializer)
            .valueSerializer(Serializer.LONG)
            .nodeSize(100);
    this.idxTermsMaker = this.db
            .createTreeSet(Caches.IDX_TERMS.name()).
            serializer(new BTreeKeySerializer.BasicKeySerializer(
                            ByteArray.SERIALIZER))
            .counterEnable();
    this.idxDfMapMaker = this.db
            .createTreeMap(Caches.IDX_DFMAP.name())
            .keySerializerWrap(ByteArray.SERIALIZER)
            .valueSerializer(Serializer.INTEGER)
            .counterEnable();
    final DB.BTreeMapMaker cachedFieldsMapMaker = this.db
            .createTreeMap(Caches.IDX_FIELDS.name())
            .keySerializer(BTreeKeySerializer.STRING)
            .valueSerializer(SerializableByte.SERIALIZER)
            .counterEnable();

    if (create) {
      clearCache = true;
    } else {
      if (!this.db.exists(Stores.IDX_TERMS_MAP.name())) {
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

    if (this.isTemporary) {
      this.cachedFieldsMap = new HashMap<>();
    } else {
      this.cachedFieldsMap = cachedFieldsMapMaker.makeOrGet();
    }

    LOG.debug("Fields: cached={} active={}", this.cachedFieldsMap.keySet(),
            Arrays.toString(this.currentFields));

    this.idxTermsMap = idxTermsMapMkr.makeOrGet();

    if (clearCache) {
      clearCache();
      buildCache(this.currentFields);
    } else {
      if (dbMeta.stopWordsCurrent()) {
        // load terms index
        this.idxTerms = this.idxTermsMaker.makeOrGet();
        LOG.info("Stopwords unchanged. "
                + "Loaded index terms cache with {} entries.", this.idxTerms.
                size());

        // try load overall term frequency
        this.idxTf = this.db.getAtomicLong(Caches.IDX_TF.name()).get();
        if (this.idxTf == 0L) {
          this.idxTf = null;
        }
      } else {
        // reset terms index
        LOG.info("Stopwords changed. Deleting index terms cache.");
        this.db.delete(Caches.IDX_TERMS.name());
        this.idxTerms = this.idxTermsMaker.make();

        // reset overall term frequency
        this.idxTf = null;
      }

      this.idxDfMap = this.idxDfMapMaker.makeOrGet();
    }

    this.pData.updateMetaData();
    this.db.commit();
  }

  @Override
  public void warmUp() {
    if (this.warmed) {
      LOG.info("Caches are up to date.");
      return;
    }
    setStopwordsFromEnvironment();
    final TimeMeasure tOverAll = new TimeMeasure().start();
    final TimeMeasure tStep = new TimeMeasure();

    // cache all index terms
    if (this.idxTerms.isEmpty()) {
      tStep.start();
      LOG.info("Cache warming: terms..");
      getTerms(); // caches this.idxTerms
      LOG.info("Cache warming: collecting {} unique terms took {}.",
              this.idxTerms.size(), tStep.stop().getTimeString());
      if (this.idxTerms.isEmpty()) {
        throw new IllegalStateException("Zero terms.");
      }
    } else {
      LOG.info("Cache warming: {} Unique terms already loaded.",
              this.idxTerms.size());
    }

    // collect index term frequency
    if (this.idxTf == null) {
      tStep.start();
      LOG.info("Cache warming: index term frequencies..");
      getTermFrequency(); // caches this.idxTf
      LOG.info("Cache warming: index term frequency calculation "
              + "for {} terms took {}.", this.idxTerms.size(), tStep.stop().
              getTimeString());
      if (this.idxTf == 0) {
        throw new IllegalStateException("Zero term frequency.");
      }
    } else {
      LOG.info("Cache warming: index term frequency already loaded: {}",
              this.idxTf);
    }

    // cache document ids
    tStep.start();
    LOG.info("Cache warming: documents..");
    getDocumentIds(); // caches this.idxDocumentIds
    LOG.info("Cache warming: collecting {} documents took {}.",
            this.idxDocumentIds.size(), tStep.stop().getTimeString());
    if (this.idxDocumentIds.isEmpty()) {
      throw new IllegalStateException("Zero document ids.");
    }

    // cache document frequency values for each term
    if (this.idxDfMap.isEmpty()) {
      tStep.start();
      LOG.info("Cache warming: document frequencies..");
      new Processing(
              new Target.TargetFuncCall<>(
                      new CollectionSource<>(
                              Environment.getIndexReader().
                              getContext().leaves()),
                      new DocFreqCalcTarget(this.idxDfMap, this.currentFields)
              )).process();
      LOG.info("Cache warming: calculating document frequencies "
              + "for {} documents and {} terms took {}.", this.idxDocumentIds.
              size(), this.idxTerms.size(), tStep.stop().getTimeString());
    } else {
      LOG.info("Cache warming: Document frequencies "
              + "for {} documents and {} terms already loaded.",
              this.idxDocumentIds.size(), this.idxTerms.size());
    }

    LOG.info("Cache warmed. Took {}.", tOverAll.stop().getTimeString());
    this.pData.updateMetaData();
    this.warmed = true;

    LOG.info("Writing data.");
    this.db.commit();
    LOG.debug("Writing data - done.");
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
   * Get the id for a named field.
   *
   * @param fieldName Field name
   * @return Id of the field
   */
  SerializableByte getFieldId(final String fieldName) {
    final SerializableByte fieldId = this.cachedFieldsMap.get(fieldName);
    if (fieldId == null) {
      throw new IllegalStateException("Unknown field '" + fieldName
              + "'. No id found.");
    }
    return fieldId;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped.
   */
  @Override
  public long getTermFrequency() {
    if (this.idxTf == null) {
      LOG.info("Building term frequency index.");
      this.idxTf = 0L;

      SerializableByte fieldId;
      for (String field : this.currentFields) {
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
      if (!DirectIndexDataProvider.stopWords.isEmpty()) {
        Long tf;
        for (ByteArray stopWord : DirectIndexDataProvider.stopWords) {
          tf = _getTermFrequency(stopWord);
          if (tf != null) {
            this.idxTf -= tf;
          }
        }
      }
      this.db.getAtomicLong(Caches.IDX_TF.name()).set(this.idxTf);
    }
    return this.idxTf;
  }

  /**
   * Get the term frequency including stop-words.
   *
   * @param term Term to lookup
   * @return Term frequency
   */
  private Long _getTermFrequency(final ByteArray term) {
    Long tf = 0L;
    for (String field : this.currentFields) {
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
  public Long getTermFrequency(final ByteArray term) {
    if (DirectIndexDataProvider.stopWords.contains(term)) {
      // skip stop-words
      return 0L;
    }
    return _getTermFrequency(term);
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>0</tt>).
   */
  @Override
  public double getRelativeTermFrequency(final ByteArray term) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    if (DirectIndexDataProvider.stopWords.contains(term)) {
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
  public void dispose() {
    if (!this.db.isClosed()) {
      LOG.info("Closing database.");
      this.db.commit();
      this.db.compact();
      this.db.close();
    }
  }

  /**
   * Build a cache of the current index.
   *
   * @param fields List of fields to cache
   */
  private void buildCache(final String[] fields) {
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

//    // update list of cached fields
//    Iterator<String> ufIt = updatingFields.iterator();
//    while (ufIt.hasNext()) {
//      final String field = ufIt.next();
//      if (this.cachedFieldsMap.containsKey(field)) {
//        ufIt.remove();
//      } else {
//        Collection<SerializableByte> keys = this.cachedFieldsMap.values();
//        for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
//          final SerializableByte sByte = new SerializableByte(i);
//          if (!keys.contains(sByte)) {
//            this.cachedFieldsMap.put(field, sByte);
//            break;
//          }
//        }
//      }
//    }
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

    new Processing(
            new Target.TargetFuncCall<>(
                    new CollectionSource<>(
                            Environment.getIndexReader().
                            getContext().leaves()),
                    target
            )).process();
    target.closeLocalDb();

    LOG.info("Writing data.");

    this.db.commit();
  }

  /**
   * Collect and cache all index terms. Stop-words will be removed from the
   * list.
   *
   * @return Unique collection of all terms in index
   */
  private Collection<ByteArray> getTerms() {
    if (this.idxTerms.isEmpty()) {
      LOG.info("Building transient index term cache.");

      if (this.currentFields.length > 1) {
        new Processing(new Target.TargetFuncCall<>(
                new CollectionSource<>(Arrays.asList(this.currentFields)),
                new TermCollectorTarget(this.idxTermsMap.keySet(),
                        this.idxTerms)
        )).process();
      } else {
        for (String field : this.currentFields) {
          Iterator<ByteArray> bytesIt = Fun.filter(this.idxTermsMap.keySet(),
                  getFieldId(field)).iterator();

          while (bytesIt.hasNext()) {
            this.idxTerms.add(bytesIt.next().clone());
          }
        }
      }
      this.idxTerms.removeAll(DirectIndexDataProvider.stopWords);
    }
    return Collections.unmodifiableCollection(this.idxTerms);
  }

  @Override
  public Iterator<ByteArray> getTermsIterator() {
    return getTerms().iterator();
  }

  @Override
  public Source<ByteArray> getTermsSource() {
    return new CollectionSource<>(getTerms());
  }

  /**
   * Collect and cache all document-ids from the index.
   *
   * @return Unique collection of all (non-deleted) document ids
   */
  private Collection<Integer> getDocumentIds() {
    if (this.idxDocumentIds == null) {
      final int maxDoc = Environment.getIndexReader().maxDoc();
      this.idxDocumentIds = new ArrayList<>(maxDoc);

      final Bits liveDocs = MultiFields.getLiveDocs(Environment.
              getIndexReader());
      for (int i = 0; i < maxDoc; i++) {
        if (liveDocs != null && !liveDocs.get(i)) {
          continue;
        }
        this.idxDocumentIds.add(i);
      }
    }
    return Collections.unmodifiableCollection(this.idxDocumentIds);
  }

  @Override
  public Iterator<Integer> getDocumentIdIterator() {
    return getDocumentIds().iterator();
  }

  @Override
  public Source<Integer> getDocumentIdSource() {
    return new CollectionSource<>(getDocumentIds());
  }

  @Override
  public long getUniqueTermsCount() {
    return getTerms().size();
  }

  /**
   * {@inheritDoc} Stop-words will be skipped while creating the model.
   */
  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkDocId(docId);
    final DocumentModel.DocumentModelBuilder dmBuilder
            = new DocumentModel.DocumentModelBuilder(docId);

    try {
      final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(docId);
      BytesRef bytesRef = dftEnum.next();
      @SuppressWarnings(
              "CollectionWithoutInitialCapacity")
      final Map<ByteArray, AtomicLong> tfMap = new HashMap<>();
      while (bytesRef != null) {
        final ByteArray byteArray = BytesRefUtil.toByteArray(bytesRef);
        // skip stop-words
        if (!DirectIndexDataProvider.stopWords.contains(byteArray)) {
          if (tfMap.containsKey(byteArray)) {
            tfMap.get(byteArray).getAndAdd(dftEnum.getTotalTermFreq());
          } else {
            tfMap.put(byteArray, new AtomicLong(dftEnum.getTotalTermFreq()));
          }
        }
        bytesRef = dftEnum.next();
      }
      for (Entry<ByteArray, AtomicLong> tfEntry : tfMap.entrySet()) {
        dmBuilder.setTermFrequency(tfEntry.getKey(), tfEntry.getValue().
                longValue());
      }
      return dmBuilder.getModel();
    } catch (IOException ex) {
      LOG.error("Caught exception while iterating document terms. "
              + "docId=" + docId + ".", ex);
    }
    return null;
  }

  @Override
  public boolean hasDocument(final Integer docId) {
    final int maxDoc = Environment.getIndexReader().maxDoc();

    if (docId <= (maxDoc - 1) && docId >= 0) {
      final Bits liveDocs = MultiFields.getLiveDocs(Environment.
              getIndexReader());
      return liveDocs == null || liveDocs.get(docId);
    }
    return false;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped.
   */
  @Override
  public Collection<ByteArray> getDocumentsTermSet(
          final Collection<Integer> docIds) {
    @SuppressWarnings(
            "CollectionWithoutInitialCapacity")
    final Collection<ByteArray> terms = new HashSet<>();
    for (Integer docId : new HashSet<>(docIds)) {
      checkDocId(docId);
      try {
        final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(docId);
        BytesRef br = dftEnum.next();
        ByteArray termBytes;
        while (br != null) {
          termBytes = BytesRefUtil.toByteArray(br);
          // skip adding stop-words
          if (!DirectIndexDataProvider.stopWords.contains(termBytes)) {
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

  @Override
  public long getDocumentCount() {
    return getDocumentIds().size();
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is always
   * <tt>false</tt>).
   */
  @Override
  public boolean documentContains(final int documentId, final ByteArray term) {
    if (DirectIndexDataProvider.stopWords.contains(term)) {
      // skip stop-words
      return false;
    }

    checkDocId(documentId);
    try {
      final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(documentId);
      BytesRef br = dftEnum.next();
      while (br != null) {
        if (term.compareBytes(br.bytes) == 0) {
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
   * Clear all dynamic caches. This must be called, if the fields or
   * stop-words have changed.
   */
  private void clearCache() {
    LOG.info("Clearing temporary caches.");

    // index terms cache (content depends on current fields & stopwords)
    if (this.db.exists(Caches.IDX_TERMS.name())) {
      this.db.delete(Caches.IDX_TERMS.name());
    }
    this.idxTerms = this.idxTermsMaker.make();

    // document term-frequency map
    // (content depends on current fields)
    if (this.db.exists(Caches.IDX_DFMAP.name())) {
      this.db.delete(Caches.IDX_DFMAP.name());
    }
    this.idxDfMap = this.idxDfMapMaker.make();

    this.idxTf = null;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>0</tt>).
   */
  @Override
  public int getDocumentFrequency(final ByteArray term) {
    if (DirectIndexDataProvider.stopWords.contains(term)) {
      // skip stop-words
      return 0;
    }

    if (this.idxDfMap.get(term) == null) {
      final BytesRef termBr = new BytesRef(term.bytes);
      @SuppressWarnings(
              "CollectionWithoutInitialCapacity")
      final Collection<Integer> matchedDocs = new HashSet<>();
      Terms terms;
      TermsEnum termsEnum = TermsEnum.EMPTY;
      DocsEnum docsEnum;

      for (String field : this.currentFields) {
        try {
          for (AtomicReaderContext aReader : Environment.getIndexReader().
                  leaves()) {
            terms = aReader.reader().fields().terms(field);
            if (terms != null) {
              termsEnum = terms.iterator(termsEnum);
              if (terms.iterator(termsEnum).seekExact(termBr)) {
                docsEnum = termsEnum.docs(aReader.reader().getLiveDocs(),
                        null, DocsEnum.FLAG_FREQS);
                if (docsEnum != null) {
                  int docId = docsEnum.nextDoc();
                  while (docId != DocsEnum.NO_MORE_DOCS) {
                    matchedDocs.add(docId);
                    docId = docsEnum.nextDoc();
                  }
                }
              }
            }
          }
        } catch (IOException ex) {
          LOG.error("Error retrieving term frequency value.", ex);
        }
      }

      Integer oldValue = this.idxDfMap.putIfAbsent(term, matchedDocs.size());
      if (oldValue == null) {
        return matchedDocs.size();
      } else {
        final int addValue = matchedDocs.size();
        for (;;) {
          if (this.idxDfMap.replace(term, oldValue, oldValue + addValue)) {
            return oldValue + addValue;
          }
          oldValue = this.idxDfMap.get(term);
        }
      }
    }

    final Integer freq = this.idxDfMap.get(term);
    if (freq == null) {
      return 0;
    }
    return freq;
  }

  /**
   * Update cached list of stopwords from the {@link Environment}.
   */
  private void setStopwordsFromEnvironment() {
    final Collection<String> newStopWords = Environment.getStopwords();
    DirectIndexDataProvider.stopWords = new HashSet<>(newStopWords.size());
    for (String stopWord : newStopWords) {
      try {
        DirectIndexDataProvider.stopWords.add(new ByteArray(stopWord.
                getBytes("UTF-8")));
      } catch (UnsupportedEncodingException ex) {
        LOG.error("Error adding stopword '" + stopWord + "'.", ex);

      }
    }
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
            ConcurrentNavigableMap<
              Fun.Tuple2<SerializableByte, ByteArray>, Long> map) {
      for (Entry<Fun.Tuple2<SerializableByte, ByteArray>, Long> entry
              : map.entrySet()) {
        final Fun.Tuple2<SerializableByte, ByteArray> t2 = Fun.t2(
                entry.getKey().a, entry.getKey().b);
        Long oldValue = null;
        try {
          oldValue = this.map.putIfAbsent(t2, entry.getValue());

          if (oldValue != null) {
            for (;;) {
              if (this.map.replace(
                      t2, oldValue, oldValue + entry.getValue())) {
                break;
              }
              oldValue = this.map.get(t2);
            }
          }
        } catch (Exception ex) {
          LOG.error("EXCEPTION CAUGHT: t2.a={} t2.b={} old={} v={}",
                  t2.a, ByteArrayUtil.utf8ToString(t2.b), oldValue, entry.
                  getValue());
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
              .keySerializer(
                      DirectIndexDataProvider.this.idxTermsMapKeySerializer)
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
              LOG.warn("({}) No terms in field '{}'.", getName(), field);
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
            LOG.error("({}) Failed to parse terms in field {}.", getName(),
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

  @Override
  public Collection<String> testGetStopwords() {
    final Collection<String> sWords = new ArrayList<>(stopWords.size());
    for (ByteArray sw : stopWords) {
      sWords.add(ByteArrayUtil.utf8ToString(sw));
    }
    return sWords;
  }

  @Override
  public Collection<String> testGetFieldNames() {
    return Arrays.asList(this.currentFields);

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

    private DocFreqCalcTarget(
            final ConcurrentNavigableMap<ByteArray, Integer> targetMap,
            final String[] fields) {
      this.currentFields = fields;
      this.map = targetMap;
    }

    @Override
    public void call(final AtomicReaderContext rContext) {
      if (rContext == null) {
        return;
      }
      AtomicReader reader = rContext.reader();
      Terms terms;
      TermsEnum termsEnum = TermsEnum.EMPTY;
      DocsEnum docsEnum;

      for (String field : this.currentFields) {
        try {
          terms = reader.fields().terms(field);
        } catch (IOException ex) {
          LOG.error("Error retrieving terms. field={}.", field, ex);
          continue;
        }

        if (terms != null) {
          BytesRef br;
          try {
            termsEnum = terms.iterator(termsEnum);
            br = termsEnum.next();
          } catch (IOException ex) {
            LOG.error("Error getting terms enumerator. field={}.", field, ex);
            continue;
          }

          while (br != null) {
            try {
              docsEnum = termsEnum.docs(reader.getLiveDocs(), null,
                      DocsEnum.FLAG_FREQS);

              if (docsEnum != null) {
                @SuppressWarnings("CollectionWithoutInitialCapacity")
                final Collection<Integer> matchedDocs = new HashSet<>();

                int docId = docsEnum.nextDoc();
                while (docId != DocsEnum.NO_MORE_DOCS) {
                  matchedDocs.add(docId);
                  docId = docsEnum.nextDoc();
                }

                final ByteArray term = BytesRefUtil.toByteArray(br);
                Integer oldValue = this.map.putIfAbsent(term, matchedDocs.
                        size());
                if (oldValue != null) {
                  final int addValue = matchedDocs.size();
                  for (;;) {
                    if (this.map.replace(term, oldValue,
                            oldValue + addValue)) {
                      break;
                    }
                    oldValue = this.map.get(term);
                  }
                }
              }
            } catch (IOException ex) {
              LOG.error("Error enumerating documents. field={} term={}",
                      field, br.utf8ToString(), ex);
            }
            try {
              br = termsEnum.next();
            } catch (IOException ex) {
              LOG.error("Error retrieving next term. field={}", field, ex);
              break;
            }
          }
        }
      }
    }
  }
}
