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

import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.Tuple;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.ProcessingException;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DB.BTreeMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link IndexDataProvider} implementation directly accessing the Lucene
 * index.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DirectIndexDataProvider
        implements IndexDataProvider, Environment.FieldsChangedListener,
        Environment.StopwordsChangedListener {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          DirectIndexDataProvider.class);

  /**
   * Cached collection of all index terms mapped by document field. Mapping is
   * <tt>(Field, Term)</tt> to <tt>Frequency</tt>.
   */
  private BTreeMap<Fun.Tuple2<String, BytesWrap>, Long> idxTermsMap = null;

  private final BTreeKeySerializer idxTermsMapKeySerializer;

  /**
   * Cached collection of all index terms.
   */
  private Set<BytesWrap> idxTerms = null;
  /**
   * Cached collection of all (non deleted) document-ids.
   */
  private Collection<Integer> idxDocumentIds = null;
  /**
   * Cached document-frequency map for all terms in index.
   */
  private Map<BytesWrap, Integer> idxDfMap = null;
  /**
   * Cached overall term frequency of the index.
   */
  private Long idxTf = null;
  /**
   * Persistent disk backed storage backend.
   */
  private DB db;
  /**
   * Flag indicating, if this instance is temporary (no data is hold
   * persistent).
   */
  private boolean isTemporary = false;

  /**
   * Prefix used to store configuration.
   */
  private static final String IDENTIFIER = "DirectIDP";
  /**
   * Manager for external added document term-data values.
   */
  private ExternalDocTermDataManager externalTermData;

  /**
   * List of stop-words to exclude from term frequency calculations.
   */
  private static Collection<BytesWrap> stopWords = Collections.emptySet();

  /**
   * Properties keys to store cache information. a = cache directory key, b =
   * cache generation key.
   */
  private Tuple.Tuple2<String, String> cacheProperties = null;

  /**
   * List of fields cached by this instance.
   */
  private final Set<String> cachedFields;

  /**
   * Local copy of fields currently set by {@link Environment}.
   */
  private String[] currentFields;

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
    this.isTemporary = temporaray;

    this.idxTermsMapKeySerializer
            = new BTreeKeySerializer.Tuple2KeySerializer<String, BytesWrap>(
                    null, Serializer.STRING, new BytesWrap.Serializer());

    if (this.isTemporary) {
      LOG.warn("Caches are temporary!");
      this.cachedFields = Collections.emptySet();
      initDb(DBMaker.newTempFileDB());
    } else {
      final Tuple.Tuple4<Boolean, Integer, String, String> props
              = getCacheProperties();
      // store properties keys
      this.cacheProperties = Tuple.tuple2(props.c, props.d);

      // create permanent database
      DBMaker dbMkr = DBMaker.newFileDB(
              new File(Environment.getDataPath(), IDENTIFIER + "_" + props.b));
      initDb(dbMkr);
      this.cachedFields = this.db.getHashSet("cachedFields");

      LOG.debug("Cached fields: {}", this.cachedFields);
      LOG.debug("Active fields: {}", Environment.getFields());

      this.currentFields = Environment.getFields().clone();
      final List<String> eFields = Arrays.asList(this.currentFields);
      boolean missingFields = !this.cachedFields.containsAll(eFields);

      if (props.a) {
        // needs complete rebuild
        LOG.info("Building cache.");
        buildCache(props.c, props.d, eFields);
      } else if (missingFields) {
        // needs caching of some fields
        eFields.removeAll(this.cachedFields);
        LOG.info("Adding missing fields to cache ({}).", eFields);
        buildCache(props.c, props.d, eFields);
      }
    }

    this.externalTermData = new ExternalDocTermDataManager(this.db,
            IDENTIFIER);

    Environment.addFieldsChangedListener(this);
    Environment.addStopwordsChangedListener(this);
  }

  /**
   * Initialize the database with a default configuration.
   *
   * @param dbMkr Database maker
   */
  private void initDb(final DBMaker dbMkr) {
//    dbMkr.cacheLRUEnable(); // enable last-recent-used cache
//    dbMkr.cacheSoftRefEnable(); // enable soft reference cache
    dbMkr.transactionDisable(); // no transaction log
//    dbMkr.cacheDisable();
//    dbMkr.asyncWriteEnable();
    dbMkr.mmapFileEnableIfSupported(); // use memory mapping
    dbMkr.closeOnJvmShutdown(); // close db on JVM termination
    this.db = dbMkr.make();
  }

  /**
   * Check current list of fields against currently indexed ones. Starts
   * indexing of probably missing fields.
   */
  private void indexMissingFields() {
    final List<String> eFields = Arrays.asList(this.currentFields);
    boolean hasMissingFields = !this.cachedFields.containsAll(eFields);

    if (hasMissingFields) {
      // needs caching of some fields
      eFields.removeAll(this.cachedFields);
      LOG.info("Adding missing fields to cache ({}).", eFields);
      buildCache(this.cacheProperties.a, this.cacheProperties.b, eFields);
    } else {
      LOG.debug("All cached fields are up to date. (c:{}) e:{}",
              this.cachedFields, eFields);
    }
  }

  @Override
  public void warmUp() {
    final TimeMeasure tOverAll = new TimeMeasure().start();
    final TimeMeasure tStep = new TimeMeasure().start();
    // cache all index terms
    LOG.info("Cache warming: terms.");
    getTerms(); // caches this.idxTerms
    LOG.info("Cache warming: terms took {}.", tStep.stop().getTimeString());
    tStep.start();
    // collect index term frequency
    LOG.info("Cache warming: index term frequency.");
    getTermFrequency(); // caches this.idxTf
    LOG.info("Cache warming: index term frequency took {}.", tStep.stop().
            getTimeString());
    tStep.start();
    // cache document ids
    LOG.info("Cache warming: documents.");
    getDocumentIds(); // caches this.idxDocumentIds
    LOG.info("Cache warming: documents ({}) took {}.", this.idxDocumentIds.
            size(), tStep.stop().getTimeString());
    LOG.info("Cache warmed. Took {}.", tOverAll.stop().getTimeString());
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
   */
  @Override
  public long getTermFrequency() {
    if (this.idxTf == null) {
      LOG.info("Building term frequency index.");
      this.idxTf = 0L;

      for (String field : this.currentFields) {
        final Iterator<BytesWrap> bwIt = Fun.
                filter(this.idxTermsMap.keySet(), field).iterator();
        while (bwIt.hasNext()) {
          this.idxTf += this.idxTermsMap.get(Fun.t2(field, bwIt.next()));
        }
      }

      // remove term frequencies of stop-words
      if (!DirectIndexDataProvider.stopWords.isEmpty()) {
        Long tf;
        for (BytesWrap stopWord : DirectIndexDataProvider.stopWords) {
          tf = _getTermFrequency(stopWord);
          if (tf != null) {
            this.idxTf -= tf;
          }
        }
      }
    }
    return this.idxTf;
  }

  /**
   * Get the term frequency including stop-words.
   *
   * @param term Term to lookup
   * @return Term frequency
   */
  private Long _getTermFrequency(final BytesWrap term) {
    Long tf = 0L;
    for (String field : this.currentFields) {
      tf += this.idxTermsMap.get(Fun.t2(field, term));
    }
    return tf;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>null</tt>).
   */
  @Override
  public Long getTermFrequency(final BytesWrap term) {
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
  public double getRelativeTermFrequency(final BytesWrap term) {
    if (DirectIndexDataProvider.stopWords.contains(term)) {
      // skip stop-words
      return 0d;
    }

    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    final double tf = getTermFrequency(term).doubleValue();
    if (tf == 0) {
      return 0d;
    }
    return tf / Long.valueOf(getTermFrequency());
  }

  @Override
  public void dispose() {
    if (Environment.isInitialized()) {
      Environment.removeFieldsChangedListener(this);
    }
  }

  /**
   * Get the number of caches created by this instance.
   *
   * @return Number of caches created by this instance
   */
  private int getNumberOfCaches() {
    int count = 0;
    final String value = Environment.getProperty(IDENTIFIER,
            Properties.numberOfCaches.toString());
    if (value == null) {
      return count;
    }
    try {
      count = Integer.parseInt(value);
    } catch (NumberFormatException ex) {
      throw new IllegalStateException(
              "Failed to get the number of available caches.", ex);
    }
    return count;
  }

  /**
   * Initializes persistent data storage.
   *
   * @param rebuild If true any existing data will be purged
   */
  private void initDb(final boolean rebuild) {
    DB.BTreeMapMaker idxTermsMapMkr = this.db.createTreeMap("idxTermsMap");
    idxTermsMapMkr.keySerializer(this.idxTermsMapKeySerializer);
//    idxTermsMapMkr.keySerializer(BTreeKeySerializer.TUPLE2);
    idxTermsMapMkr.valueSerializer(Serializer.LONG);
    idxTermsMapMkr.nodeSize(100);

    if (rebuild) {
      this.db.delete("idxTermsMap");
      this.db.delete("idxDocFreqMap");
    } else {
      if (!this.db.exists("idxTermsMap")) {
        throw new IllegalStateException("Invalid database state. "
                + "'idxTermsMap' does not exist.");
      }
      if (!this.db.exists("idxDocFreqMap")) {
        throw new IllegalStateException("Invalid database state. "
                + "'idxDocFreqMap' does not exist.");
      }
    }
    this.idxTermsMap = idxTermsMapMkr.makeOrGet();
  }

  /**
   * Tries to get an already generated cache for the current index, identified
   * by the index path. This will also check if there were changes to the
   * index after cache creation. If this is the case, the cache will be
   * invalidated an re-created.
   *
   * @return 4-value tuple containing a boolean indicating, if the index
   * should be rebuild; integer value as id of the cache; a string containing
   * the properties index path key-name and a string containing the properties
   * index generation key-name.
   */
  private Tuple.Tuple4<Boolean, Integer, String, String> getCacheProperties() {
    final int cachesCount = getNumberOfCaches();
    // string pattern for storing cached index directory
    final String cachePathKey = Properties.cachePrefix.toString()
            + "%s" + Properties.cachePathSuffix.toString();
    // string pattern for storing cached index generation
    final String cacheGenKey = Properties.cachePrefix.toString()
            + "%s" + Properties.cacheGenSuffix.toString();
    if (cachesCount == 0) {
      return Tuple.tuple4(Boolean.TRUE, 0, String.format(cachePathKey, "0"),
              String.format(cacheGenKey, "0"));
    }

    int newCacheId = -1;

    // current index path
    final String iPath = Environment.getIndexPath();
    String cPath = ""; // properties path value
    String cPathKey = ""; // properties path key
    int cId = -1; // cache id
    for (int i = 0; i < cachesCount; i++) {
      cPathKey = String.format(cachePathKey, Integer.toString(i));
      cPath = Environment.getProperty(IDENTIFIER, cPathKey);

      // no value set for this id - save it, if we need to create a new cache
      if (cPath == null && newCacheId == -1) {
        newCacheId = i;
      }

      // check if a cache to the current index exists
      if (iPath.equals(cPath)) {
        cId = i;
        break;
      }
    }
    // no cache found for the current index path, return new cache properties
    if (cId < 0) {
      if (newCacheId == -1) {
        newCacheId = cachesCount + 1;
      }
      return Tuple.tuple4(Boolean.TRUE, newCacheId, String.
              format(cachePathKey, Integer.toString(newCacheId)), String.
              format(cacheGenKey, Integer.toString(newCacheId)));
    }

    // cache found - check for changes
    final String cGenKey = String.format(cacheGenKey, Integer.toString(cId));

    // check index generation
    final String iGen = Environment.getProperty(IDENTIFIER, cGenKey);

    boolean rebuild = false; // track if we need to rebuild current cache
    if (iGen == null) {
      // no generation information stored
      LOG.error("Missing generation information for cache.");
      rebuild = true;
    } else if (!iGen.equals(Long.toString(Environment.getIndexGeneration()))) {
      // compare generations
      LOG.info("Index changed. Rebuilding of cache needed.");
      rebuild = true;
    }

    // return current cache parameters
    if (rebuild) {
      return Tuple.tuple4(Boolean.TRUE, cId, cPathKey, cGenKey);
    } else {
      return Tuple.tuple4(Boolean.FALSE, cId, cPathKey, cGenKey);
    }
  }

  /**
   * Build a cache of the current index.
   *
   * @param cPathKey Properties cache path key
   * @param cGenKey Properties cache generation key
   * @param fields List of fields to cache
   */
  private void buildCache(final String cPathKey, final String cGenKey,
          final Collection<String> fields) {
    LOG.info("Building index term cache.");

    // clear all caches
    initDb(true);

    List<AtomicReaderContext> leaves = Environment.getIndexReader().
            getContext().leaves();

    Processing p = new Processing();
    p.setSourceAndTarget(new IndexTermsCollectorTarget(fields,
            new CollectionSource(leaves)));
    p.process(Processing.THREADS);

    // update properties
    Environment.setProperty(IDENTIFIER, cGenKey, Environment.
            getIndexGeneration().toString());
    Environment.setProperty(IDENTIFIER, cPathKey, Environment.getIndexPath());

    // update list of cached fields
    this.cachedFields.addAll(fields);
  }

  /**
   * Collect and cache all index terms. Stop-words will be removed from the
   * list.
   *
   * @return Unique collection of all terms in index
   */
  private Collection<BytesWrap> getTerms() {
    if (this.idxTerms == null) {
      LOG.info("Building index term cache.");
      this.idxTerms = DBMaker.newTempTreeSet();
      for (String field : this.currentFields) {
        Iterator<BytesWrap> bwIt = Fun.
                filter(this.idxTermsMap.keySet(), field).iterator();
        while (bwIt.hasNext()) {
          this.idxTerms.add(bwIt.next());
        }
      }
      // remove stop-words from index terms
      this.idxTerms.removeAll(DirectIndexDataProvider.stopWords);
    }
    return Collections.unmodifiableCollection(this.idxTerms);
  }

  @Override
  public Iterator<BytesWrap> getTermsIterator() {
    return getTerms().iterator();
  }

  @Override
  public Source<BytesWrap> getTermsSource() {
    return new CollectionSource<>(getTerms());
  }

  /**
   * Collect and cache all document-ids from the index.
   *
   * @return Unique collection of all (non-deleted) document ids
   */
  private Collection<Integer> getDocumentIds() {
    if (this.idxDocumentIds == null) {
      this.idxDocumentIds = DBMaker.newTempTreeSet();
      final int maxDoc = Environment.getIndexReader().maxDoc();

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

  @Override
  public Object setTermData(final String prefix, final int documentId,
          final BytesWrap term, final String key, final Object value) {
    return this.externalTermData.setData(prefix, documentId, term, key,
            value);
  }

  @Override
  public Object getTermData(final String prefix, final int documentId,
          final BytesWrap term, final String key) {
    return this.externalTermData.getData(prefix, documentId, term, key);
  }

  @Override
  public Map<BytesWrap, Object> getTermData(final String prefix,
          final int documentId, final String key) {
    return this.externalTermData.getData(prefix, documentId, key);
  }

  @Override
  public void clearTermData() {
    this.externalTermData.clear();
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
      BytesRef br = dftEnum.next();
      @SuppressWarnings("CollectionWithoutInitialCapacity")
      final Map<BytesWrap, AtomicLong> tfMap = new HashMap<>();
      while (br != null) {
        final BytesWrap bw = new BytesWrap(br);
        // skip stop-words
        if (!DirectIndexDataProvider.stopWords.contains(bw)) {
          if (tfMap.containsKey(bw)) {
            tfMap.get(bw).getAndAdd(dftEnum.getTotalTermFreq());
          } else {
            tfMap.put(bw.clone(), new AtomicLong(dftEnum.
                    getTotalTermFreq()));
          }
        }
        br = dftEnum.next();
      }
      for (Entry<BytesWrap, AtomicLong> tfEntry : tfMap.entrySet()) {
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
  public Collection<BytesWrap> getDocumentsTermSet(
          final Collection<Integer> docIds) {
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<BytesWrap> terms = new HashSet<>();
    for (Integer docId : new HashSet<>(docIds)) {
      checkDocId(docId);
      try {
        final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(docId);
        BytesRef br = dftEnum.next();
        while (br != null) {
          terms.add(new BytesWrap(br));
          br = dftEnum.next();
        }
      } catch (IOException ex) {
        LOG.error("Caught exception while iterating document terms. "
                + "docId=" + docId + ".", ex);
      }
    }
    // remove stop-words
    terms.removeAll(DirectIndexDataProvider.stopWords);
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
  public boolean documentContains(final int documentId, final BytesWrap term) {
    if (DirectIndexDataProvider.stopWords.contains(term)) {
      // skip stop-words
      return false;
    }

    checkDocId(documentId);
    try {
      final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum(documentId);
      BytesRef br = dftEnum.next();
      while (br != null) {
        if (new BytesWrap(br).equals(term)) {
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

  @Override
  public void registerPrefix(final String prefix) {
    this.externalTermData.loadPrefix(prefix);
  }

  @Override
  public void fieldsChanged(final String[] oldFields) {
    LOG.debug("Fields changed, clearing cached data.");
    this.currentFields = Environment.getFields().clone();
    indexMissingFields();
    clearCache();
    warmUp();
  }

  /**
   * Clear all dynamic caches. This must be called, if the fields or
   * stop-words have changed.
   */
  private void clearCache() {
    this.idxTerms = null;
    this.idxDfMap = null;
    this.idxTf = null;
  }

  /**
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>0</tt>).
   */
  @Override
  public int getDocumentFrequency(final BytesWrap term) {
    if (DirectIndexDataProvider.stopWords.contains(term)) {
      // skip stop-words
      return 0;
    }

    if (this.idxDfMap == null || this.idxDfMap.get(term) == null) {
      if (this.idxDfMap == null) {
        this.idxDfMap = DBMaker.newTempHashMap();
      }

      @SuppressWarnings("CollectionWithoutInitialCapacity")
      final Collection<Integer> matchedDocs = new HashSet<>();
      for (String field : this.currentFields) {
        try {
          DocsEnum de = MultiFields.
                  getTermDocsEnum(Environment.getIndexReader(), MultiFields.
                          getLiveDocs(Environment.getIndexReader()), field,
                          new BytesRef(term.getBytes()));
          if (de == null) {
            // field or term not found
            continue;
          }

          int docId = de.nextDoc();
          while (docId != DocsEnum.NO_MORE_DOCS) {
            matchedDocs.add(docId);
            docId = de.nextDoc();
          }
        } catch (IOException ex) {
          LOG.error("Error retrieving term frequency value.", ex);
        }
      }
      this.idxDfMap.put(term.clone(), matchedDocs.size());
    }

    final Integer freq = this.idxDfMap.get(term);
    if (freq == null) {
      return 0;
    }
    return freq;
  }

  @Override
  public void wordsChanged(final Collection<String> oldWords) {
    LOG.debug("Stopwords changed, clearing cached data.");
    clearCache();
    final Collection<String> newStopWords = Environment.getStopwords();
    DirectIndexDataProvider.stopWords = DBMaker.newTempHashSet();
    for (String stopWord : newStopWords) {
      try {
        DirectIndexDataProvider.stopWords.add(new BytesWrap(stopWord.getBytes(
                "UTF-8")));
      } catch (UnsupportedEncodingException ex) {
        LOG.error("Error adding stopword '" + stopWord + "'.", ex);
      }
    }
    warmUp();
  }

  private class IndexTermsCollectorTarget extends Target<AtomicReaderContext> {

    /**
     * List of fields to collect terms from.
     */
    private final Collection<String> fields;
    /**
     * Reusable terms enumerator instance.
     */
    private TermsEnum termsEnum = TermsEnum.EMPTY;
    /**
     * Local portion of the global {@link #idxTermsMap}. Written to the global
     * index after reading all terms from the current reader.
     */
    private final BTreeMap<Fun.Tuple2<String, BytesWrap>, Long> localIdxTermsMap;
    /**
     * Local memory database instance.
     */
    private final DB db;

    /**
     * Initial constructor.
     *
     * @param newFields List of fields to collect terms from.
     * @param newSource Source providing reader contexts
     */
    public IndexTermsCollectorTarget(final Collection<String> newFields,
            final Source newSource) {
      super(newSource);
      this.fields = newFields;
      this.db = DBMaker.newMemoryDirectDB().asyncWriteFlushDelay(100).
              transactionDisable().compressionEnable().make();
      DB.BTreeMapMaker mapMkr = this.db.createTreeMap("localIdxTermsMap");
      mapMkr.keySerializer(
              DirectIndexDataProvider.this.idxTermsMapKeySerializer);
      mapMkr.valueSerializer(Serializer.LONG);
      mapMkr.nodeSize(100);
      this.localIdxTermsMap = mapMkr.makeOrGet();
    }

    @Override
    public Target<AtomicReaderContext> newInstance() {
      return new IndexTermsCollectorTarget(this.fields, getSource());
    }

    @Override
    public void runProcess() throws Exception {
      try {
        while (!isTerminating()) {
          AtomicReaderContext rContext;
          try {
            rContext = getSource().next();
          } catch (ProcessingException.SourceHasFinishedException ex) {
            break;
          }

          if (rContext == null) {
            continue;
          }

          Terms terms;
          BytesRef br;
          // use the plain reader
          for (String field : this.fields) {
            terms = rContext.reader().terms(field);
            try {
              if (terms == null) {
                LOG.warn("No terms in field '{}'.", field);
              } else {
                // termsEnum is bound to the current field
                termsEnum = terms.iterator(termsEnum);
                br = termsEnum.next();
                while (br != null) {
                  if (termsEnum.seekExact(br)) {
                    // get the current term in field
                    final BytesWrap bw = new BytesWrap(br);

                    // get the total term frequency of the current term
                    // across all documents and the current field
                    final long ttf = termsEnum.totalTermFreq();
                    // add value up for all fields
                    final Fun.Tuple2<String, BytesWrap> fieldTerm = Fun.
                            t2(field, bw);

                    try {
                      Long oldValue = this.localIdxTermsMap.putIfAbsent(
                              fieldTerm, ttf);
                      if (oldValue != null) {
                        for (;;) {
                          oldValue = this.localIdxTermsMap.get(fieldTerm);
                          if (this.localIdxTermsMap.replace(fieldTerm,
                                  oldValue, oldValue + ttf)) {
                            break;
                          }
                        }
                      }
                    } catch (Exception ex) {
                      LOG.error("Error: f={} t={} v={}", field, bw.toString(),
                              ttf, ex);
                      throw ex;
                    }
                  }
                  br = termsEnum.next();
                }
              }
            } catch (IOException ex) {
              LOG.error("Failed to parse terms in field {}.", field, ex);
            }
          }

          // write local cached data to global index
          LOG.debug("Commiting cached data.");
//          for (Entry<Fun.Tuple2<String, BytesWrap>, Long> entry
//                  : this.localIdxTermsMap.entrySet()) {
//            Long oldValue = DirectIndexDataProvider.this.idxTermsMap.
//                    putIfAbsent(entry.getKey(), entry.getValue());
//            if (oldValue != null) {
//              for (;;) {
//                oldValue = DirectIndexDataProvider.this.idxTermsMap.
//                        get(entry.getKey());
//                if (DirectIndexDataProvider.this.idxTermsMap.
//                        replace(entry.getKey(), oldValue, oldValue + entry.
//                                getValue())) {
//                  break;
//                }
//              }
//            }
//          }
          this.localIdxTermsMap.clear();
        }
      } finally {
        this.db.close();
      }
    }
  }
}
