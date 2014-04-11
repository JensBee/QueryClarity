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
import de.unihildesheim.SerializableByte;
import de.unihildesheim.Tuple;
import de.unihildesheim.lucene.Environment;
import de.unihildesheim.lucene.document.DocFieldsTermsEnum;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.util.BytesRefUtil;
import de.unihildesheim.util.ByteArrayUtil;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicLong;
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
public final class DirectIndexDataProvider
        implements IndexDataProvider, Environment.EnvironmentEventListener {

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
  ConcurrentNavigableMap<Fun.Tuple2<
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
  private static Collection<ByteArray> stopWords = Collections.
          <ByteArray>emptySet();

  /**
   * Properties keys to store cache information. a = cache directory key, b =
   * cache generation key.
   */
  private Tuple.Tuple2<String, String> cacheProperties = null;

  /**
   * List of fields cached by this instance. Mapping of field name to id
   * value.
   */
  final Map<String, SerializableByte> cachedFieldsMap;

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
    IDX_DFMAP
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
  @SuppressWarnings({"LeakingThisInConstructor",
    "CollectionWithoutInitialCapacity"})
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
    this.currentFields = Environment.getFields().clone();

    final Tuple.Tuple4<Boolean, Integer, String, String> props
            = getCacheProperties();
    // store properties keys
    this.cacheProperties = Tuple.tuple2(props.c, props.d);

    if (this.isTemporary) {
      LOG.warn("Caches are temporary!");
      setupDb(DBMaker.newTempFileDB());
      this.cachedFieldsMap = new HashMap<>();
    } else {
      // create permanent database
      DBMaker dbMkr = DBMaker.newFileDB(
              new File(Environment.getDataPath(), IDENTIFIER + "_" + props.b));
      setupDb(dbMkr);
      this.cachedFieldsMap = this.db.getHashMap("cachedFields");
    }

    LOG.debug("Cached fields: {}", this.cachedFieldsMap.keySet());
    LOG.debug("Active fields: {}", Arrays.toString(this.currentFields));

    final List<String> eFields = Arrays.asList(this.currentFields);
    boolean missingFields = !this.cachedFieldsMap.keySet().containsAll(
            eFields);

    if (props.a) {
      // needs complete rebuild
      LOG.info("Building cache.");
      buildCache(props.c, props.d, eFields);
    } else if (missingFields) {
      // needs caching of some fields
      eFields.removeAll(this.cachedFieldsMap.keySet());
      LOG.info("Adding missing fields to cache ({}).", eFields);
      buildCache(props.c, props.d, eFields);
    } else {
      // load persistent data
      initDb(false);
    }

    this.externalTermData = new ExternalDocTermDataManager(this.db,
            IDENTIFIER);

    setStopwordsFromEnvironment();
    // pre fill caches - ensures caches are pre-filled,
    // otherwise may cause inconsistency, if concurrently accessed
    if (!Environment.isTestRun()) {
      this.warmUp();
    }

    Environment.addEventListener(this);
  }

  /**
   * Initialize the database with a default configuration.
   *
   * @param dbMkr Database maker
   */
  private void setupDb(final DBMaker dbMkr) {
    this.db = dbMkr
            .transactionDisable()
            .asyncWriteEnable()
            .asyncWriteFlushDelay(100)
            // bug #313
            //            .mmapFileEnableIfSupported() // needs 64bit machine
            .mmapFileEnablePartial()
            .closeOnJvmShutdown()
            //            .freeSpaceReclaimQ(0)
            .make();
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
  }

  /**
   * Initializes persistent data storage.
   *
   * @param rebuild If true any existing data will be purged
   */
  private void initDb(final boolean rebuild) {
    final DB.BTreeMapMaker idxTermsMapMkr = this.db
            .createTreeMap(Stores.IDX_TERMS_MAP.name())
            .keySerializer(this.idxTermsMapKeySerializer)
            .valueSerializer(Serializer.LONG)
            .nodeSize(100);
    boolean clearCache = false;

    if (rebuild) {
      this.db.delete(Stores.IDX_TERMS_MAP.name());
      clearCache = true;
    } else {
      if (!this.db.exists(Stores.IDX_TERMS_MAP.name())) {
        throw new IllegalStateException("Invalid database state. "
                + "'idxTermsMap' does not exist.");
      }

      final Collection<String> aFields = Arrays.asList(this.currentFields);
      final Collection<String> cFields = this.db.getHashSet(Stores.FIELDS.
              name());
      if (cFields.size() == aFields.size() && cFields.containsAll(aFields)) {
        final Set<ByteArray> cStopwords = this.db.getHashSet(Stores.STOPWORDS.
                name());
        if (cStopwords.size() != DirectIndexDataProvider.stopWords.size()
                && !cStopwords.containsAll(DirectIndexDataProvider.stopWords)) {
          LOG.info("Caches will be cleared. Stopwords changed since caching.");
          clearCache = true;
        }
      } else {
        LOG.info("Caches will be cleared. Fields changed since caching.");
        clearCache = true;
      }
    }

    if (clearCache) {
      clearCache(); // clear any possible leftover caches
    } else {
      this.idxTerms = this.idxTermsMaker.makeOrGet();
      LOG.info("Loaded index terms cache with {} entries.", this.idxTerms.
              size());
      this.idxDfMap = this.idxDfMapMaker.makeOrGet();
      LOG.info("Loaded document frequency cache with {} entries.",
              this.idxDfMap.size());
      this.idxTf = null;
    }

    this.idxTermsMap = idxTermsMapMkr.makeOrGet();
  }

  /**
   * Check current list of fields against currently indexed ones. Starts
   * indexing of probably missing fields.
   */
  private void indexMissingFields() {
    final List<String> eFields = Arrays.asList(this.currentFields);
    boolean hasMissingFields = !this.cachedFieldsMap.keySet().containsAll(
            eFields);

    if (hasMissingFields) {
      // needs caching of some fields
      eFields.removeAll(this.cachedFieldsMap.keySet());
      LOG.info("Adding missing fields to cache ({}).", eFields);
      buildCache(this.cacheProperties.a, this.cacheProperties.b, eFields);
    } else {
      LOG.debug("All cached fields are up to date. (c:{}) e:{}",
              this.cachedFieldsMap.keySet(), eFields);
    }
  }

  @Override
  public void warmUp() {
    if (this.warmed) {
      LOG.info("Caches are up to date.");
      return;
    }
    final TimeMeasure tOverAll = new TimeMeasure().start();
    final TimeMeasure tStep = new TimeMeasure();

    // cache all index terms
    tStep.start();
    LOG.info("Cache warming: terms..");
    getTerms(); // caches this.idxTerms
    LOG.info("Cache warming: collecting {} unique terms took {}.",
            this.idxTerms.size(), tStep.stop().getTimeString());

    // collect index term frequency
    tStep.start();
    LOG.info("Cache warming: index term frequencies..");
    getTermFrequency(); // caches this.idxTf
    LOG.info("Cache warming: index term frequency calculation "
            + "for {} terms took {}.", this.idxTerms.size(), tStep.stop().
            getTimeString());

    // cache document ids
    tStep.start();
    LOG.info("Cache warming: documents..");
    getDocumentIds(); // caches this.idxDocumentIds
    LOG.info("Cache warming: collecting {} documents took {}.",
            this.idxDocumentIds.size(), tStep.stop().getTimeString());

    // cache document frequency values for each term
    tStep.start();
    LOG.info("Cache warming: document frequencies..");
    new Processing(
            new Target.TargetFuncCall<>(
                    new CollectionSource<>(this.idxTerms),
                    new DocFreqCalcTarget()
            )).process();
    LOG.info("Cache warming: calculating document frequencies "
            + "for {} documents and {} terms took {}.", this.idxDocumentIds.
            size(), this.idxTerms.size(), tStep.stop().getTimeString());

    // store cache configuration
    LOG.info("Updating cache information.");
    // cached fields
    if (this.db.exists(Stores.FIELDS.name())) {
      this.db.delete(Stores.FIELDS.name());
    }
    final Set<String> dbCachedFields = this.db.createHashSet(Stores.FIELDS.
            name()).serializer(Serializer.STRING).make();
    dbCachedFields.addAll(Arrays.asList(this.currentFields));
    // stopwords list
    if (this.db.exists(Stores.STOPWORDS.name())) {
      this.db.delete(Stores.STOPWORDS.name());
    }
    final Set<ByteArray> dbCachedStopwords = this.db.createHashSet(
            Stores.STOPWORDS.name()).serializer(ByteArray.SERIALIZER).make();
    dbCachedStopwords.addAll(DirectIndexDataProvider.stopWords);

    LOG.info("Writing cachnges.");
    this.db.commit();

    LOG.info("Cache warmed. Took {}.", tOverAll.stop().getTimeString());
    this.warmed = true;
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
  private SerializableByte getFieldId(final String fieldName) {
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
   * {@inheritDoc} Stop-words will be skipped (their value is <tt>null</tt>).
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
    if (Environment.isInitialized()) {
      Environment.removeEventListener(this);
    }
    if (!this.db.isClosed()) {
      LOG.info("Closing database.");
      this.db.close();
      LOG.debug("Closing database - done.");
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
    LOG.debug("Cache loader - start.");
    final int cachesCount = getNumberOfCaches();
    // string pattern for storing cached index directory
    final String cachePathKey = Properties.cachePrefix.toString()
            + "%s" + Properties.cachePathSuffix.toString();
    // string pattern for storing cached index generation
    final String cacheGenKey = Properties.cachePrefix.toString()
            + "%s" + Properties.cacheGenSuffix.toString();
    if (cachesCount == 0) {
      LOG.debug("No cached data found.");
      LOG.debug("Cache loader - finish.");
      return Tuple.tuple4(Boolean.TRUE, 0, String.format(cachePathKey, "0"),
              String.format(cacheGenKey, "0"));
    }

    int newCacheId = -1;

    // current index path
    final String iPath = Environment.getIndexPath();
    String cPath; // properties path value
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
      LOG.debug("No cache found for current index path.");
      LOG.debug("Cache loader - finish.");
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

    LOG.debug("Cache loader - finish. rebuild: {}", rebuild);
    // return current cache parameters
    if (rebuild) {
      return Tuple.tuple4(Boolean.TRUE, cId, cPathKey, cGenKey);
    } else {
      return Tuple.tuple4(Boolean.FALSE, cId, cPathKey, cGenKey);
    }
  }

  /**
   * Add a field to the list of cached fields.
   *
   * @param fieldName Field name to add
   */
  private void addCachedField(final String fieldName) {
    Collection<SerializableByte> keys = this.cachedFieldsMap.values();
    for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
      final SerializableByte sByte = new SerializableByte(i);
      if (!keys.contains(sByte)) {
        this.cachedFieldsMap.put(fieldName, sByte);
        break;
      }
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
    LOG.info("Building persistent index term cache.");

    // clear all caches
    initDb(true);

    // update list of cached fields
    for (String field : fields) {
      addCachedField(field);
    }

    new Processing(
            new Target.TargetFuncCall<>(
                    new CollectionSource<>(
                            Environment.getIndexReader().
                            getContext().leaves()),
                    new IndexTermsCollectorTarget(fields)
            )).process();

    final Map<String, Object> props = Environment.getProperties(IDENTIFIER);
    final String cPrefix = Properties.cachePrefix.toString();
    int caches = 1; // we have just created a new one
    for (String pName : props.keySet()) {
      if (pName.startsWith(cPrefix)) {
        caches++;
      }
    }

    // update properties
    Environment.setProperty(IDENTIFIER, Properties.numberOfCaches.toString(),
            Integer.toString(caches));
    Environment.setProperty(IDENTIFIER, cGenKey, Environment.
            getIndexGeneration().toString());
    Environment.setProperty(IDENTIFIER, cPathKey, Environment.getIndexPath());
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

      for (String field : this.currentFields) {
        Iterator<ByteArray> bytesIt = Fun.filter(this.idxTermsMap.keySet(),
                getFieldId(field)).iterator();

        while (bytesIt.hasNext()) {
          final ByteArray bytes = bytesIt.next();
          if (!this.idxTerms.contains(bytes)
                  && !DirectIndexDataProvider.stopWords.contains(bytes)) {
            this.idxTerms.add(bytes.clone());
          }
        }
      }
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

  @Override
  public Object setTermData(final String prefix, final int documentId,
          final ByteArray term, final String key, final Object value) {
    return this.externalTermData.setData(prefix, documentId, term, key,
            value);
  }

  @Override
  public Object getTermData(final String prefix, final int documentId,
          final ByteArray term, final String key) {
    return this.externalTermData.getData(prefix, documentId, term, key);
  }

  @Override
  public Map<ByteArray, Object> getTermData(final String prefix,
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
      BytesRef bytesRef = dftEnum.next();
      @SuppressWarnings("CollectionWithoutInitialCapacity")
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
    @SuppressWarnings("CollectionWithoutInitialCapacity")
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

  @Override
  public void registerPrefix(final String prefix) {
    this.externalTermData.loadPrefix(prefix);
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
    // (content depends on current fields & stopwords)
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

    if (this.idxDfMap.isEmpty() || this.idxDfMap.get(term) == null) {
      @SuppressWarnings("CollectionWithoutInitialCapacity")
      final Collection<Integer> matchedDocs = new HashSet<>();
      for (String field : this.currentFields) {
        try {
          DocsEnum de = MultiFields.
                  getTermDocsEnum(Environment.getIndexReader(), MultiFields.
                          getLiveDocs(Environment.getIndexReader()), field,
                          new BytesRef(term.bytes));
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
      Integer oldValue = this.idxDfMap.putIfAbsent(term, matchedDocs.size());
      if (oldValue != null) {
        for (;;) {
          if (this.idxDfMap.replace(term, oldValue, oldValue + matchedDocs.
                  size())) {
            break;
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
   * {@link Processing} {@link Target} for collecting index terms.
   */
  private class IndexTermsCollectorTarget
          extends Target.TargetFunc<AtomicReaderContext> {

    /**
     * List of fields to collect terms from.
     */
    private final Collection<String> fields;

    /**
     * Id of the temporary cache map.
     */
    private static final String MAPID = "localIdxTermsMap";

    /**
     * Create a new collector for index terms.
     *
     * @param newFields Lucene index segment provider
     */
    IndexTermsCollectorTarget(final Collection<String> newFields) {
      super();
      this.fields = newFields;
    }

    @Override
    public void call(final AtomicReaderContext rContext) {
      if (rContext == null) {
        return;
      }

      TermsEnum termsEnum = TermsEnum.EMPTY;
      final DB db = DBMaker.newMemoryDirectDB().asyncWriteEnable().
              transactionDisable().make();
      final ConcurrentNavigableMap<Fun.Tuple2<
              SerializableByte, ByteArray>, Long> localIdxTermsMap
              = db
              .createTreeMap(MAPID + getName())
              .keySerializer(
                      DirectIndexDataProvider.this.idxTermsMapKeySerializer)
              .valueSerializer(Serializer.LONG)
              .nodeSize(100)
              .makeOrGet();

      try {
        Terms terms;
        BytesRef br;
        // use the plain reader
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
                          = Fun.t2(fieldId.clone(), BytesRefUtil.
                                  toByteArray(br));

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

              // write local cached data to global index
              LOG.trace("({}) Commiting cached data "
                      + "for field '{}' ({} entries).", getName(), field,
                      localIdxTermsMap.size());

              for (Entry<Fun.Tuple2<SerializableByte, ByteArray>, Long> entry
                      : localIdxTermsMap.entrySet()) {
                final Fun.Tuple2<SerializableByte, ByteArray> t2 = Fun.t2(
                        entry.getKey().a.clone(), entry.getKey().b.clone());
                final Long v = entry.getValue();
                try {
                  Long oldValue = DirectIndexDataProvider.this.idxTermsMap.
                          putIfAbsent(t2, v);

                  if (oldValue != null) {
                    for (;;) {
                      if (DirectIndexDataProvider.this.idxTermsMap.
                              replace(t2, oldValue, oldValue
                                      + entry.getValue())) {
                        break;
                      }
                      oldValue = DirectIndexDataProvider.this.idxTermsMap.
                              get(t2);
                    }
                  }
                } catch (Exception ex) {
                  LOG.error("EXCEPTION CAUGHT: eK={} eKb={} eV={}", entry.
                          getKey(), ByteArrayUtil.utf8ToString(entry.
                                  getKey().b), entry.getValue(), ex);
                }
              }
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
        db.close();
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
   * Handle index fields changes.
   */
  private void fieldsChanged() {
    LOG.debug("Fields changed, clearing cached data.");
    this.currentFields = Environment.getFields().clone();
    indexMissingFields();
  }

  /**
   * Handle stopword changes.
   */
  private void wordsChanged() {
    LOG.debug("Stopwords changed, clearing cached data.");
    setStopwordsFromEnvironment();
  }

  /**
   * Handler for events fired by the {@link Environment}.
   *
   * @param eType Event type
   */
  private void handleEnvironmentEvent(final Environment.EventType eType) {
    switch (eType) {
      case FIELDS_CHANGED:
        fieldsChanged();
        break;
      case STOPWORDS_CHANGED:
        wordsChanged();
        break;
      default:
        throw new IllegalArgumentException("Unhandeled event type: "
                + eType.name());
    }
  }

  @Override
  public void eventsFired(final List<Environment.EventType> events) {
    for (Environment.EventType eType : events) {
      handleEnvironmentEvent(eType);
    }
    clearCache();
    warmUp();
  }

  @Override
  public void eventFired(final Environment.EventType event) {
    handleEnvironmentEvent(event);
  }

  /**
   * {@link Processing} {@link Target} for document term-frequency
   * calculation.
   */
  private final class DocFreqCalcTarget extends
          Target.TargetFunc<ByteArray> {

    @Override
    public void call(final ByteArray term) {
      if (term != null) {
        getDocumentFrequency(term);
      }
    }
  }
}
