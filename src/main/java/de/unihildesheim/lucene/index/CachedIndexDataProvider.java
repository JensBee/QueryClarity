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
import de.unihildesheim.lucene.document.DocumentModel.DocumentModelBuilder;
import de.unihildesheim.lucene.document.DocumentModelException;
import de.unihildesheim.util.StringUtils;
import de.unihildesheim.util.Configuration;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Processing;
import de.unihildesheim.util.concurrent.processing.ProcessingException;
import de.unihildesheim.util.concurrent.processing.Source;
import de.unihildesheim.util.concurrent.processing.Target;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.Bind;
import org.mapdb.Bind.MapListener;
import org.mapdb.DB;
import org.mapdb.DB.BTreeSetMaker;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation of the {@link IndexDataProvider} stores it's data with
 * disk-backed {@link ConcurrentMap} implementations to cache calculated
 * values. This allows to store a huge amount of data exceeding memory limits,
 * with a bit of speed tradeoff to load cached values from disk.
 *
 * TODO: should implement Environment.FieldsChangedListener
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class CachedIndexDataProvider implements IndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          CachedIndexDataProvider.class);

  /**
   * Prefix used to store configuration.
   */
  private static final String PREFIX = "CachedIDP";

  /**
   * Separator to store field names.
   */
  private static final String FIELD_NAME_SEP
          = Configuration.get(PREFIX + "fieldNameSep", "\\|");

  /**
   * Unique identifier for this cache.
   */
  private final String storageId;

  /**
   * Disk storage backend.
   */
  private DB db;

  /**
   * Store mapping of <tt>term : frequency values</tt>.
   */
  private ConcurrentMap<BytesWrap, Fun.Tuple2<Long, Double>> termFreqMap;
  /**
   * R/w lock for {@link #termFreqMap}.
   */
  private final ReentrantReadWriteLock termFreqMapLock;

  /**
   * Base document model data storage. Stores <tt>document-id :
   * term-frequency</tt>.
   */
  private ConcurrentMap<Integer, Long> docModels;

  /**
   * Base document-term data. Store <tt>document-id, key, term -> value</tt>.
   */
  private BTreeMap<Fun.Tuple3<
          Integer, String, BytesWrap>, Object> docTermDataInternal;
  /**
   * R/w lock for {@link #docTermDataInternal}.
   */
  private final ReadWriteLock docTermDataInternalLock;

  /**
   * Manager for external added document term-data values.
   */
  private ExternalDocTermDataManager externalTermData;

  /**
   * List of known prefixes to stored data in {@link #docTermDataPrefixed}.
   */
  private Collection<String> knownPrefixes;

  /**
   * Summed frequency of all terms in the index.
   */
  private transient Long overallTermFreq = null;

  /**
   * Prefix to use for internal data stored to {@link #docTermDataInternal}.
   */
  private static final String INTERNAL_PREFIX = "_";

  /**
   * Flag indicating if a database rollback should be done. Used, if
   * calculation is in progress and JVM is shutting down.
   */
  private boolean rollback = false;

  /**
   * Thread handling the JVM shutdown signal.
   */
  private final Thread shutdowHandler;

  /**
   * Flag indicating, if the storage has bee initialized.
   */
  private boolean storageInitialized = false;

  /**
   * Flag indicating, if this instance stores data only temporary.
   */
  private boolean isTemporary = false;

  /**
   * Static keys used to store and retrieve persistent meta information.
   */
  private enum DataKeys {

    /**
     * Document-Data: External stored, from instances using this class.
     */
    _external,
    /**
     * Document-Data: External stored, from instances using this class. This
     * is a fast lookup-list based on the data stored in <tt>_external</tt>.
     */
    _external_ll,
    /**
     * Document-Data: Term frequency.
     */
    _docTermFreq;

    /**
     * Keys to identify objects in the database.
     */
    private enum DataBase {

      /**
       * Name of document model storage.
       */
      documentModel,
      /**
       * Name of document, term-data storage.
       */
      documentModelTermData,
      /**
       * Name of document model, term frequency storage.
       */
      documentModelTermFreq,
      /**
       * Name of index, term-frequency storage.
       */
      indexTermFreq,
      /**
       * List of known prefixes for external data store.
       */
      prefixes
    }

    /**
     * Keys for storing properties to disk.
     */
    private enum Properties {

      /**
       * Boolean flag, if document-model data is available.
       */
      documentModel,
      /**
       * Boolean flag, if document-model, term-data data is available.
       */
      documentModelTermData,
      /**
       * Boolean flag, if document-model, term-frequency data is available.
       */
      documentModelTermFreq,
      /**
       * Fields indexed by this instance.
       */
      fields,
      /**
       * Boolean flag, if index term-frequency data is available.
       */
      indexTermFreq,
      /**
       * Cache data last access date.
       */
      timestamp,
    }

    /**
     * Get a string representation of the given key.
     *
     * @param key Key to get as String
     * @return String representation of the given key
     */
    public static String get(final DataKeys key) {
      return key.name();
    }

    /**
     * Get a string representation of the given key.
     *
     * @param key Key to get as String
     * @return String representation of the given key
     */
    public static String get(final DataKeys.Properties key) {
      return key.name();
    }

    /**
     * Get a string representation of the given key.
     *
     * @param key Key to get as String
     * @return String representation of the given key
     */
    public static String get(final DataKeys.DataBase key) {
      return key.name();
    }
  }

  /**
   * Creates a new disk backed (cached) {@link IndexDataProvider} with the
   * given storage path.
   *
   * @param newStorageId Unique identifier for this cache
   * @throws IOException Thrown on low-level I/O errors
   */
  public CachedIndexDataProvider(final String newStorageId) throws IOException {
    this(newStorageId, false);
  }

  /**
   * Creates a new disk backed (cached) {@link IndexDataProvider} with the
   * given storage path. This instance may be temporary, meaning that all data
   * will be removed, after closing this instance.
   *
   * @param newStorageId Unique identifier for this cache. This value is
   * ignored, if temporary storage is used
   * @param temporary If true, all stored data will be removed on closing this
   * instance
   * @throws IOException Thrown on low-level I/O errors
   */
  CachedIndexDataProvider(final String newStorageId, final boolean temporary)
          throws IOException {
    this.docTermDataInternalLock = new ReentrantReadWriteLock();
    this.termFreqMapLock = new ReentrantReadWriteLock();
    this.isTemporary = temporary;
    if (!temporary && (newStorageId == null || newStorageId.isEmpty())) {
      throw new IllegalArgumentException("Missing storage information.");
    }

    LOG.info("Created IndexDataProvider::{} instance storage={}.", this.
            getClass().getCanonicalName(), Environment.getDataPath());
    this.storageId = newStorageId;

    // create the manager for disk storage
    createDb();

    // create a shutdown hooking thread to handle db closing and rollbacks
    this.shutdowHandler = new Thread(new Runnable() {
      @Override
      public void run() {
        if (!CachedIndexDataProvider.this.db.isClosed()) {
          if (CachedIndexDataProvider.this.rollback) {
            LOG.warn("Process interrupted. Rolling back changes.");
            CachedIndexDataProvider.this.db.rollback();
            CachedIndexDataProvider.this.db.close();
          } else {
            CachedIndexDataProvider.this.db.commit();
            CachedIndexDataProvider.this.db.close();
          }
        }
      }
    }, "CachedIndexDataProvider_shutdownHandler");
  }

  private void createDb() {
    // create the manager for disk storage
    try {
      DBMaker dbMkr;
      if (this.isTemporary) {
        dbMkr = DBMaker.newTempFileDB();
      } else {
        dbMkr = DBMaker.newFileDB(new File(Environment.getDataPath(),
                this.storageId));
      }
      dbMkr.cacheLRUEnable(); // enable last-recent-used cache
      dbMkr.cacheSoftRefEnable();
      this.db = dbMkr.make();
    } catch (RuntimeException ex) {
      LOG.error(
              "Caught runtime exception. Maybe your classes have changed."
              + "You may want to delete the cache, because it's invalid now.");
    }
  }

  protected Collection<String> getKnownPrefixes() {
    return knownPrefixes;
  }

  public boolean isStorageInitialized() {
    return storageInitialized;
  }

  /**
   * Checks, if document models are available.
   *
   * @return True, if data is available
   */
  private boolean hasDocModels() {
    final boolean state = Boolean.parseBoolean(Environment.getProperty(
            PREFIX, DataKeys.get(DataKeys.Properties.documentModel),
            "false"));
    return state && !this.docModels.isEmpty();
  }

  /**
   * Checks, if document -> term-data is available.
   *
   * @return True, if data is available
   */
  private boolean hasDocTermData() {
    final boolean state = Boolean.parseBoolean(Environment.getProperty(
            PREFIX, DataKeys.get(
                    DataKeys.Properties.documentModelTermData), "false"));
    // the data store may be empty, so no check here
    return state;
  }

  /**
   * Checks if data for index term frequencies are available.
   *
   * @return True, if data is available
   */
  private boolean hasTermFreqMap() {
    final boolean state = Boolean.parseBoolean(Environment.getProperty(
            PREFIX, DataKeys.get(DataKeys.Properties.indexTermFreq),
            "false"));
    return state && !this.termFreqMap.isEmpty();
  }

  /**
   * Tries to read the cached data and recalculates the data if desired.
   *
   * @return True, if all data could be read, false if recalculation is needed
   * and automatic recalculation was not enabled
   * @throws IOException Thrown, on low-level errors while accessing the
   * cached data
   */
  public boolean tryGetStoredData() throws IOException {
    LOG.info("Trying to get disk storage ({})", Environment.getDataPath());

    boolean needsRecalc;

    // try load cached data
    final TimeMeasure timeMeasure = new TimeMeasure().start();
    initStorage(false);
    timeMeasure.stop();

    // check if storage meta information is there and fields are defined
    final String targetFields = Environment.getProperty(PREFIX,
            CachedIndexDataProvider.DataKeys.get(
                    CachedIndexDataProvider.DataKeys.Properties.fields));

    if (targetFields == null) {
      LOG.info(
              "No chached field information specified in meta information. "
              + "Need to recalculate values.");
      needsRecalc = true;
    } else {
      // check if data was loaded
      needsRecalc = !hasDocModels() || !hasDocTermData()
              || !hasTermFreqMap();
      if (!needsRecalc) {
        LOG.info("Loading cache (docModels={} termFreq={}) "
                + "took {}.", this.docModels.size(),
                this.termFreqMap.size(), timeMeasure.getTimeString());
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(
                "Recalc needed: hasDocModels({}) hasDocTermData({}) "
                + "hasTermFreqMap({})",
                hasDocModels(), hasDocTermData(),
                hasTermFreqMap());
      }
    }
    return !needsRecalc;
  }

  /**
   * Initializes the stored data. Tries to load the data, if any was found and
   * <tt>clear</tt> is not true.
   *
   * @param clear If true all data will be erased before initialization. This
   * is meant for rebuilding the index.
   */
  private void initStorage(final boolean clear) {
    if (this.db.isClosed()) {
      createDb();
    }

    // base document->term-frequency data
    DB.HTreeMapMaker dmBase = this.db.createHashMap(DataKeys.get(
            DataKeys.DataBase.documentModel));
    dmBase.keySerializer(Serializer.INTEGER);
    dmBase.valueSerializer(Serializer.LONG);
    dmBase.counterEnable();

    DB.BTreeMapMaker dmTD = this.db.createTreeMap(
            DataKeys.get(DataKeys.DataBase.documentModelTermData));
    // comparators set to null to use default values
//    final BTreeKeySerializer dmTDKeySerializer
//            = new BTreeKeySerializer.Tuple3KeySerializer(null, null,
//                    Serializer.INTEGER, Serializer.STRING_INTERN,
//                    new BytesWrap.Serializer());
//    dmTD.keySerializer(dmTDKeySerializer);
    dmTD.keySerializer(BTreeKeySerializer.TUPLE3);
    dmTD.valueSerializer(Serializer.JAVA);
//    dmTD.valuesOutsideNodesEnable();
    dmTD.counterEnable();

    final DB.HTreeMapMaker tfmMkr = this.db.createHashMap(
            DataKeys.get(DataKeys.DataBase.indexTermFreq));
    tfmMkr.keySerializer(new BytesWrap.Serializer());

    // initialize known prefixes set
    final BTreeSetMaker prefixMkr = this.db.createTreeSet(DataKeys.get(
            DataKeys.DataBase.prefixes));
    prefixMkr.counterEnable();
    prefixMkr.serializer(BTreeKeySerializer.STRING);

    if (clear) {
      LOG.trace("Clearing all stored data.");
      this.db.delete(DataKeys.get(DataKeys.DataBase.documentModel));
      this.docModels = dmBase.make();
      this.db.delete(DataKeys.get(DataKeys.DataBase.documentModelTermData));
      this.docTermDataInternal = dmTD.make();
      this.db.delete(DataKeys.get(DataKeys.DataBase.indexTermFreq));
      this.termFreqMap = tfmMkr.make();
      ((Bind.MapWithModificationListener) this.termFreqMap).
              addModificationListener(new TermFreqMapChangeListener());
      this.db.delete(DataKeys.get(DataKeys.DataBase.prefixes));
      this.knownPrefixes = prefixMkr.make();
    } else {
      this.docModels = dmBase.makeOrGet();
      if (!this.docModels.isEmpty()) {
        LOG.info("DocumentModels loaded. ({} entries)", this.docModels.
                size());
      }
      this.docTermDataInternal = dmTD.makeOrGet();
      if (!this.docTermDataInternal.isEmpty()) {
        LOG.info("DocumentModel TermData loaded. ({} entries)",
                this.docTermDataInternal.size());
      }
      this.termFreqMap = tfmMkr.makeOrGet();
      if (!this.termFreqMap.isEmpty()) {
        LOG.info("Term frequency map loaded. ({} entries)",
                this.termFreqMap.
                size());
      }
      this.knownPrefixes = prefixMkr.makeOrGet();
      if (!this.knownPrefixes.isEmpty()) {
        LOG.info("Prefixes for DocumentModel TermData loaded. ({} entries)",
                this.knownPrefixes.size());
      }
    }
    this.storageInitialized = true;
    // load after knownPrefixes are available
    this.externalTermData = new ExternalDocTermDataManager(this.db,
            PREFIX);
    try {
      Runtime.getRuntime().addShutdownHook(this.shutdowHandler);
    } catch (IllegalArgumentException ex) {
      // already registered, or shutdown is currently happening
    }
  }

  /**
   * Force recalculation of cached index informations.
   *
   * @param all If true, all data will be recalculated. If false, only missing
   * data will be recalculated.
   * @throws java.io.IOException IOException Thrown, on low-level errors
   * @throws DocumentModelException Thrown, if the {@link DocumentModel} of
   * the requested type could not be instantiated
   */
  public void recalculateData(final boolean all) throws IOException,
          DocumentModelException {
    boolean recalcAll = all;

    if (!this.storageInitialized) {
      initStorage(false);
    }

    if (recalcAll) {
      LOG.info("Recalculation of all cached data forced.");
    } else {
      LOG.info("Recalculating cached data.");
    }

    // check if fields have changed
    final String currentFields = Environment.getProperty(PREFIX,
            CachedIndexDataProvider.DataKeys.get(
                    CachedIndexDataProvider.DataKeys.Properties.fields));
    if (currentFields == null || !Arrays.equals(currentFields.split(
            FIELD_NAME_SEP), Environment.getFields())) {
      LOG.debug("Fields fields={} targetFields={}", currentFields,
              Environment.getFields());
      LOG.info("Indexed fields changed. "
              + "Recalculation of all cached data forced.");
      recalcAll = true;
    }

    clearTermData();
    // update field data
    Environment.setProperty(PREFIX, DataKeys.get(
            DataKeys.Properties.fields),
            StringUtils.join(Environment.getFields(), FIELD_NAME_SEP));

    // if we don't have term frequency values, we need to recalculate all data
    // to ensure consistency
    if (!recalcAll && !hasTermFreqMap()) {
      LOG.info("No term frequency data found. "
              + "Recalculation of all cached data forced.");
      recalcAll = true;
    }

    if (recalcAll) {
      // clear any possible existing data
      LOG.trace("Recalculation of all cached data triggered.");
      initStorage(true); // clear all data
      Environment.setProperty(PREFIX, DataKeys.get(
              DataKeys.Properties.indexTermFreq), "false");
      Environment.setProperty(PREFIX, DataKeys.get(
              DataKeys.Properties.documentModel), "false");
      Environment.setProperty(PREFIX, DataKeys.get(
              DataKeys.Properties.documentModelTermData), "false");
      Environment.setProperty(PREFIX, DataKeys.get(
              DataKeys.Properties.documentModelTermFreq), "false");
    }

    // NOTE: Order of calculation steps matters!
    // 1. Term-Frequencies: this fills the term-frequency map
    if (recalcAll || !hasTermFreqMap()) {
      strictTransactionHookRequest();
      calculateTermFrequencies(Environment.getIndexReader());

      // 2. Relative Term-Frequency: Calculate the relative frequency
      // of each term found in step 1.
      calculateRelativeTermFrequencies(termFreqMap.keySet());

      transactionHookRelease();
      commitHook();

      // save state
      Environment.setProperty(PREFIX, DataKeys.get(
              DataKeys.Properties.indexTermFreq), "true");
    }

    // 3. Create document models
    if (recalcAll || !hasDocModels() || !hasDocTermData()) {
      // calculate
      strictTransactionHookRequest();
      createDocumentModels(Environment.getIndexReader());
      transactionHookRelease();
      // save results
      commitHook();
      // save state
      Environment.setProperty(PREFIX, DataKeys.get(
              DataKeys.Properties.documentModel), "true");
      Environment.setProperty(PREFIX, DataKeys.get(
              DataKeys.Properties.documentModelTermData), "true");
      Environment.setProperty(PREFIX, DataKeys.get(
              DataKeys.Properties.documentModelTermFreq), "true");
    }
  }

  /**
   * Calculates the relative term frequency for each term in the index.
   * Overall term frequency values must be calculated beforehand by calling
   * {@link AbstractIndexDataProvider#calculateTermFrequencies(IndexReader)}.
   *
   * @param terms List of terms to do the calculation for. Usually this is a
   * list of all terms known from the index.
   */
  private void calculateRelativeTermFrequencies(
          final Collection<BytesWrap> terms) {
    checkStorage();
    if (terms == null) {
      throw new IllegalArgumentException("Term set was null.");
    }
    LOG.info("Calculating relative term frequencies for {} terms.", terms.
            size());

    new Processing(new RelTermFreqCalculator(new CollectionSource<>(terms))
    ).process();
  }

  /**
   * Calculate term frequencies for all terms in the index in the initial
   * given fields. This will collect all terms from all specified fields and
   * record their frequency in the whole index.
   *
   * @param reader Reader to access the index
   * @throws IOException Thrown, if the index could not be opened
   */
  private void calculateTermFrequencies(final IndexReader reader)
          throws IOException {
    checkStorage();
    if (reader == null) {
      throw new IllegalArgumentException("Reader was null.");
    }

    final TimeMeasure timeMeasure = new TimeMeasure().start();
    final Fields idxFields = MultiFields.getFields(reader);
    LOG.info("Calculating term frequencies for all unique terms in index. "
            + "This may take some time.");

    Terms fieldTerms;
    TermsEnum fieldTermsEnum = null;

    // go through all fields..
    for (String field : Environment.getFields()) {
      fieldTerms = idxFields.terms(field);

      // ..check if we have terms..
      if (fieldTerms != null) {
        fieldTermsEnum = fieldTerms.iterator(fieldTermsEnum);

        // ..iterate over them..
        BytesRef bytesRef = fieldTermsEnum.next();
        while (bytesRef != null) {
          // fast forward seek to term..
          if (fieldTermsEnum.seekExact(bytesRef)) {
            // ..and update the frequency value for term
            addToTermFreqValue(new BytesWrap(bytesRef), fieldTermsEnum.
                    totalTermFreq());
          }
          bytesRef = fieldTermsEnum.next();
        }
      }
    }
    timeMeasure.stop();
    LOG.info("Calculation of term frequencies for {} unique terms in index "
            + "took {}.", getUniqueTermsCount(), timeMeasure.getTimeString());
  }

  /**
   * Create the document models used by this instance.
   *
   * Since the used {@link Map} implementation is unknown here, a map with
   * immutable objects is assumed and a modification of already stored entries
   * is prohibited. So an entry has to be removed to be updated.
   *
   * @param reader Reader to access the index
   * @throws DocumentModelException Thrown, if the {@link DocumentModel} of
   * the requested type could not be instantiated
   * @throws java.io.IOException Thrown on low-level I7O errors
   */
  private void createDocumentModels(final IndexReader reader) throws
          DocumentModelException, IOException {
    LOG.info("Creating models for approx. {} documents.", reader.
            maxDoc());
    checkStorage();
    if (reader == null) {
      throw new IllegalArgumentException("Reader was null.");
    }

    new Processing(
            new DocModelCreator(new DocModelCreatorSource(reader), reader)
    ).process();
  }

  /**
   * Add a value to the overall term frequency value. Removal is handled by
   * adding negative values.
   *
   * @param mod Value to add to the overall frequency
   */
  protected void addToTermFrequency(final long mod) {
    if (this.overallTermFreq == null) {
      if (mod > 0) {
        this.overallTermFreq = mod;
      } else {
        throw new IllegalStateException(
                "Tried to set a negative term frequency value.");
      }
    } else {
      this.overallTermFreq += mod;
    }
  }

  /**
   * Get the term frequency for a document.
   *
   * @param documentId Document-id to lookup
   * @return Term frequency of the corresponding document, or null if there
   * was no value stored
   */
  private Long getTermFrequency(final int documentId) {
    return this.docModels.get(documentId);
  }

  @Override
  public Collection<BytesWrap> getDocumentsTermSet(
          final Collection<Integer> docIds) {
    final Set<Integer> uniqueDocIds = new HashSet<>(docIds);
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Collection<BytesWrap> terms = new HashSet<>();

    this.docTermDataInternalLock.readLock().lock();

    try {
      for (Integer docId : uniqueDocIds) {
        Iterable<BytesWrap> termsIt = Fun.
                filter(this.docTermDataInternal.navigableKeySet(),
                        docId, DataKeys.get(DataKeys._docTermFreq));
        for (BytesWrap term : termsIt) {
          terms.add(term);
        }
      }
    } catch (Exception ex) {
      LOG.error("Caught exception while getting term frequency value. ", ex);
    } finally {
      this.docTermDataInternalLock.readLock().unlock();
    }
    return terms;
  }

  protected void checkStorage() {
    if (!this.storageInitialized) {
      throw new IllegalStateException("Index not yet initialized.");
    }
  }

  @Override
  public void dispose() {
    LOG.info("Closing cache.");

    // commit changes & close storage
    if (!this.db.isClosed()) {
      this.db.commit();
      this.db.close();
    }
    Runtime.getRuntime().removeShutdownHook(this.shutdowHandler);
    this.storageInitialized = false;
  }

  public void commitHook() {
    checkStorage();
    final TimeMeasure tm = new TimeMeasure().start();
    LOG.info("Updating database..");
    this.db.commit();
    LOG.info("Database updated. (took {})", tm.getTimeString());
  }

  @Override
  public long getUniqueTermsCount() {
    checkStorage();
    return this.termFreqMap.size();
  }

  /**
   * Add a new document (model) to the list if it is not already known.
   *
   * @param docModel DocumentModel to add
   * @return True, if the model was added, false if there's already a model
   * known by the model's id
   */
  protected boolean addDocument(final DocumentModel docModel) {
    checkStorage();
    if (docModel == null) {
      throw new IllegalArgumentException("Model was null.");
    }

    Object oldValue = this.docModels.putIfAbsent(docModel.id,
            docModel.termFrequency);
    if (oldValue != null) {
      // there's already a value stored
      return false;
    }

    for (Entry<BytesWrap, Long> entry : docModel.termFreqMap.entrySet()) {
      if (entry.getValue() == null) {
        throw new NullPointerException("Value was null.");
      }

      if (entry.getKey() == null) {
        throw new NullPointerException("Term was null.");
      }

      this.docTermDataInternalLock.writeLock().lock();
      try {
        this.docTermDataInternal.put(Fun.t3(docModel.id, DataKeys.get(
                DataKeys._docTermFreq), entry.getKey()), entry.getValue());
      } finally {
        this.docTermDataInternalLock.writeLock().unlock();
      }
    }
    return true;
  }

  @Override
  public boolean hasDocument(final Integer docId) {
    checkStorage();
    if (docId == null) {
      return false;
    }
    return this.docModels.containsKey(docId);
  }

  @Override
  public long getTermFrequency() {
    checkStorage();
    if (this.overallTermFreq == null) {
      LOG.info("Collection term frequency not found. Need to recalculate.");
      this.overallTermFreq = 0L;

      for (Fun.Tuple2<Long, Double> freq : this.termFreqMap.values()) {
        if (freq == null) {
          // value should never be null
          throw new IllegalStateException("Frequency value was null.");
        } else {
          this.overallTermFreq += freq.a;
        }
      }
      LOG.debug("Calculated collection term frequency: {}",
              this.overallTermFreq);
    }

    return this.overallTermFreq;
  }

  @Override
  public Long getTermFrequency(final BytesWrap term) {
    checkStorage();
    if (term == null) {
      return null;
    }

    final Fun.Tuple2<Long, Double> freq = this.termFreqMap.get(term);
    Long value;

    if (freq == null) {
      // term not found
      value = null;
    } else {
      value = freq.a;
    }

    return value;
  }

  @Override
  public double getRelativeTermFrequency(final BytesWrap term) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }

    final Fun.Tuple2<Long, Double> freq = this.termFreqMap.get(term);
    if (freq == null) {
      // term not found
      return 0d;
    }
    return freq.b;
  }

  @Override
  public Iterator<BytesWrap> getTermsIterator() {
    checkStorage();
    return Collections.unmodifiableSet(this.termFreqMap.keySet()).iterator();
  }

  @Override
  public long getDocumentCount() {
    checkStorage();
    return this.docModels.size();
  }

  protected void addToTermFreqValue(final BytesWrap term, final long value) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }

    Fun.Tuple2<Long, Double> oldValue;

    oldValue = this.termFreqMap.putIfAbsent(term, Fun.t2(value, 0d));
    if (oldValue == null) {
      // data was not already stored
      return;
    }

    this.termFreqMapLock.writeLock().lock();
    try {
      oldValue = this.termFreqMap.get(term);
      this.termFreqMap.put(term, Fun.t2(oldValue.a + value, oldValue.b));
    } finally {
      this.termFreqMapLock.writeLock().unlock();
    }
  }

  protected void setTermFreqValue(final BytesWrap term, final double value) {
    checkStorage();
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }

    this.termFreqMapLock.writeLock().lock();
    try {
      final Fun.Tuple2<Long, Double> tfData = this.termFreqMap.get(term);
      if (tfData == null) {
        throw new IllegalStateException("Term " + term + " not found.");
      }
      this.termFreqMap.put(term, Fun.t2(tfData.a, value));
    } finally {
      this.termFreqMapLock.writeLock().unlock();
    }
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    checkStorage();
    if (this.hasDocument(docId)) {
      final DocumentModelBuilder dmBuilder = new DocumentModelBuilder(
              docId);
//      dmBuilder.setTermFrequency(this.docModels.get(docId));

      this.docTermDataInternalLock.readLock().lock();
      // add term frequency values
      Iterator<BytesWrap> tfIt = Fun.
              filter(this.docTermDataInternal.navigableKeySet(),
                      docId, DataKeys.get(DataKeys._docTermFreq)).
              iterator();

      try {
        while (tfIt.hasNext()) {
          try {
            final BytesWrap term = tfIt.next();
            final Fun.Tuple3 dataTuple = Fun.t3(docId, DataKeys.get(
                    DataKeys._docTermFreq), term);
            final Object data = this.docTermDataInternal.get(dataTuple);
            if (data == null) {
              LOG.error(
                      "Encountered null while adding term frequency values "
                      + "to document model. key={} docId={} "
                      + "term={} termStr={}", DataKeys.get(
                              DataKeys._docTermFreq), docId, term, term);
            } else {
              dmBuilder.setTermFrequency(term, (Long) data);
            }
          } catch (Exception ex) {
            LOG.error(
                    "Caught exception while setting term frequency value. "
                    + "key={} docId={}", DataKeys.get(DataKeys._docTermFreq),
                    docId, ex);
          }
        }
      } catch (Exception ex) {
        LOG.error(
                "Caught exception while setting term frequency value. "
                + "docId={}", docId, ex);
      } finally {
        this.docTermDataInternalLock.readLock().unlock();
      }
      return dmBuilder.getModel();
    }
    return null;
  }

  @Override
  public Iterator<Integer> getDocumentIdIterator() {
    checkStorage();
    return this.docModels.keySet().iterator();
  }

  @Override
  public Object setTermData(final String prefix,
          final int documentId,
          final BytesWrap term,
          final String key,
          final Object value
  ) {
    checkStorage();
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }

    if (INTERNAL_PREFIX.equals(prefix)
            || prefix.startsWith(INTERNAL_PREFIX)) {
      throw new IllegalArgumentException("The sequence '"
              + INTERNAL_PREFIX
              + "' is not allowed as prefix, as it's reserved "
              + "for internal purpose.");
    }

    return this.externalTermData.setData(prefix, documentId, term, key,
            value);
  }

  /**
   * Returns the internal term-data-map. Meant only for debugging purpose as
   * changing data may cause inconsistency and break things.
   *
   * @return Internal term-data-map
   */
  public Map<Fun.Tuple3<Integer, String, BytesWrap>, Object>
          debugGetInternalTermDataMap() {
    return Collections.unmodifiableMap(this.docTermDataInternal);
  }

  /**
   * Get a list of known prefixes, needed to access externally stored
   * document-term data. Meant only for debugging purpose.
   *
   * @return List of known prefixes.
   */
  public Collection<String> debugGetKnownPrefixes() {
    checkStorage();
    return Collections.unmodifiableCollection(getKnownPrefixes());
  }

  /**
   * Get the number of known prefixes for external document term-data.
   *
   * @return Number of known prefixes
   */
  protected int getKnownPrefixCount() {
    checkStorage();
    return this.knownPrefixes.size();
  }

  /**
   * Get a map with externally stored document-term data entries, identified
   * by the given prefix. Meant only for debugging purpose as changing data
   * may cause inconsistency and break things.
   *
   * @param prefix Prefix to identify the map
   * @return Map associated with the given prefix, or <tt>null</tt> if there
   * was no map stored with the given prefix
   */
  public Map<Fun.Tuple2<String, String>, Object> debugGetPrefixMap(
          final String prefix) {

//        if (debugGetKnownPrefixes().contains(prefix)) {
//            return this.externalTermData.getDataMap(prefix);
//        }
//        return null;
    throw new UnsupportedOperationException("BROKEN!"); // FIXME!
  }

  @Override
  public void clearTermData() {
    checkStorage();
    this.externalTermData.clear();
    getKnownPrefixes().clear();
  }

  @Override
  public Object getTermData(final String prefix, final int documentId,
          final BytesWrap term, final String key) {
    checkStorage();
    return this.externalTermData.getData(prefix, documentId, term, key);
  }

  @Override
  public Map<BytesWrap, Object> getTermData(
          final String prefix, final int documentId, final String key) {
    checkStorage();
    return this.externalTermData.getData(prefix, documentId, key);
  }

  public boolean transactionHookRequest() {
    checkStorage();
    if (this.rollback) {
      return false;
    }
    this.rollback = true;
    LOG.trace("Transaction hook set.");
    return true;
  }

  /**
   * Request a transaction hook or throw an error if it does not succeed.
   */
  private void strictTransactionHookRequest() {
    if (!transactionHookRequest()) {
      throw new IllegalStateException("Failed to aquire transaction hook.");
    }
  }

  public void transactionHookRelease() {
    checkStorage();
    if (!this.rollback) {
      throw new IllegalStateException("No transaction hook set.");
    }
    LOG.trace("Transaction hook released.");
    this.rollback = false;
  }

  public void transactionHookRoolback() {
    checkStorage();
    transactionHookRelease();
    LOG.warn("Rolling back changes.");
    this.db.rollback();
  }

  public Collection<Integer> documentsContaining(final BytesWrap term) {
    checkStorage();
    final Collection<Integer> matchingDocIds = new ArrayList<>();
    for (Integer docId : this.docModels.keySet()) {
      if (!matchingDocIds.contains(docId) && documentContains(docId, term)) {
        matchingDocIds.add(docId);
      }
    }
    return matchingDocIds;
  }

  @Override
  public boolean documentContains(final int documentId, final BytesWrap term) {
    checkStorage();
    return this.docTermDataInternal.containsKey(Fun.t3(documentId,
            DataKeys.get(DataKeys._docTermFreq), term));
  }

  @Override
  public Source<Integer> getDocumentIdSource() {
    checkStorage();
    return new CollectionSource<>(this.docModels.keySet());
  }

  @Override
  public Source<BytesWrap> getTermsSource() {
    checkStorage();
    return new CollectionSource<>(this.termFreqMap.keySet());
  }

  /**
   * Tell the data provider, we want to access custom data specified by the
   * given prefix.
   * <p>
   * A prefix must be registered before any call to
   * {@link #setTermData(String, int, BytesWrap, String, Object)} or null null
   * null null null null null null null null null null   {@link #getTermData(String, int, BytesWrap, String) can be made.
   *
   * @param prefix Prefix name to register
   */
  @Override
  public void registerPrefix(final String prefix) {
    checkStorage();
    this.externalTermData.loadPrefix(prefix);
    getKnownPrefixes().add(prefix);
  }

  /**
   * EventListener tracking changes to term frequencies map.
   */
  private class TermFreqMapChangeListener
          implements MapListener<BytesWrap, Fun.Tuple2<Long, Double>> {

    @Override
    public void update(final BytesWrap key,
            final Fun.Tuple2<Long, Double> oldVal,
            final Fun.Tuple2<Long, Double> newVal) {
      if (newVal == null) {
        // removal
        addToTermFrequency(oldVal.a * -1);
      } else if (oldVal == null) {
        addToTermFrequency(newVal.a);
      } else {
        addToTermFrequency(newVal.a - oldVal.a);
      }
    }
  }

  /**
   * {@link Processing.Target} create document models from a document-id
   * {@link Processing.Source}.
   */
  private final class RelTermFreqCalculator extends Target<BytesWrap> {

    /**
     * @param source {@link Source} for this {@link Target}
     */
    public RelTermFreqCalculator(final Source<BytesWrap> source) {
      super(source);
    }

    @Override
    public Target<BytesWrap> newInstance() {
      return new RelTermFreqCalculator(getSource());
    }

    @Override
    @SuppressWarnings({"BroadCatchBlock", "TooBroadCatch"})
    public void runProcess() {
      final long collFreq = getTermFrequency();
      while (!isTerminating()) {
        try {
          BytesWrap term;
          try {
            term = getSource().next();
          } catch (ProcessingException.SourceHasFinishedException ex) {
            break;
          }

          if (term == null) {
            // nothing found
            continue;
          }
          final Long termFreq = getTermFrequency(term);
          if (termFreq != null) {
            final double rTermFreq = termFreq.doubleValue() / Long.valueOf(
                    collFreq).doubleValue();
            setTermFreqValue(term, rTermFreq);
          }
        } catch (Exception ex) {
          LOG.error("({}) Caught exception while processing term.",
                  getName(), ex);
        }
      }
    }
  }

  /**
   * {@link Processing.Source} providing document-ids to create document
   * models.
   */
  private static final class DocModelCreatorSource extends Source<Integer> {

    /**
     * Expected number of documents to retrieve from Lucene.
     */
    final int itemsCount;
    /**
     * Current number of items provided.
     */
    AtomicInteger currentNum;

    /**
     * Create a new {@link Processing.Source} providing document-ids. Used to
     * generate document models.
     *
     * @param newReader Reader to access Lucene index
     */
    DocModelCreatorSource(final IndexReader newReader) {
      super();
      this.itemsCount = newReader.maxDoc();
      this.currentNum = new AtomicInteger(-1); // first index will be 0
    }

    @Override
    public synchronized Integer next() throws ProcessingException,
            InterruptedException {
      // current count must be smaller, because last item will be returned,
      // before stopping
      if (this.currentNum.incrementAndGet() == itemsCount - 1) {
        stop();
      }
      if (this.currentNum.get() > (itemsCount - 1)) {
        return null;
      }
      return this.currentNum.get();
    }

    @Override
    public Long getItemCount() {
      return (long) this.itemsCount;
    }

    @Override
    public synchronized long getSourcedItemCount() {
      // add 1 to counter, because we start counting at 0
      return this.currentNum.get() < 0 ? 0 : this.currentNum.get() + 1;
    }
  }

  /**
   * {@link Processing.Target} create document models from a document-id
   * {@link Processing.Source}.
   */
  private final class DocModelCreator extends Target<Integer> {

    /**
     * Reader to access Lucene index.
     */
    private final IndexReader reader;

    /**
     * @param newSource {@link Source} for this {@link Target}
     * @param newReader Reader to access Lucene index
     */
    public DocModelCreator(final Source<Integer> newSource,
            final IndexReader newReader) {
      super(newSource);
      this.reader = newReader;
    }

    @Override
    public Target<Integer> newInstance() {
      return new DocModelCreator(getSource(), this.reader);
    }

    @Override
    public void runProcess() throws IOException, ProcessingException,
            InterruptedException {
      final long[] docTermEsitmate = new long[]{0L, 0L, 100L};
      final DocFieldsTermsEnum dftEnum = new DocFieldsTermsEnum();

      BytesRef bytesRef;

      while (!isTerminating()) {
        Integer docId;
        try {
          docId = getSource().next();
        } catch (ProcessingException.SourceHasFinishedException ex) {
          break;
        }

        if (docId == null) {
          continue;
        }

        try {
          // go through all document fields..
          dftEnum.setDocument(docId);
        } catch (IOException ex) {
          LOG.error("({}) Error retrieving document id={}.", getName(),
                  docId, ex);
          continue;
        }

        try {
          // iterate over all terms in all specified fields
          bytesRef = dftEnum.next();
          if (bytesRef == null) {
            // nothing found
            continue;
          }
          final Map<BytesWrap, AtomicLong> docTerms = new HashMap<>(
                  (int) docTermEsitmate[2]);
          while (bytesRef != null) {
            final BytesWrap term = new BytesWrap(bytesRef);

            // update frequency counter for current term
            if (docTerms.containsKey(term)) {
              docTerms.get(term).getAndAdd(dftEnum.getTotalTermFreq());
            } else {
              docTerms.put(term.clone(), new AtomicLong(dftEnum.
                      getTotalTermFreq()));
            }
            bytesRef = dftEnum.next();
          }

          final DocumentModel.DocumentModelBuilder dmBuilder
                  = new DocumentModel.DocumentModelBuilder(docId,
                          docTerms.size());

          // All terms from all document fields are gathered.
          // Store the document frequency of each document term to the model
          for (Entry<BytesWrap, AtomicLong> entry : docTerms.entrySet()) {
            dmBuilder.setTermFrequency(entry.getKey(), entry.getValue().
                    longValue());
          }

          try {
            if (!addDocument(dmBuilder.getModel())) {
              throw new IllegalArgumentException("(" + getName()
                      + ") Document model already known at creation time.");
            }
          } catch (Exception ex) {
            LOG.error("(" + getName() + ") Caught exception "
                    + "while adding document model.", ex);
            continue;
          }

          // estimate size for term buffer
          docTermEsitmate[0] += docTerms.size();
          docTermEsitmate[1]++;
          docTermEsitmate[2] = docTermEsitmate[0] / docTermEsitmate[1];
          // take the default load factor into account
          docTermEsitmate[2] += docTermEsitmate[2] * 0.8;
        } catch (IOException ex) {
          LOG.error("({}) Error while getting terms for document id {}",
                  super.getName(), docId, ex);
          continue;
        }
      }
    }
  }
}
