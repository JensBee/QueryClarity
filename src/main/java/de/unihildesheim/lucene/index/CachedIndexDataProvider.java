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

import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.DocumentModel.DocumentModelBuilder;
import de.unihildesheim.lucene.document.DocumentModelException;
import de.unihildesheim.util.StringUtils;
import de.unihildesheim.util.Configuration;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.util.BytesWrapUtil;
import de.unihildesheim.util.concurrent.ConcurrentMapTools;
import de.unihildesheim.util.TimeMeasure;
import de.unihildesheim.util.concurrent.processing.CollectionSource;
import de.unihildesheim.util.concurrent.processing.Source;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DB.BTreeMapMaker;
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
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class CachedIndexDataProvider extends AbstractIndexDataProvider {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          CachedIndexDataProvider.class);

  /**
   * Prefix used to store configuration.
   */
  private static final String CONF_PREFIX = "CachedIDP_";

  /**
   * Separator to store field names.
   */
  private static final String FIELD_NAME_SEP
          = Configuration.get(CONF_PREFIX + "fieldNameSep", "\\|");

  /**
   * Directory where cached data should be stored.
   */
  private final String storagePath;

  /**
   * Unique identifier for this cache.
   */
  private final String storageId;

  /**
   * Storage meta data.
   */
  private transient Properties storageProp = null;

  /**
   * Disk storage backend.
   */
  private DB db;

  /**
   * Store mapping of <tt>term : frequency values</tt>.
   */
  private ConcurrentMap<BytesWrap, Fun.Tuple2<Long, Double>> termFreqMap;

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
   * Mapping function to create fast lookup indices for external prefixed
   * stored term-data.
   */
  private static final Fun.Function2 PREFIXMAP_INDEX_FUNC
          = new Fun.Function2<
              String, Fun.Tuple3<Integer, BytesWrap, String>, Object>() {
            @Override
            public String run(Fun.Tuple3<Integer, BytesWrap, String> key,
                    Object value) {
              // create a lookup list based on docmentid+term which points
              // to the original map key
              return key.a.toString() + BytesWrapUtil.bytesWrapToString(
                      key.b);
            }
          };

  /**
   * Creates a new disk backed (cached) {@link IndexDataProvider} with the
   * given storage path.
   *
   * @param newStorageId Unique identifier for this cache
   * @throws IOException Thrown on low-level I/O errors
   */
  public CachedIndexDataProvider(final String newStorageId) throws IOException {
    if (newStorageId.isEmpty()) {
      throw new IllegalArgumentException("Missing storage information.");
    }

    this.storagePath = Configuration.get(CONF_PREFIX
            + "storagePath", "data/cache/");
    LOG.info("Created IndexDataProvider::{} instance storage={}.", this.
            getClass().getCanonicalName(), this.storagePath);
    this.storageId = newStorageId;

    // create the manager for disk storage
    try {
      final DBMaker dbMkr = DBMaker.newFileDB(new File(this.storagePath,
              this.storageId));
//      dbMkr.mmapFileEnableIfSupported(); // use mmaped files, if supported
//      dbMkr.fullChunkAllocationEnable(); // allocate space in 1GB steps
      dbMkr.cacheLRUEnable(); // enable last-recent-used cache
      dbMkr.cacheSoftRefEnable();
      this.db = dbMkr.make();
    } catch (RuntimeException ex) {
      LOG.error(
              "Caught runtime exception. Maybe your classes have changed."
              + "You may want to delete the cache, because it's invalid now.");
    }

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
    Runtime.getRuntime().addShutdownHook(this.shutdowHandler);

    getStorageInfo();
  }

  protected DB getDb() {
    return db;
  }

  protected Collection<String> getKnownPrefixes() {
    return knownPrefixes;
  }

  /**
   * Try to read the properties file stored alongside with the cached data.
   *
   * @return True, if the file is there, false otherwise
   * @throws IOException Thrown on low-level I/O errors
   */
  private boolean getStorageInfo() throws IOException {
    this.storageProp = new Properties();
    boolean hasProp;
    final File propFile = new File(this.storagePath, this.storageId
            + ".properties");
    try {
      try (FileInputStream propStream = new FileInputStream(propFile)) {
        this.storageProp.load(propStream);
      }

      final String targetFields = this.storageProp.getProperty(
              DataKeys.get(DataKeys.Properties.fields));
      if (targetFields == null) {
        // no fields specified
        this.setFields(new String[0]);
      } else {
        this.setFields(targetFields.split(FIELD_NAME_SEP));
      }

      hasProp = true;
    } catch (FileNotFoundException ex) {
      LOG.trace("Cache meta file " + propFile + " not found.", ex);
      hasProp = false;
    }
    return hasProp;
  }

  /**
   * Checks, if document models are available.
   *
   * @return True, if data is available
   */
  private boolean hasDocModels() {
    final boolean state = Boolean.parseBoolean(this.storageProp.getProperty(
            DataKeys.get(DataKeys.Properties.documentModel), "false"));
    return state && !this.docModels.isEmpty();
  }

  /**
   * Checks, if document -> term-data is available.
   *
   * @return True, if data is available
   */
  private boolean hasDocTermData() {
    final boolean state = Boolean.parseBoolean(this.storageProp.getProperty(
            DataKeys.get(DataKeys.Properties.documentModelTermData), "false"));
    // the data store may be empty, so no check here
    return state;
  }

  /**
   * Checks if data for index term frequencies are available.
   *
   * @return True, if data is available
   */
  private boolean hasTermFreqMap() {
    final boolean state = Boolean.parseBoolean(this.storageProp.getProperty(
            DataKeys.get(DataKeys.Properties.indexTermFreq), "false"));
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
    LOG.info("Trying to get disk storage ({})", this.storagePath);

    boolean needsRecalc;

    // try load cached data
    final TimeMeasure timeMeasure = new TimeMeasure().start();
    initStorage(false);
    timeMeasure.stop();

    // check if storage meta information is there and fields are defined
    if (getStorageInfo()) {
      if (this.getFields().length == 0) {
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
        // debug
        if (LOG.isTraceEnabled()) {
          for (Entry<BytesWrap, Fun.Tuple2<Long, Double>> data
                  : this.termFreqMap.entrySet()) {
            LOG.trace("load: t={} f={} rf={}", data.getKey(),
                    data.getValue().a,
                    data.getValue().b);
          }
        }
      }
    } else {
      LOG.info("No or not all cache meta information found. "
              + "Need to recalculate some or all values.");
      needsRecalc = true;
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
    // base document->term-frequency data
    DB.HTreeMapMaker dmBase = this.db.createHashMap(DataKeys.get(
            DataKeys.DataBase.documentModel));
    dmBase.keySerializer(Serializer.INTEGER);
    dmBase.valueSerializer(Serializer.LONG);
    dmBase.counterEnable();

    DB.BTreeMapMaker dmTD = this.db.createTreeMap(
            DataKeys.get(DataKeys.DataBase.documentModelTermData));
    // comparators set to null to use default values
    final BTreeKeySerializer dmTDKeySerializer
            = new BTreeKeySerializer.Tuple3KeySerializer(null, null,
                    Serializer.INTEGER, Serializer.STRING_INTERN,
                    new BytesWrap.Serializer());
    dmTD.keySerializer(dmTDKeySerializer);
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
    // load after knownPrefixes are available
    this.externalTermData = new ExternalDocTermDataManager();
  }

  /**
   * Check if all requested fields are available in the current index.
   *
   * @param indexReader Reader to access the index
   */
  private void checkFields(final IndexReader indexReader) {
    // get all indexed fields from index - other fields are not of interes here
    final Collection<String> indexedFields = MultiFields.getIndexedFields(
            indexReader);

    // check if all requested fields are available
    if (!indexedFields.containsAll(Arrays.asList(this.getFields()))) {
      throw new IllegalStateException(MessageFormat.format(
              "Not all requested fields ({0}) "
              + "are available in the current index ({1}) or are not indexed.",
              this.getFields(), Arrays.toString(indexedFields.toArray(
                              new String[indexedFields.size()]))));
    }
  }

  /**
   * Force recalculation of cached index informations.
   *
   * @param indexReader Reader to use to access the index
   * @param targetFields Index fields to gather data from
   * @param all If true, all data will be recalculated. If false, only missing
   * data will be recalculated.
   * @throws java.io.IOException IOException Thrown, on low-level errors
   * @throws DocumentModelException Thrown, if the {@link DocumentModel} of
   * the requested type could not be instantiated
   */
  public void recalculateData(final IndexReader indexReader,
          final String[] targetFields, final boolean all) throws IOException,
          DocumentModelException {
    if (indexReader == null) {
      throw new IllegalArgumentException("No index reader specified.");
    }
    if (targetFields == null || targetFields.length == 0) {
      throw new IllegalArgumentException("No target fields specified.");
    }

    boolean recalcAll = all;

    if (recalcAll) {
      LOG.info("Recalculation of all cached data forced.");
    }

    // check parameter sanity
    if (targetFields.length == 0) {
      throw new IllegalArgumentException(
              "Empty list of target fields given.");
    }

    // check if fields have changed
    if (this.getFields().length == 0 || !Arrays.equals(this.getFields(),
            targetFields)) {
      LOG.debug("Fields fields={} targetFields={}", this.getFields(),
              targetFields);
      this.setFields(targetFields.clone());
      LOG.info("Indexed fields changed. "
              + "Recalculation of all cached data forced.");
      recalcAll = true;
    }

    checkFields(indexReader);
    // update field data
    this.storageProp.setProperty(DataKeys.get(DataKeys.Properties.fields),
            StringUtils.join(targetFields, FIELD_NAME_SEP));
    saveMetadata();

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
      this.storageProp.setProperty(DataKeys.get(
              DataKeys.Properties.indexTermFreq), "false");
      this.storageProp.setProperty(DataKeys.get(
              DataKeys.Properties.documentModel), "false");
      this.storageProp.setProperty(DataKeys.get(
              DataKeys.Properties.documentModelTermData),
              "false");
      this.storageProp.setProperty(DataKeys.get(
              DataKeys.Properties.documentModelTermFreq), "false");
      saveMetadata();
    }

    // NOTE: Order of calculation steps matters!
    // 1. Term-Frequencies: this fills the term-frequency map
    if (recalcAll || !hasTermFreqMap()) {
      strictTransactionHookRequest();
      calculateTermFrequencies(indexReader);

      // 2. Relative Term-Frequency: Calculate the relative frequency
      // of each term found in step 1.
      calculateRelativeTermFrequencies(termFreqMap.keySet());

      transactionHookRelease();
      commitHook();

      // save state
      this.storageProp.setProperty(DataKeys.get(
              DataKeys.Properties.indexTermFreq), "true");
      saveMetadata();
    }

    // 3. Create document models
    if (recalcAll || !hasDocModels() || !hasDocTermData()) {
      // calculate
      strictTransactionHookRequest();
      createDocumentModels(indexReader);
      transactionHookRelease();
      // save results
      commitHook();
      // save state
      this.storageProp.setProperty(DataKeys.get(
              DataKeys.Properties.documentModel), "true");
      this.storageProp.setProperty(DataKeys.get(
              DataKeys.Properties.documentModelTermData), "true");
      this.storageProp.setProperty(DataKeys.get(
              DataKeys.Properties.documentModelTermFreq), "true");
      saveMetadata();
    }

    // update meta data
    saveMetadata();
  }

  /**
   * Save meta information for stored data.
   *
   * @throws IOException If there where any low-level I/O errors
   */
  private void saveMetadata() throws IOException {
    this.storageProp.
            setProperty(DataKeys.get(DataKeys.Properties.timestamp),
                    new SimpleDateFormat("MM/dd/yyyy h:mm:ss a").format(
                            new Date()));
    final File propFile = new File(this.storagePath, this.storageId
            + ".properties");
    try (FileOutputStream propFileOut = new FileOutputStream(propFile)) {
      this.storageProp.store(propFileOut, "");
    }
  }

  /**
   * Set the overall frequency of all terms in the index.
   *
   * @param oTermFreq Overall frequency of all terms in the index
   */
  private void setTermFrequency(final Long oTermFreq) {
    this.overallTermFreq = oTermFreq;
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
  public void dispose() {
    try {
      // update meta-data
      saveMetadata();
    } catch (IOException ex) {
      LOG.error("Error while storing meta informations.", ex);
    }

    // commit changes & close storage
    if (!this.db.isClosed()) {
      this.db.commit();
      this.db.close();
    }
    Runtime.getRuntime().removeShutdownHook(this.shutdowHandler);
  }

  @Override
  public void setProperty(final String prefix, final String key,
          final String value) {
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Null is not allowed as value.");
    }
    this.storageProp.setProperty(prefix + '_' + key, value);
  }

  @Override
  public String getProperty(final String prefix, final String key) {
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    return this.storageProp.getProperty(prefix + "_" + key);
  }

  @Override
  public String getProperty(final String prefix, final String key,
          final String defaultValue) {
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    return this.storageProp.getProperty(prefix + "_" + key, defaultValue);
  }

  @Override
  public void commitHook() {
    final TimeMeasure tm = new TimeMeasure().start();
    LOG.trace("Updating database..");
    this.db.commit();
    LOG.info("Database updated. (took {})", tm.getTimeString());
  }

  @Override
  public long getUniqueTermsCount() {
    return this.termFreqMap.size();
  }

  @Override
  public boolean addDocument(final DocumentModel docModel) {
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
      this.docTermDataInternal.putIfAbsent(Fun.t3(docModel.id, DataKeys.
              get(
                      DataKeys._docTermFreq), entry.getKey()), entry.
              getValue());
    }
    return true;
  }

  @Override
  public void updateDocument(final DocumentModel docModel) {
    if (docModel == null) {
      throw new IllegalArgumentException("Model was null.");
    }
    ConcurrentMapTools.ensurePut(this.docModels, docModel.id,
            docModel.termFrequency);

    for (Entry<BytesWrap, Long> entry : docModel.termFreqMap.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        throw new NullPointerException("Encountered null value in "
                + "termFreqMap.");
      }
      ConcurrentMapTools.ensurePut(this.docTermDataInternal, Fun.t3(
              docModel.id, DataKeys.get(DataKeys._docTermFreq),
              entry.getKey()), entry.getValue());
    }
  }

  @Override
  public boolean hasDocument(final Integer docId) {
    if (docId == null) {
      return false;
    }
    return this.docModels.containsKey(docId);
  }

  @Override
  public long getTermFrequency() {
    if (this.overallTermFreq == null) {
      LOG.
              info("Collection term frequency not found. Need to recalculate.");
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

    LOG.trace("TermFrequency t={} f={}", term, value);
    return value;
  }

  @Override
  public double getRelativeTermFrequency(final BytesWrap term) {
    if (term == null) {
      return 0d;
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
    return Collections.unmodifiableSet(this.termFreqMap.keySet()).iterator();
  }

  @Override
  public long getDocumentCount() {
    return this.docModels.size();
  }

  @Override
  protected void addToTermFreqValue(final BytesWrap term, final long value) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }

    Fun.Tuple2<Long, Double> oldValue;

    try {
      for (;;) {
        oldValue = this.termFreqMap.putIfAbsent(term, Fun.t2(value, 0d));
        if (oldValue == null) {
          // data was not already stored
          break;
        }

        if (this.termFreqMap.replace(term, oldValue, Fun.
                t2(oldValue.a + value,
                        oldValue.b))) {
          // replacing actually worked
          break;
        }
      }
    } catch (Exception ex) {
      LOG.error("Catched exception while updating term frequency value.",
              ex);
    }
    this.setTermFrequency(this.getTermFrequency() + value);
  }

  @Override
  protected void setTermFreqValue(final BytesWrap term, final double value) {
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }

    Fun.Tuple2<Long, Double> tfData;
    for (;;) {
      tfData = this.termFreqMap.get(term);
      if (tfData == null) {
        throw new IllegalStateException("Term " + BytesWrapUtil.
                bytesWrapToString(term) + " not found.");
      }
      if (this.termFreqMap.replace(term, tfData, Fun.t2(tfData.a, value))) {
        // replacing actually worked
        break;
      }
    }
  }

  @Override
  public DocumentModel getDocumentModel(final int docId) {
    if (this.hasDocument(docId)) {
      final DocumentModelBuilder dmBuilder = new DocumentModelBuilder(
              docId);
      dmBuilder.setTermFrequency(this.docModels.get(docId));

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
            final Object data = this.docTermDataInternal.get(
                    dataTuple);
            if (data == null) {
              LOG.error(
                      "Encountered null while adding term frequency values "
                      + "to document model. key={} docId={} "
                      + "term={} termStr={}", DataKeys.get(
                              DataKeys._docTermFreq), docId, term,
                      BytesWrapUtil.bytesWrapToString(term));
            } else {
              dmBuilder.setTermFrequency(term, ((Number) data).
                      longValue());
            }
          } catch (Exception ex) {
            LOG.error(
                    "Caught exception while setting term frequency value. "
                    + "key={} docId={}", DataKeys.get(
                            DataKeys._docTermFreq),
                    docId, ex);
          }
        }
      } catch (Exception ex) {
        LOG.error(
                "Caught exception while setting term frequency value. "
                + "docId={}", docId, ex);
      }
      return dmBuilder.getModel();
    }
    return null;
  }

  @Override
  public Iterator<Integer> getDocumentIdIterator() {
    return this.docModels.keySet().iterator();
  }

  @Override
  public Object setTermData(final String prefix,
          final int documentId,
          final BytesWrap term,
          final String key,
          final Object value
  ) {
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (term == null) {
      throw new IllegalArgumentException("Term was null.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Null is not allowed as value.");
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
    return Collections.unmodifiableCollection(this.knownPrefixes);
  }

  /**
   * Get the number of known prefixes for external document term-data.
   *
   * @return Number of known prefixes
   */
  protected int getKnownPrefixCount() {
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
  public Object getTermData(final String prefix, final int documentId,
          final BytesWrap term, final String key) {
    return this.externalTermData.getData(prefix, documentId, term, key);
  }

  @Override
  public Map<BytesWrap, Object> getTermData(
          final String prefix, final int documentId, final String key) {
    return this.externalTermData.getData(prefix, documentId, key);
  }

  @Override
  public boolean transactionHookRequest() {
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

  @Override
  public void transactionHookRelease() {
    if (!this.rollback) {
      throw new IllegalStateException("No transaction hook set.");
    }
    LOG.trace("Transaction hook released.");
    this.rollback = false;
  }

  @Override
  public void transactionHookRoolback() {
    transactionHookRelease();
    LOG.warn("Rolling back changes.");
    this.db.rollback();
  }

  @Override
  public boolean documentContains(final int documentId, final BytesWrap term) {
    return this.docTermDataInternal.containsKey(Fun.t3(documentId,
            DataKeys.get(DataKeys._docTermFreq), term));
  }

  @Override
  public Source<Integer> getDocumentIdSource() {
    return new CollectionSource<>(this.docModels.keySet());
  }

  @Override
  public Source<BytesWrap> getTermsSource() {
    return new CollectionSource<>(this.termFreqMap.keySet());
  }

  @Override
  public void registerPrefix(final String prefix) {
    this.externalTermData.loadPrefix(prefix);
  }

  /**
   * Manages the externally stored document term-data.
   */
  private class ExternalDocTermDataManager {

    /**
     * Store an individual map for each prefix.
     */
    private final Map<String, BTreeMap<
                Fun.Tuple3<Integer, String, BytesWrap>, Object>> prefixMap;

    /**
     * Initialize the manager.
     */
    ExternalDocTermDataManager() {
      this.prefixMap = new HashMap<>(getKnownPrefixCount());
    }

    /**
     * Check, if the given prefix is known. Throws a runtime
     * {@link Exception}, if the prefix is not known.
     *
     * @param prefix Prefix to check
     */
    private void checkPrefix(final String prefix) {
      if (!this.prefixMap.containsKey(prefix)) {
        throw new IllegalArgumentException(
                "Prefixed data was not known. "
                + "Was prefix '" + prefix
                + "' registered before being accessed?");
      }
    }

    /**
     * Get the document term-data map for a given prefix.
     *
     * @param prefix Prefix to lookup
     * @return Map containing stored data for the given prefix, or null if
     * there was no such prefix
     */
    private BTreeMap<Fun.Tuple3<Integer, String, BytesWrap>, Object> getDataMap(
            final String prefix) {
      return this.prefixMap.get(prefix);
    }

    /**
     * Loads a stored prefix map from the database into the cache.
     *
     * @param prefix Prefix to load
     */
    private void loadPrefix(final String prefix) {
      final String mapName = DataKeys.get(DataKeys._external) + "_"
              + prefix;

      // stored data
      BTreeMap<Fun.Tuple3<Integer, String, BytesWrap>, Object> map;

      // try get stored data, or create it if not existant
      if (getDb().exists(mapName)) {
        map = getDb().get(mapName);
      } else {
        LOG.debug("Creating a new docTermData map with prefix '{}'",
                prefix);
        // create a new map
        BTreeMapMaker mapMkr = getDb().createTreeMap(mapName);
        final BTreeKeySerializer mapKeySerializer
                = new BTreeKeySerializer.Tuple3KeySerializer(null, null,
                        Serializer.INTEGER, Serializer.STRING_INTERN,
                        new BytesWrap.Serializer());
        mapMkr.keySerializer(mapKeySerializer);
        mapMkr.valueSerializer(Serializer.JAVA);
        map = mapMkr.makeOrGet();

        // store for reference
        getKnownPrefixes().add(prefix);
      }
      this.prefixMap.put(prefix, map);
    }

    /**
     * Store ter-data to the database.
     *
     * @param prefix Data prefix to use
     * @param documentId Document-id the data belongs to
     * @param term Term the data belongs to
     * @param key Key to identify the data
     * @param value Value to store
     * @return Any previous assigned data, or null, if there was none
     */
    private Object setData(final String prefix, final int documentId,
            final BytesWrap term, final String key, final Object value) {
      checkPrefix(prefix);
      return ConcurrentMapTools.ensurePut(this.prefixMap.get(prefix),
              Fun.t3(documentId, key, term), value);
    }

    /**
     * Get stored document term-data for a specific prefix and key. This
     * returns only <tt>term, value</tt> pairs for the given document and
     * prefix.
     *
     * @param prefix Prefix to lookup
     * @param documentId Document-id whose data to get
     * @param key Key to identify the data to get
     * @return Map with stored data for the given combination
     */
    private Map<BytesWrap, Object> getData(
            final String prefix, final int documentId, final String key) {
      if (prefix == null || prefix.isEmpty()) {
        throw new IllegalArgumentException("No prefix specified.");
      }
      if (key == null || key.isEmpty()) {
        throw new IllegalArgumentException("Key may not be null or empty.");
      }
      BTreeMap<Fun.Tuple3<Integer, String, BytesWrap>, Object> dataMap
              = getDataMap(prefix);
      // use the documents term frequency as initial size for the map
      Map<BytesWrap, Object> map = new HashMap<>(
              (int) (long) getTermFrequency(documentId));
      for (BytesWrap term : Fun.filter(dataMap.keySet(), documentId, key)) {
        map.put(term, dataMap.get(Fun.t3(documentId, key, term)));
      }

      return map;
    }

    /**
     * Get a single stored document term-data value for a specific prefix,
     * term and key.
     *
     * @param prefix Prefix to lookup
     * @param documentId Document-id whose data to get
     * @param key Key to identify the data to get
     * @param term Term to lookup
     * @return Value stored for the given combination, or null if there was no
     * data stored
     */
    private Object getData(final String prefix, final int documentId,
            final BytesWrap term, final String key) {
      if (term == null) {
        throw new IllegalArgumentException("Term was null.");
      }
      if (prefix == null || prefix.isEmpty()) {
        throw new IllegalArgumentException("No prefix specified.");
      }
      if (key == null || key.isEmpty()) {
        throw new IllegalArgumentException("Key may not be null or empty.");
      }
      checkPrefix(prefix);

      return this.prefixMap.get(prefix).get(Fun.t3(documentId, key, term));
    }
  }
}
