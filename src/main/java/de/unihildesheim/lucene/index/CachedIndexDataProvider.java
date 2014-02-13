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

import de.unihildesheim.lucene.document.model.DocumentModel;
import de.unihildesheim.lucene.document.model.DocumentModel.DocumentModelBuilder;
import de.unihildesheim.lucene.document.model.DocumentModelException;
import de.unihildesheim.util.StringUtils;
import de.unihildesheim.lucene.scoring.clarity.ClarityScoreConfiguration;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.lucene.util.BytesWrapUtil;
import de.unihildesheim.util.ConcurrentMapTools;
import de.unihildesheim.util.TimeMeasure;
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
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Fun.Tuple4;
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
          = ClarityScoreConfiguration.INSTANCE.get(CONF_PREFIX
                  + "fieldNameSep", "\\|");

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
   * Document-term data. Store <tt>prefix -> key -> document-id -> term :
   * value</tt>. Prefix is null for internal data.
   */
  private BTreeMap<Fun.Tuple4<
          String, String, Integer, BytesWrap>, Object> docTermData;

  /**
   * Summed frequency of all terms in the index.
   */
  private transient Long overallTermFreq = null;

  /**
   * Prefix to use for internal data stored to {@link #docTermData}.
   */
  private static final String INTERNAL_PREFIX
          = ClarityScoreConfiguration.INSTANCE.get(CONF_PREFIX
                  + "internalPrefix", "_");

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
     * Properties: Fields indexed by this instance.
     */
    propFields,
    /**
     * Properties: Cache data last access date.
     */
    propTimestamp,
    /**
     * Properties: Boolean flag, if document-model, term-frequency data is
     * available.
     */
    propDocumentModelTermFreq,
    /**
     * Properties: Boolean flag, if document-model data is available.
     */
    propDocumentModel,
    /**
     * Properties: Boolean flag, if document-model, term-data data is
     * available.
     */
    propDocumentModelTermData,
    /**
     * Properties: Boolean flag, if index term-frequency data is available.
     */
    propIndexTermFreq,
    /**
     * Data backend: Name of document model, term frequency storage.
     */
    dbDocumentModelTermFreq,
    /**
     * Data backend: Name of document model storage.
     */
    dbDocumentModel,
    /**
     * Data backend: Name of document, term-data storage.
     */
    dbDocumentModelTermData,
    /**
     * Data backend: Name of index, term-frequency storage.
     */
    dbIndexTermFreq,
    /**
     * Document-Data: Term frequency.
     */
    _TF
  }

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

    this.storagePath = ClarityScoreConfiguration.INSTANCE.get(CONF_PREFIX
            + "storagePath", "data/cache/");
    LOG.info("Created IndexDataProvider::{} instance storage={}.", this.
            getClass().getCanonicalName(), this.storagePath);
    this.storageId = newStorageId;

    // create the manager for disk storage
    try {
      final DBMaker dbMkr = DBMaker.newFileDB(new File(this.storagePath,
              this.storageId));
//      dbMkr.transactionDisable(); // wo do not need transactions
      dbMkr.mmapFileEnableIfSupported(); // use mmaped files, if supported
//      dbMkr.asyncWriteFlushDelay(100); // reduce record fragmentation
      dbMkr.fullChunkAllocationEnable(); // allocate space in 1GB steps
//      dbMkr.cacheLRUEnable(); // enable last-recent-used cache
//      dbMkr.cacheHardRefEnable(); // use hard reference map
//      dbMkr.closeOnJvmShutdown(); // auto close db on exit
      this.db = dbMkr.make();
    } catch (RuntimeException ex) {
      LOG.error("Caught runtime exception. Maybe your classes have changed."
              + "You may want to delete the cache, because it's invalid now.");
    }

    // create a shutdown hooking thread to handle db closing and rollbacks
    this.shutdowHandler = new Thread(new Runnable() {
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
              DataKeys.propFields.name());
      if (targetFields == null) {
        // no fields specified
        this.setFields(new String[0]);
      } else {
        this.setFields(targetFields.split(FIELD_NAME_SEP));
      }

      hasProp = true;
    } catch (FileNotFoundException ex) {
      LOG.debug("Cache meta file " + propFile + " not found.", ex);
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
            DataKeys.propDocumentModel.name(), "false"));
    return state && !this.docModels.isEmpty();
  }

  /**
   * Checks, if document -> term-data is available.
   *
   * @return True, if data is available
   */
  private boolean hasDocTermData() {
    final boolean state = Boolean.parseBoolean(this.storageProp.getProperty(
            DataKeys.propDocumentModelTermData.name(), "false"));
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
            DataKeys.propIndexTermFreq.name(), "false"));
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
                  this.termFreqMap.size(), timeMeasure.getElapsedTimeString());
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("Recalc needed: hasDocModels({}) hasDocTermData({}) "
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
    DB.HTreeMapMaker dmBase = this.db.createHashMap(DataKeys.dbDocumentModel.
            name());
    dmBase.keySerializer(Serializer.INTEGER);
    dmBase.valueSerializer(Serializer.LONG);
    dmBase.counterEnable();

    DB.BTreeMapMaker dmTD = this.db.createTreeMap(
            DataKeys.dbDocumentModelTermData.name());
    // comparators set to null to use default values
    final BTreeKeySerializer dmTDKeySerializer
            = new BTreeKeySerializer.Tuple4KeySerializer(null, null, null,
                    Serializer.STRING, Serializer.STRING, Serializer.INTEGER,
                    new BytesWrap.Serializer());
    dmTD.keySerializer(dmTDKeySerializer); // BTreeKeySerializer.TUPLE4
    dmTD.valueSerializer(Serializer.JAVA);
    dmTD.valuesOutsideNodesEnable();
    dmTD.counterEnable();

    final DB.HTreeMapMaker tfmMkr = this.db.createHashMap(
            DataKeys.dbIndexTermFreq.name());
    tfmMkr.keySerializer(new BytesWrap.Serializer());

    if (clear) {
      LOG.debug("Clearing all stored data.");
      this.docModels = dmBase.make();
      this.docTermData = dmTD.make();
      this.termFreqMap = tfmMkr.make();
    } else {
      this.docModels = dmBase.makeOrGet();
      if (!this.docModels.isEmpty()) {
        LOG.info("DocumentModels loaded. ({} entries)", this.docModels.size());
      }
      this.docTermData = dmTD.makeOrGet();
      if (!this.docTermData.isEmpty()) {
        LOG.info("DocumentModel TermData loaded. ({} entries)",
                this.docTermData.size());
      }
      this.termFreqMap = tfmMkr.makeOrGet();
      if (!this.termFreqMap.isEmpty()) {
        LOG.info("Term frequency map loaded. ({} entries)", this.termFreqMap.
                size());
      }
    }
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
   * @throws de.unihildesheim.lucene.document.model.DocumentModelException
   * Thrown, if the {@link DocumentModel} of the requested type could not be
   * instantiated
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
      throw new IllegalArgumentException("Empty list of target fields given.");
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
    this.storageProp.setProperty(DataKeys.propFields.name(), StringUtils.join(
            targetFields, FIELD_NAME_SEP));
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
      LOG.debug("Recalculation of all cached data triggered.");
      initStorage(true); // clear all data
      this.storageProp.setProperty(DataKeys.propIndexTermFreq.name(), "false");
      this.storageProp.setProperty(DataKeys.propDocumentModel.name(), "false");
      this.storageProp.setProperty(DataKeys.propDocumentModelTermData.name(),
              "false");
      this.storageProp.setProperty(DataKeys.propDocumentModelTermFreq.name(),
              "false");
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
      this.storageProp.setProperty(DataKeys.propIndexTermFreq.name(), "true");
      saveMetadata();
    }

//    LOG.error("--STOP--HERE-- {}");
//    System.exit(0);
    // 3. Create document models
    if (recalcAll || !hasDocModels() || !hasDocTermData()) {
      // calculate
      strictTransactionHookRequest();
      createDocumentModels(indexReader);
      transactionHookRelease();
      // save results
      commitHook();
      // save state
      this.storageProp.setProperty(DataKeys.propDocumentModel.name(), "true");
      this.storageProp.setProperty(DataKeys.propDocumentModelTermData.name(),
              "true");
      this.storageProp.setProperty(DataKeys.propDocumentModelTermFreq.name(),
              "true");
      saveMetadata();
    }

//    LOG.error("--STOP--HERE--");
//    System.exit(0);
    // update meta data
    saveMetadata();
  }

  /**
   * Save meta information for stored data.
   *
   * @throws IOException If there where any low-level I/O errors
   */
  private void saveMetadata() throws IOException {
    this.storageProp.setProperty(DataKeys.propTimestamp.name(),
            new SimpleDateFormat("MM/dd/yyyy h:mm:ss a").format(new Date()));
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

  @Override
  public void dispose() {
    // debug
    if (LOG.isTraceEnabled()) {
      for (Entry<BytesWrap, Fun.Tuple2<Long, Double>> data : this.termFreqMap.
              entrySet()) {
        LOG.trace("store: t={} f={} rf={}", data.getKey(), data.getValue().a,
                data.getValue().b);
      }
    }

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
    LOG.info("Updating database..");
    this.db.commit();
    LOG.info("Database updated. (took {})", tm.getElapsedTimeString());
  }

  @Override
  public long getTermsCount() {
    return this.termFreqMap.size();
  }

  @Override
  public boolean addDocumentModel(final DocumentModel docModel) {
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
      this.docTermData.putIfAbsent(Fun.
              t4(INTERNAL_PREFIX, DataKeys._TF.name(), docModel.id,
                      entry.getKey()), entry.getValue());
    }
    return true;
  }

  @Override
  public void updateDocumentModel(final DocumentModel docModel) {
    if (docModel == null) {
      throw new IllegalArgumentException("Model was null.");
    }
    ConcurrentMapTools.ensurePut(this.docModels, docModel.id,
            docModel.termFrequency);

    for (Entry<BytesWrap, Long> entry : docModel.termFreqMap.entrySet()) {
      // TODO: remove, this is debug code!
      if (entry.getKey() == null || entry.getValue() == null) {
        throw new NullPointerException("Encountered null value in "
                + "termFreqMap.");
      }
      // end debug code
      ConcurrentMapTools.ensurePut(this.docTermData, Fun.t4(INTERNAL_PREFIX,
              DataKeys._TF.name(), docModel.id, entry.getKey()), entry.
              getValue());
    }
  }

  @Override
  public boolean hasDocumentModel(final Integer docId) {
    if (docId == null) {
      return false;
    }
    return this.docModels.containsKey(docId);
  }

  @Override
  public long getTermFrequency() {
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
    double value;

    if (freq == null) {
      // term not found
      value = 0d;
    } else {
      value = freq.b;
    }

    return value;
  }

  @Override
  public Iterator<BytesWrap> getTermsIterator() {
    return Collections.unmodifiableSet(this.termFreqMap.keySet()).iterator();
  }

  @Override
  public long getDocModelCount() {
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
      LOG.error("Catched exception while updating term frequency value.", ex);
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
  // FIXME: optimize with bind keys
  public DocumentModel getDocumentModel(final int docId) {
    if (this.hasDocumentModel(docId)) {
      final DocumentModelBuilder dmBuilder = new DocumentModelBuilder(docId);
      dmBuilder.setTermFrequency(this.docModels.get(docId));

      // add term frequency values
      Iterator<BytesWrap> tfIt = Fun.
              filter(this.docTermData.navigableKeySet(),
                      INTERNAL_PREFIX, DataKeys._TF.name(), docId).iterator();

      try {
        while (tfIt.hasNext()) {
          try {
            final BytesWrap term = tfIt.next();
            final Tuple4 t4 = Fun.
                    t4(INTERNAL_PREFIX, DataKeys._TF.name(), docId,
                            term);
            final Object data = this.docTermData.get(t4);
            if (data == null) {
              LOG.error("Encountered null while adding term frequency values "
                      + "to document model. prefix={} key={} docId={} "
                      + "term={} termStr={}", INTERNAL_PREFIX, DataKeys._TF.
                      name(),
                      docId, term, BytesWrapUtil.bytesWrapToString(term));
            } else {
              dmBuilder.setTermFrequency(term, ((Number) data).
                      longValue());
            }
          } catch (Exception ex) {
            LOG.error("Caught exception while setting term frequency value. "
                    + "prefix={} key={} docId={}", INTERNAL_PREFIX,
                    DataKeys._TF.name(), docId, ex);
          }
        }
      } catch (Exception ex) {
        LOG.error("Caught exception while setting term frequency value. "
                + "docId={}", docId, ex);
      }
      return dmBuilder.getModel();
    }
    return null;
  }

  @Override
  public Iterator<Integer> getDocIdIterator() {
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
      throw new IllegalArgumentException("The sequence '" + INTERNAL_PREFIX
              + "' is not allowed as prefix, as it's reserved "
              + "for internal purpose.");
    }
    return ConcurrentMapTools.ensurePut(this.docTermData, Fun.t4(prefix, key,
            documentId, term), value);
  }

  /**
   * Returns the internal term-data-map. Meant only for debugging purpose as
   * changing data may cause inconsistency and break things.
   *
   * @return Internal term-data-map
   */
  @SuppressWarnings("ReturnOfCollectionOrArrayField")
  public BTreeMap<Fun.Tuple4<String, String, Integer, BytesWrap>, Object>
          debugGetTermDataMap() {
    return this.docTermData;
  }

  @Override
  public Object getTermData(final String prefix,
          final int documentId,
          final BytesWrap term,
          final String key
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
    return this.docTermData.get(Fun.t4(prefix, key, documentId, term));
  }

  @Override
  public boolean transactionHookRequest() {
    if (this.rollback) {
      return false;
    }
    this.rollback = true;
    LOG.info("Transaction hook set.");
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
    LOG.info("Transaction hook released.");
    this.rollback = false;
  }

  @Override
  public void transactionHookRoolback() {
    transactionHookRelease();
    LOG.info("Rolling back changes.");
    this.db.rollback();
  }
}
