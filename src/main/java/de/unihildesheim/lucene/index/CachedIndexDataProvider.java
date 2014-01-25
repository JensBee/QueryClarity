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

import de.unihildesheim.util.StringUtils;
import de.unihildesheim.lucene.document.DefaultDocumentModel;
import de.unihildesheim.lucene.document.DefaultDocumentModelSerializer;
import de.unihildesheim.lucene.document.DocumentModel;
import de.unihildesheim.lucene.document.DocumentModelException;
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
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.util.BytesRef;
import org.mapdb.DB;
import org.mapdb.DB.HTreeMapMaker;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation of the {@link IndexDataProvider} stores it's data with
 * disk-backed {@link Map} implementations to cache calculated values. This
 * allows to store a huge amount of data exceeding memory limits, with a bit of
 * speed tradeoff to load cached values from disk.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class CachedIndexDataProvider extends AbstractIndexDataProvider {

  /**
   * Separator to store field names.
   */
  private static final String FIELD_NAME_SEP = "|";

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
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
          CachedIndexDataProvider.class);

  /**
   * Creates a new disk backed (cached) {@link IndexDataProvider} with the given
   * storage path.
   *
   * @param newStorageId Unique identifier for this cache
   * @param newStoragePath Path where the cached data are/should be stored
   * @throws IOException Thrown on low-level I/O errors
   */
  public CachedIndexDataProvider(final String newStoragePath,
          final String newStorageId) throws IOException {
    LOG.info("Created IndexDataProvider::{} instance storage={}.", this.
            getClass().getCanonicalName(), newStoragePath);
    this.storagePath = newStoragePath;
    this.storageId = newStorageId;
    // create the manager for disk storage
    try {
      final DBMaker dbMkr = DBMaker.newFileDB(new File(this.storagePath,
              this.storageId));
      dbMkr.randomAccessFileEnableIfNeeded(); // support 32bit JVMs
//      if (useLRUCache) {
      dbMkr.cacheLRUEnable(); // enable last-recent-used cache
//      }
//      if (useHardRefs) {
      dbMkr.cacheHardRefEnable(); // use hard reference map
//      }
//      dbMkr.closeOnJvmShutdown(); // auto close db on exit
      this.db = dbMkr.make();
    } catch (RuntimeException ex) {
      LOG.error("Caught runtime exception. Maybe your classes have changed."
              + "You may want to delete the cache, because it's invalid now.");
    }

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

      final String targetFields = this.storageProp.getProperty("fields");
      if (targetFields == null) {
        // no fields specified
        this.setFields(new String[0]);
      } else {
        this.setFields(targetFields.split(FIELD_NAME_SEP));
      }

      hasProp = true;
    } catch (FileNotFoundException ex) {
      LOG.debug("Cache meta file " + propFile + " not found.", ex);
      this.setFields(new String[0]);
      hasProp = false;
    }
    return hasProp;
  }

  /**
   * Tries to read the cached data and recalculates the data if desired.
   *
   * @return True, if all data could be read, false if recalculation is needed
   * and automatic recalculation was not enabled
   * @throws IOException Thrown, on low-level errors while accessing the cached
   * data
   */
  public boolean tryGetStoredData() throws IOException {
    LOG.info("Trying to get disk storage ({})", this.storagePath);

    boolean needsRecalc;

    // try load cached data
    final TimeMeasure timeMeasure = new TimeMeasure().start();

    final HTreeMapMaker dmmMkr = this.db.createHashMap("docModels");
    // map-key serializer
    dmmMkr.keySerializer(Serializer.INTEGER);
    // map-value serializer
    final Serializer ddmSerializer = new DefaultDocumentModelSerializer();
    dmmMkr.valueSerializer(ddmSerializer);
    // create map
    this.docModelMap = dmmMkr.makeOrGet();

    final HTreeMapMaker tfmMkr = this.db.createHashMap("termFreq");
    // map-key serializer
    tfmMkr.keySerializer(Serializer.BYTE_ARRAY);
    // map-value serializer
    final Serializer tfdSerializer = new TermFreqDataSerializer();
    tfmMkr.valueSerializer(tfdSerializer);
    // create map
    this.termFreqMap = tfmMkr.makeOrGet();
    timeMeasure.stop();

    // check if storage meta information is there and fields are defined
    if (getStorageInfo()) {
      if (this.getFields().length == 0) {
        LOG.info("No chached field information specified in meta information. "
                + "Need to recalculate values.");
        needsRecalc = true;
      } else {
        // check if data was loaded
        needsRecalc = this.docModelMap.isEmpty() || this.termFreqMap.isEmpty();
        if (!needsRecalc) {
          LOG.info("Loading cache (docModels={} termFreq={}) "
                  + "took {} seconds.", this.docModelMap.size(),
                  this.termFreqMap.size(), timeMeasure.getElapsedSeconds());
        }
        // debug
        if (LOG.isTraceEnabled()) {
          for (Entry<byte[], TermFreqData> data : this.termFreqMap.entrySet()) {
            LOG.trace("load: t={} f={} rf={}", data.getKey(), data.getValue().
                    getTotalFreq(), data.getValue().getRelFreq());
          }
        }
      }
    } else {
      LOG.info("No cache meta information found. Need to recalculate values.");
      needsRecalc = true;
    }

    return !needsRecalc;
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
   * @throws de.unihildesheim.lucene.document.DocumentModelException Thrown, if
   * the {@link DocumentModel} of the requested type could not be instantiated
   */
  public void recalculateData(final IndexReader indexReader,
          final String[] targetFields, final boolean all) throws IOException,
          DocumentModelException {
    // check parameter sanity
    if (targetFields.length == 0) {
      throw new IllegalArgumentException("Empty list of target fields given.");
    }

    // check if fields have changed
    if (this.getFields().length == 0 || !Arrays.equals(this.getFields(),
            targetFields)) {
      this.setFields(targetFields.clone());
    }

    checkFields(indexReader);

    // clear any possible existing data
    clearData();

    // oder of calculation steps matters!
    calculateTermFrequencies(indexReader);
    calculateRelativeTermFrequencies();
    createDocumentModels(DefaultDocumentModel.class, indexReader);

    // debug
    if (LOG.isTraceEnabled()) {
      for (Entry<byte[], TermFreqData> data : this.termFreqMap.entrySet()) {
        LOG.trace("new: t={} f={} rf={}", data.getKey(), data.getValue().
                getTotalFreq(), data.getValue().getRelFreq());
      }
    }

    // write cached data
    this.db.commit();

    // update meta data
    this.storageProp.setProperty("fields", StringUtils.join(targetFields,
            FIELD_NAME_SEP));
    this.storageProp.setProperty("timestamp", new SimpleDateFormat(
            "MM/dd/yyyy h:mm:ss a").format(new Date()));
    saveMetadata();
  }

  /**
   * Save meta information for stored data.
   *
   * @throws IOException If there where any low-level I/O errors
   */
  private void saveMetadata() throws IOException {
    final File propFile = new File(this.storagePath, this.storageId
            + ".properties");
    try (FileOutputStream propFileOut = new FileOutputStream(propFile)) {
      this.storageProp.store(propFileOut, "");
    }
  }

  @Override
  public void dispose() {
    // debug
    if (LOG.isTraceEnabled()) {
      for (Entry<byte[], TermFreqData> data : this.termFreqMap.entrySet()) {
        LOG.trace("store: t={} f={} rf={}", data.getKey(), data.getValue().
                getTotalFreq(), data.getValue().getRelFreq());
      }
    }

    try {
      // update meta-data
      saveMetadata();
    } catch (IOException ex) {
      LOG.error("Error while storing meta informations.", ex);
    }

    // commit changes & close storage
    this.db.commit();
    this.db.close();
  }

  @Override
  public void setProperty(final String prefix, final String key,
          final String value) {
    this.storageProp.setProperty(prefix + '_' + key, value);
  }

  @Override
  public String getProperty(final String prefix, final String key) {
    return this.storageProp.getProperty(prefix + "_" + key);
  }

  @Override
  public String getProperty(final String prefix, final String key,
          final String defaultValue) {
    return this.storageProp.getProperty(prefix + "_" + key, defaultValue);
  }

  @Override
  protected final void updateTermFreqValue(final byte[] term,
          final long value) {

    if (this.termFreqMap.containsKey(term)) {
      final TermFreqData tfData = this.termFreqMap.get(term);
      tfData.addToTotalFreq(value);
      if (((HTreeMap) this.termFreqMap).replace(term, tfData) == null) {
        // previous value should never be null - this smells like an error
        throw new IllegalStateException("Got null while updating "
                + "term frequency value for term '" + term + "'.");
      }
    } else {
      final TermFreqData tfData = new TermFreqData(value);
      this.termFreqMap.put(term.clone(), tfData);
    }
    this.setOverallTermFreq(null); // force recalculation
  }

  @Override
  protected final void updateTermFreqValue(final byte[] term,
          final double value) {

    if (this.termFreqMap.containsKey(term)) {
      final TermFreqData tfData = this.termFreqMap.get(term);
      tfData.setRelFreq(value);
      if (((HTreeMap) this.termFreqMap).replace(term, tfData) == null) {
        // previous value should never be null - this smells like an error
        throw new IllegalStateException("Got null while updating "
                + "relative term frequency value for term '" + term + "'.");
      }
    } else {
      final TermFreqData tfData = new TermFreqData(value);
      this.termFreqMap.put(term.clone(), tfData);
    }
  }
}
