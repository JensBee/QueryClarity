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
package de.unihildesheim.lucene;

import de.unihildesheim.lucene.index.DirectIndexDataProvider;
import de.unihildesheim.lucene.index.IndexDataProvider;
import de.unihildesheim.lucene.index.IndexUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Global environment for calculations.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class Environment {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(Environment.class);

  /**
   * DataProvider used, to access the index.
   */
  private static IndexDataProvider dataProvider = null;

  /**
   * Path where the Lucene index is located at.
   */
  private static String indexPath;
  /**
   * Reader to access the Lucene index.
   */
  private static IndexReader indexReader = null;

  /**
   * Path where additional data should be stored.
   */
  private static String dataPath;

  /**
   * Flag indicating, if the environment is initialized.
   */
  private static Environment instance = null;

  /**
   * Lucene index fields to use by DataProviders.
   */
  private static String[] fields;

  /**
   * Properties persistent stored.
   */
  private static final Properties PROP_STORE = new Properties();

  /**
   * Initial number of listeners expected.
   */
  private static final int DEFAULT_LISTENERS_SIZE = 5;

  /**
   * Last commit generation of the index. Used to validate the cached data.
   */
  private static long indexGeneration;

  /**
   * Listeners looking for changes to document fields. Directly initialized to
   * allow adding of listeners prior to environment initialization.
   */
  private static Collection<FieldsChangedListener> fieldsChangedListeners
          = new HashSet<>(DEFAULT_LISTENERS_SIZE);

  /**
   * Listeners looking for changes to the stop-words list. Directly
   * initialized to allow adding of listeners prior to environment
   * initialization.
   */
  private static Collection<StopwordsChangedListener> stopwordsChangedListeners
          = new HashSet<>(DEFAULT_LISTENERS_SIZE);

  /**
   * Default initial size of the stop-words list.
   */
  private static final int DEFAULT_STOPWORDS_SIZE = 200;

  /**
   * List of stop-words to use.
   */
  private static Collection<String> stopWords = new HashSet<>(
          DEFAULT_STOPWORDS_SIZE);

  /**
   * Flag indicating, if properties were loaded.
   */
  private static boolean propLoaded = false;

  /**
   * Shutdown thread saving properties back to disk.
   */
  private static final Thread EXIT_HANDLER = new Thread(new Runnable() {
    @Override
    public void run() {
      try {
        saveProperties();
      } catch (IOException ex) {
        LOG.error("Shutdown handler: Error while saving properties.", ex);
      }
    }
  }, "Environment_shutdownHandler");

  /**
   * Basic configuration keys for application data.
   */
  private enum PropertiesConf {

    /**
     * Name of the configuration file.
     */
    FILENAME("environment.properties"),
    /**
     * Key to store a last modified timestamp.
     */
    KEY_TIMESTAMP("timestamp"),
    /**
     * Format of the last modified timestamp.
     */
    TIMESTAMP_FORMAT("MM/dd/yyyy h:mm:ss a");

    /**
     * String data of the current key.
     */
    private final String data;

    /**
     * Initialize the Enum.
     *
     * @param newData Data for a key
     */
    PropertiesConf(final String newData) {
      this.data = newData;
    }

    @Override
    public String toString() {
      return this.data;
    }
  }

  /**
   * Initialize a default environment. All document fields in the index will
   * be used for searching. Any fields set prior to calling the constructor
   * get overwritten.
   *
   * @param newIndexPath Path where the Lucene index resides
   * @param newDataPath Path where application data is stored
   * @throws java.io.IOException Thrown on low-level I/O errors
   */
  public Environment(final String newIndexPath, final String newDataPath)
          throws IOException {
    this(newIndexPath, newDataPath, null);
  }

  /**
   * Initialize the environment. Only the passed list of document fields will
   * be used for searching. Any fields set prior to calling the constructor
   * get overwritten.
   *
   * @param newIndexPath Path where the Lucene index resides
   * @param newDataPath Path where application data is stored
   * @param fields Document fields to use for searching
   * @throws IOException Thrown on low-level I/O errors
   */
  public Environment(final String newIndexPath, final String newDataPath,
          final String[] fields) throws IOException {
    if (instance != null) {
      throw new IllegalStateException("Environment already initialized.");
    }

    Environment.dataPath = newDataPath;
    Environment.indexPath = newIndexPath;

    // check, if given pathes are valid
    checkPathes();

    Environment.indexReader = openReader(new File(Environment.indexPath));

    if (fields == null) {
      // use all index fields
      final Fields idxFields = MultiFields.getFields(Environment.indexReader);
      final Iterator<String> idxFieldNamesIt = idxFields.iterator();
      final Collection<String> idxFieldNames = new HashSet<>(idxFields.size());
      while (idxFieldNamesIt.hasNext()) {
        idxFieldNames.add(idxFieldNamesIt.next());
      }
      Environment.fields = idxFieldNames.toArray(new String[idxFieldNames.
              size()]);
    } else {
      Environment.fields = fields;
      IndexUtils.checkFields(fields);
    }

    loadProperties();

    try {
      Runtime.getRuntime().addShutdownHook(Environment.EXIT_HANDLER);
    } catch (IllegalArgumentException ex) {
      // already registered, or shutdown is currently happening
    }
  }

  /**
   * Get the generation number of the index. This is the generation number of
   * the last commit to the index.
   *
   * @return Generation number of the index
   */
  public static Long getIndexGeneration() {
    if (Environment.indexReader == null) {
      throw new IllegalStateException(
              "Environment not initialized. (indexReader)");
    }
    return Environment.indexGeneration;
  }

  /**
   * Add a listener to get notified if document fields are changed.
   *
   * @param listener Listener to add
   * @return True, if the listener was added
   */
  public static boolean addStopwordsChangedListener(
          final StopwordsChangedListener listener) {
    return Environment.stopwordsChangedListeners.add(listener);
  }

  /**
   * Remove a listener to changes of document fields.
   *
   * @param listener Listener to remove
   * @return True, if the listener was removed
   */
  public static boolean removeStopwordsChangedListener(
          final StopwordsChangedListener listener) {
    return Environment.stopwordsChangedListeners.remove(listener);
  }

  /**
   * Add a listener to get notified if document fields are changed.
   *
   * @param listener Listener to add
   * @return True, if the listener was added
   */
  public static boolean addFieldsChangedListener(
          final FieldsChangedListener listener) {
    return Environment.fieldsChangedListeners.add(listener);
  }

  /**
   * Remove a listener to changes of document fields.
   *
   * @param listener Listener to remove
   * @return True, if the listener was removed
   */
  public static boolean removeFieldsChangedListener(
          final FieldsChangedListener listener) {
    return Environment.fieldsChangedListeners.remove(listener);
  }

  /**
   * Set the index document fields that get searched
   *
   * @param newFields List of fields to use for searching
   */
  public static void setFields(final String[] newFields) {
    IndexUtils.checkFields(newFields);
    final String[] oldFields = Environment.fields.clone();
    Environment.fields = newFields.clone();
    LOG.info("Index fields changed: {}", Environment.fields);
    for (FieldsChangedListener listener : Environment.fieldsChangedListeners) {
      listener.fieldsChanged(oldFields);
    }
  }

  /**
   * Set the list of stop-words to use.
   *
   * @param words List of stop-words
   */
  public static void setStopwords(final Collection<String> words) {
    final Collection<String> oldWords = new ArrayList<>(Environment.stopWords);
    Environment.stopWords.addAll(words);
    LOG.info("Stop-words changed: {}", Environment.stopWords);
    for (StopwordsChangedListener listener
            : Environment.stopwordsChangedListeners) {
      listener.wordsChanged(oldWords);
    }
  }

  /**
   * Get the list of stop-words.
   *
   * @return List of stop-words
   */
  public static Collection<String> getStopwords() {
    return Collections.unmodifiableCollection(Environment.stopWords);
  }

  /**
   * Try to read the properties file.
   *
   * @return True, if the file is there, false otherwise
   * @throws IOException Thrown on low-level I/O errors
   */
  private boolean loadProperties() throws IOException {
    boolean hasProp;

    final File propFile = new File(Environment.dataPath,
            PropertiesConf.FILENAME.toString());
    try {
      try (FileInputStream propStream = new FileInputStream(propFile)) {
        Environment.PROP_STORE.load(propStream);
      }

      hasProp = true;
    } catch (FileNotFoundException ex) {
      LOG.trace("Cache meta file " + propFile + " not found.", ex);
      hasProp = false;
    }
    propLoaded = true;
    return hasProp;
  }

  /**
   * Check if the configured pathes are available. Tries to create some of
   * them, if it's not the case.
   *
   * @throws IOException Thrown if a path is not there or it cannot be created
   */
  private void checkPathes() throws IOException {
    if (Environment.dataPath == null || Environment.dataPath.isEmpty()) {
      throw new IllegalArgumentException("Data path was empty.");
    }
    if (Environment.indexPath == null || Environment.indexPath.isEmpty()) {
      throw new IllegalArgumentException("Index path was empty.");
    }

    final File dataDir = new File(Environment.dataPath);
    if (dataDir.exists()) {
      if (!dataDir.isDirectory()) {
        throw new IOException("Data path '" + Environment.dataPath
                + "' exists, but is not a directory.");
      }
    } else if (!dataDir.mkdirs()) {
      throw new IOException("Error while creating data path '"
              + Environment.dataPath + "'.");
    }
    LOG.info("Data Path: {}", Environment.dataPath);

    final File idxDir = new File(Environment.indexPath);
    if (idxDir.exists()) {
      if (!idxDir.isDirectory()) {
        throw new IOException("Index path '" + Environment.indexPath
                + "' exists, but is not a directory.");
      }
    } else if (!idxDir.mkdirs()) {
      throw new IOException("Error while creating data path '"
              + Environment.indexPath + "'.");
    }
    LOG.info("Index Path: {}", Environment.indexPath);
  }

  /**
   * Tries to open a Lucene IndexReader.
   *
   * @param indexDir Directory of the Lucene index
   * @return Reader for accessing the Lucene index
   * @throws IOException Thrown on low-level I/O-errors
   */
  private IndexReader openReader(final File indexDir) throws IOException {
    final Directory directory = FSDirectory.open(indexDir);
    if (!DirectoryReader.indexExists(directory)) {
      throw new IOException("No index found at '" + indexDir.getAbsolutePath()
              + "'.");
    }
    Environment.indexGeneration = SegmentInfos.getLastCommitGeneration(
            directory);
    return DirectoryReader.open(directory);
  }

  /**
   * Create a new instance with a default dataProvider implementation.
   *
   * @throws IOException Thrown on low-level I/O-errors
   */
  public void create() throws IOException {
    Environment.instance = this;
    Environment.dataProvider = new DirectIndexDataProvider();
    LOG.info("DataProvider: {}", Environment.dataProvider.getClass().
            getCanonicalName());
  }

  /**
   * Create a new instance with the given dataProvider.
   *
   * @param newDataProvider DataProvider to use
   */
  public void create(final IndexDataProvider newDataProvider) {
    if (newDataProvider == null) {
      throw new IllegalArgumentException("DataProvider was null.");
    }
    Environment.instance = this;
    Environment.dataProvider = newDataProvider;
    LOG.info("DataProvider: {}", Environment.dataProvider.getClass().
            getCanonicalName());
  }

  /**
   * Create a new instance of the given dataProvider class type.
   *
   * @param dataProviderClass DataProvider to instantiate
   * @throws InstantiationException Thrown if instance could not be created
   * @throws IllegalAccessException Thrown if instance could not be created
   */
  public void create(
          final Class<? extends IndexDataProvider> dataProviderClass) throws
          InstantiationException, IllegalAccessException {
    if (dataProviderClass == null) {
      throw new IllegalArgumentException("DataProvider was null.");
    }
    Environment.instance = this;
    Environment.dataProvider = dataProviderClass.newInstance();
    LOG.info("DataProvider: {}", Environment.dataProvider.getClass().
            getCanonicalName());
  }

  /**
   * Checks, if the environment is initialized. Throws a runtime exception, if
   * not.
   */
  private static void initialized() {
    if (instance == null) {
      throw new IllegalStateException("Environment not initialized");
    }
  }

  /**
   * Checks, if the environment is initialized.
   *
   * @return True, if initialized, false otherwise
   */
  public static boolean isInitialized() {
    return instance != null;
  }

  /**
   * Get the fields passed to the {@link IndexDataProvider}.
   *
   * @return List of fields passed to the IndexDataProvider
   */
  public static String[] getFields() {
    if (Environment.fields == null) {
      throw new IllegalStateException(
              "Environment not initialized. (fields)");
    }
    return Environment.fields.clone();
  }

  /**
   * Get the {@link IndexDataProvider}.
   *
   * @return IndexDataProvider used
   */
  public static IndexDataProvider getDataProvider() {
    initialized();
    return Environment.dataProvider;
  }

  /**
   * Get the reader used to access the Lucene index.
   *
   * @return Lucene index reader
   */
  public static IndexReader getIndexReader() {
    if (Environment.indexReader == null) {
      throw new IllegalStateException(
              "Environment not initialized. (indexReader)");
    }
    return Environment.indexReader;
  }

  /**
   * Get the directory location of the Lucene index.
   *
   * @return directory location of the Lucene index
   */
  public static String getIndexPath() {
    if (Environment.indexPath == null) {
      throw new IllegalStateException(
              "Environment not initialized. (indexPath)");
    }
    return Environment.indexPath;
  }

  /**
   * Get the directory location of the data directory.
   *
   * @return directory location for storing extended data
   */
  public static String getDataPath() {
    if (Environment.dataPath == null) {
      throw new IllegalStateException(
              "Environment not initialized. (dataPath)");
    }
    return Environment.dataPath;
  }

  /**
   * Reset the environment to an uninitialized state. Meant for testing
   * purposes. Use it with care.
   */
  public static void clear() {
    if (!isInitialized()) {
      return;
    }
    LOG.warn("Clearing Environment.");
    try {
      saveProperties();
    } catch (IOException ex) {
      LOG.error("Failed to save properties.", ex);
    }
    clearAllProperties();
    Environment.fieldsChangedListeners.clear();
    Environment.stopwordsChangedListeners.clear();
    Environment.dataPath = null;
    Environment.dataProvider = null;
    Environment.fields = null;
    Environment.indexPath = null;
    Environment.stopWords.clear();
    propLoaded = false;
    indexReader = null;
    instance = null;
  }

  /**
   * Save meta information for stored data.
   *
   * @throws IOException If there where any low-level I/O errors
   */
  public static void saveProperties() throws IOException {
    Environment.PROP_STORE.
            setProperty(PropertiesConf.KEY_TIMESTAMP.toString(),
                    new SimpleDateFormat(PropertiesConf.TIMESTAMP_FORMAT.
                            toString()).format(new Date()));
    final File propFile = new File(Environment.dataPath,
            PropertiesConf.FILENAME.toString());
    try (FileOutputStream propFileOut = new FileOutputStream(propFile)) {
      Environment.PROP_STORE.store(propFileOut, "");
    }
  }

  /**
   * Stores a property value.
   *
   * @param prefix Prefix to identify the property store
   * @param key Key to assign a property to
   * @param value Property value
   */
  public static void setProperty(final String prefix, final String key,
          final String value) {
    if (!propLoaded) {
      throw new IllegalStateException(
              "Environment not initialized. (properties)");
    }
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    if (value == null) {
      throw new IllegalArgumentException("Null is not allowed as value.");
    }
    Environment.PROP_STORE.setProperty(prefix + '_' + key, value);
  }

  /**
   * Removes a property.
   *
   * @param prefix Prefix to identify the property store
   * @param key Key to remove
   * @return Old value assigned with the key or <tt>null</tt> if there was
   * none
   */
  public static Object removeProperty(final String prefix, final String key) {
    if (!propLoaded) {
      throw new IllegalStateException(
              "Environment not initialized. (properties)");
    }
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    return Environment.PROP_STORE.remove(prefix + '_' + key);
  }

  /**
   * Retrieve a previously stored property from the {@link IndexDataProvider}.
   * Depending on the implementation stored property values may be persistent
   * between instantiations.
   *
   * @param prefix Prefix to identify the property store
   * @param key Key under which the property was stored
   * @return The stored property vale or null, if none was found
   */
  public static String getProperty(final String prefix, final String key) {
    if (!propLoaded) {
      throw new IllegalStateException(
              "Environment not initialized. (properties)");
    }
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    return Environment.PROP_STORE.getProperty(prefix + "_" + key);
  }

  /**
   * Get a mapping of all values stored with the given prefix.
   *
   * @param prefix Prefix to identify the property store
   * @return Map with all key value pairs matching the prefix
   */
  public static Map<String, Object> getProperties(final String prefix) {
    if (!propLoaded) {
      throw new IllegalStateException(
              "Environment not initialized. (properties)");
    }
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    @SuppressWarnings("CollectionWithoutInitialCapacity")
    final Map<String, Object> data = new HashMap();
    final int prefixLength = prefix.length() + 1;
    for (Entry<Object, Object> entry : Environment.PROP_STORE.entrySet()) {
      if (((String) entry.getKey()).startsWith(prefix)) {
        data.put(((String) entry.getKey()).substring(prefixLength), entry.
                getValue());
      }
    }
    return data;
  }

  /**
   * Remove all externally stored properties.
   *
   * @param prefix Prefix to identify the property store to delete
   */
  public static void clearProperties(final String prefix) {
    Iterator<Object> propKeys = Environment.PROP_STORE.keySet().iterator();
    while (propKeys.hasNext()) {
      if (((String) propKeys.next()).startsWith(prefix + "_")) {
        propKeys.remove();
      }
    }
  }

  /**
   * Removes all stored properties.
   */
  public static void clearAllProperties() {
    Iterator<Object> propKeys = Environment.PROP_STORE.keySet().iterator();
    while (propKeys.hasNext()) {
      propKeys.next();
      propKeys.remove();
    }
  }

  /**
   * Same as {@link IndexDataProvider#getProperty(String, String)}, but allows
   * to specify a default value.
   *
   * @param prefix Prefix to identify the property store
   * @param key Key under which the property was stored
   * @param defaultValue Default value to return, if the specified key was not
   * found
   * @return The stored property vale or <tt>defaultValue</tt>, if none was
   * found
   */
  public static String getProperty(final String prefix, final String key,
          final String defaultValue) {
    if (!propLoaded) {
      throw new IllegalStateException(
              "Environment not initialized. (properties)");
    }
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    return Environment.PROP_STORE.getProperty(prefix + "_" + key,
            defaultValue);
  }

  /**
   * Listener interface to listen to changes of document fields.
   */
  @SuppressWarnings("PublicInnerClass")
  public interface FieldsChangedListener {

    /**
     * Called, if the index document fields have changed.
     *
     * @param oldFields List of old fields that were used
     */
    void fieldsChanged(final String[] oldFields);
  }

  /**
   * Listener interface to listen to changes of the list of stop-words.
   */
  @SuppressWarnings("PublicInnerClass")
  public interface StopwordsChangedListener {

    /**
     * Called, if the index document fields have changed.
     *
     * @param oldWords List of old words that were used
     */
    void wordsChanged(final Collection<String> oldWords);
  }
}
