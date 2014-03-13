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
import de.unihildesheim.util.StringUtils;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class Environment {

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
  private static IndexReader indexReader;

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
   * Listeners looking for changes to document fields.
   */
  private static Collection<FieldsChangedListener> fieldsChangedListeners;

  /**
   * Initial number of listeners expected.
   */
  private static final int DEFAULT_LISTENERS_SIZE = 5;

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

  private enum PropertiesConf {

    FILENAME("environment.properties"),
    KEY_TIMESTAMP("timestamp"),
    TIMESTAMP_FORMAT("MM/dd/yyyy h:mm:ss a");

    private final String data;

    PropertiesConf(final String newData) {
      this.data = newData;
    }

    @Override
    public String toString() {
      return this.data;
    }
  }

  /**
   * Initialize a default environment.
   */
  public Environment(final String newIndexPath, final String newDataPath)
          throws IOException {
    this(newIndexPath, newDataPath, null);
  }

  public Environment(final String newIndexPath, final String newDataPath,
          final String[] fields)
          throws IOException {
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
      final Collection<String> idxFieldNames = new HashSet(idxFields.size());
      while (idxFieldNamesIt.hasNext()) {
        idxFieldNames.add(idxFieldNamesIt.next());
      }
      Environment.fields = idxFieldNames.toArray(new String[idxFieldNames.
              size()]);
    } else {
      Environment.fields = fields;
      checkFields(fields);
    }

    loadProperties();

    try {
      Runtime.getRuntime().addShutdownHook(Environment.EXIT_HANDLER);
    } catch (IllegalArgumentException ex) {
      // already registered, or shutdown is currently happening
    }

    fieldsChangedListeners = new HashSet<>(DEFAULT_LISTENERS_SIZE);
  }

  public static boolean addFieldsChangedListener(
          final FieldsChangedListener listener) {
    return Environment.fieldsChangedListeners.add(listener);
  }

  public static boolean removeFieldsChangedListener(
          final FieldsChangedListener listener) {
    return Environment.fieldsChangedListeners.remove(listener);
  }

  public static void setFields(final String[] newFields) {
    checkFields(newFields);
    final String[] oldFields = Environment.fields.clone();
    Environment.fields = newFields.clone();
    for (FieldsChangedListener listener : Environment.fieldsChangedListeners) {
      listener.fieldsChanged(oldFields, newFields);
    }
  }

  /**
   * Check if all given fields are available in the current index.
   *
   * @param indexReader Reader to access the index
   * @param fields Fields to check
   */
  private static final void checkFields(final String[] fields) {
    if (fields == null || fields.length == 0) {
      throw new IllegalArgumentException("No fields specified.");
    }

    // get all indexed fields from index - other fields are not of interes here
    final Collection<String> indexedFields = MultiFields.getIndexedFields(
            Environment.indexReader);

    // check if all requested fields are available
    if (!indexedFields.containsAll(Arrays.asList(fields))) {
      throw new IllegalStateException(MessageFormat.format(
              "Not all requested fields ({0}) "
              + "are available in the current index ({1}) or are not indexed.",
              StringUtils.join(fields, ","), Arrays.toString(indexedFields.
                      toArray(new String[indexedFields.size()]))));
    }
  }

  /**
   * Try to read the properties file.
   *
   * @return True, if the file is there, false otherwise
   * @throws IOException Thrown on low-level I/O errors
   */
  private final boolean loadProperties() throws IOException {
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
    return hasProp;
  }

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
   * Tries to open a Lucene IndexReader
   *
   * @param indexDir Directory of the Lucene index
   * @return Reader for accessing the Lucene index
   * @throws IOException Thrown on low-level I/O-errors
   */
  private IndexReader openReader(final File indexDir) throws IOException {
    final Directory directory = FSDirectory.open(indexDir);
    return DirectoryReader.open(directory);
  }

  /**
   * Create a new instance with a default dataProvider implementation
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
   * Create a new instance with the given dataProvider
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
   * Get the configured environment.
   *
   * @return Global environment instance
   */
  public static Environment get() {
    initialized();
    return instance;
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
    if (instance == null) {
      return false;
    }
    return true;
  }

  /**
   * Get the fields passed to the {@link IndexDataProvider}.
   *
   * @return List of fields passed to the IndexDataProvider
   */
  public static String[] getFields() {
    initialized();
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
    initialized();
    return Environment.indexReader;
  }

  /**
   * Get the directory location of the Lucene index.
   *
   * @return directory location of the Lucene index
   */
  public static String getIndexPath() {
    initialized();
    return Environment.indexPath;
  }

  /**
   * Get the directory location of the data directory.
   *
   * @return directory location for storing extended data
   */
  public static String getDataPath() {
    initialized();
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
    fieldsChangedListeners.clear();
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
   * Stores a property value to the {@link IndexDataProvider}. Depending on
   * the implementation this property may be persistent.
   *
   * @param prefix Prefix to identify the property store
   * @param key Key to assign a property to
   * @param value Property value
   */
  public static void setProperty(final String prefix, final String key,
          final String value) {
    initialized();
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
   * Retrieve a previously stored property from the {@link IndexDataProvider}.
   * Depending on the implementation stored property values may be persistent
   * between instantiations.
   *
   * @param prefix Prefix to identify the property store
   * @param key Key under which the property was stored
   * @return The stored property vale or null, if none was found
   */
  public static String getProperty(final String prefix, final String key) {
    initialized();
    if (prefix == null || prefix.isEmpty()) {
      throw new IllegalArgumentException("No prefix specified.");
    }
    if (key == null || key.isEmpty()) {
      throw new IllegalArgumentException("Key may not be null or empty.");
    }
    return Environment.PROP_STORE.getProperty(prefix + "_" + key);
  }

  /**
   * Remove all externally stored properties.
   *
   * @param prefix Prefix to identify the property store to delete
   */
  public static void clearProperties(final String prefix) {
    initialized();
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
    initialized();
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
    initialized();
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

    void fieldsChanged(final String[] oldFields, final String[] newFields);
  }
}
