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
package de.unihildesheim.iw.lucene;

import de.unihildesheim.iw.lucene.index.DirectIndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.index.IndexUtils;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Global environment for calculations.
 *
 * @author Jens Bertram
 */
public final class Environment {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(Environment.class);
  /**
   * Properties persistent stored.
   */
  private static final Properties PROP_STORE = new Properties();
  /**
   * Default initial size of the stop-words list.
   */
  private static final int DEFAULT_STOPWORDS_SIZE = 200;
  /**
   * List of stop-words to use.
   */
  private static final Collection<String> STOPWORDS = new HashSet<>(
      DEFAULT_STOPWORDS_SIZE);
  /**
   * DataProvider used, to access the index.
   */
  private static IndexDataProvider dataProvider = null;
  /**
   * Path where the Lucene index is located at.
   */
  private static String indexPath = null;
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
  private static boolean initialized;
  /**
   * Lucene index fields to use by DataProviders.
   */
  private static String[] fields = null;
  /**
   * Last commit generation of the index. Used to validate the cached data.
   */
  private static long indexGeneration;
  /**
   * For testing only. If true, indication we run a test case.
   */
  private static boolean isTestRun = false;
  /**
   * Flag, indicating, if no index is used.
   */
  private static boolean noIndex = false;

  /**
   * Empty private constructor. Use the {@link Environment.Builder} to create.
   */
  private Environment() {
    // empty
  }

  /**
   * Create the {@link Environment}.
   *
   * @param iPath Index path. May be null, if no index should be used. If so an
   * {@link Exception} is thrown, if any index related function is called.
   * @param dPath Data path
   * @param isTest If true, testing flag is set
   * @param newFields Index fields to use
   * @param stopwords Stopwords to use
   * @throws IOException Thrown on low-level I7O errors
   */
  private static void create(final String iPath,
      final String dPath, final boolean isTest,
      final String[] newFields, final Collection<String> stopwords)
      throws
      IOException, NoIndexException {
    Environment.dataPath = dPath;
    if (iPath == null) {
      noIndex = true;
    } else {
      Environment.indexPath = iPath;
    }
    Environment.isTestRun = isTest;

    setStopwords(stopwords);

    if (!Environment.noIndex) {
      Environment.indexReader = openReader(new File(Environment.indexPath));
    }

    if (newFields == null) {
      if (!Environment.noIndex) {
        final Collection<String> idxFields = IndexUtils.getFields();
        Environment.fields = idxFields.toArray(new String[idxFields.size()]);
      }
    } else {
      Environment.fields = newFields.clone();
    }

    Environment.initialized = true;

    if (Environment.noIndex) {
      LOG.info("Index Path: None. Running without any index.");
    } else {
      LOG.info("Index Path: {}", iPath);
    }
    LOG.info("Data Path: {}", dPath);
  }

  /**
   * Get the generation number of the index. This is the generation number of
   * the last commit to the index.
   *
   * @return Generation number of the index
   * @throws Environment.NoIndexException Thrown, if no index is set in the
   * {@link Environment}
   */
  public static Long getIndexGeneration()
      throws NoIndexException {
    if (Environment.noIndex) {
      throw NoIndexException.EXCEPTION;
    }
    if (Environment.indexReader == null) {
      throw new IllegalStateException(
          "Environment not initialized. (indexReader)");
    }
    return Environment.indexGeneration;
  }

  /**
   * Testing only. Check if a test run is configured.
   *
   * @return True, if a test is currently running
   */
  public static boolean isTestRun() {
    return Environment.isTestRun;
  }

  /**
   * Get the list of stop-words.
   *
   * @return List of stop-words. An empty list is returned, if the environment
   * is not initialized.
   */
  public static Collection<String> getStopwords() {
    return Collections.unmodifiableCollection(Environment.STOPWORDS);
  }

  /**
   * Set the list of stop-words to use.
   *
   * @param words List of stop-words
   */
  private static void setStopwords(final Collection<String> words) {
    Environment.STOPWORDS.clear();
    Environment.STOPWORDS.addAll(words);
    LOG.info("Stop-words: using {} stopwords.", Environment.STOPWORDS.
        size());
  }

  /**
   * Tries to open a Lucene IndexReader.
   *
   * @param indexDir Directory of the Lucene index
   * @return Reader for accessing the Lucene index
   * @throws IOException Thrown on low-level I/O-errors
   */
  private static IndexReader openReader(final File indexDir)
      throws
      IOException {
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
   * Checks, if the environment is initialized. Throws a runtime exception, if
   * not.
   */
  private static void initialized() {
    if (!Environment.initialized) {
      throw new IllegalStateException("Environment not initialized");
    }
  }

  /**
   * Checks, if the environment is initialized.
   *
   * @return True, if initialized, false otherwise
   */
  public static boolean isInitialized() {
    return Environment.initialized;
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
   * Set the index document fields that get searched.
   *
   * @param newFields List of fields to use for searching
   */
  private static void setFields(final String[] newFields)
      throws
      NoIndexException {
    @SuppressWarnings("CollectionsToArray")
    final String[] uniqueFields
        = new HashSet<>(Arrays.asList(newFields)).toArray(
        new String[newFields.length]);
    if (!Environment.noIndex) {
      IndexUtils.checkFields(uniqueFields);
    }
    Environment.fields = uniqueFields;
    LOG.info("Index fields: {}", Arrays.toString(Environment.fields));
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
   * Sets the {@link IndexDataProvider} to use. This should normally be done
   * after the {@link Environment} is created.
   *
   * @param idp {@link IndexDataProvider} to use
   * @throws IOException Thrown on low-level I/O errors
   */
  private static void setDataProvider(final IndexDataProvider idp)
      throws
      IOException, NoIndexException {
    Environment.dataProvider = idp;

    if (!Environment.noIndex) {
      if (Environment.fields == null) {
        // use all index fields
        final Fields idxFields = MultiFields.
            getFields(Environment.indexReader);
        final Iterator<String> idxFieldNamesIt = idxFields.iterator();
        final Collection<String> idxFieldNames = new HashSet<>(idxFields.
            size());
        while (idxFieldNamesIt.hasNext()) {
          idxFieldNames.add(idxFieldNamesIt.next());
        }
        setFields(idxFieldNames.toArray(new String[idxFieldNames.
            size()]));
      } else {
        IndexUtils.checkFields(Environment.fields);
        setFields(Environment.fields);
      }
    }

    LOG.info("DataProvider: {}", Environment.dataProvider.getClass().
        getCanonicalName());
  }

  /**
   * Get the reader used to access the Lucene index.
   *
   * @return Lucene index reader
   * @throws Environment.NoIndexException Thrown, if no index is set in the
   * {@link Environment}
   */
  public static IndexReader getIndexReader()
      throws NoIndexException {
    if (Environment.noIndex) {
      throw NoIndexException.EXCEPTION;
    }
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
   * @throws Environment.NoIndexException Thrown, if no index is set in the
   * {@link Environment}
   */
  public static String getIndexPath()
      throws NoIndexException {
    if (Environment.noIndex) {
      throw NoIndexException.EXCEPTION;
    }
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
    Environment.dataPath = null;
    Environment.dataProvider = null;
    Environment.fields = null;
    Environment.indexPath = null;
    Environment.STOPWORDS.clear();
    Environment.indexReader = null;
    Environment.initialized = false;
  }

  /**
   * Shutdown the {@link Environment} and save all pending properties. This
   * tries to close the {@link IndexDataProvider} and {@link IndexReader} also.
   */
  public static void shutdown() {
    LOG.info("Shutting down Environment.");
    Environment.dataProvider.dispose();
    if (!Environment.noIndex) {
      try {
        Environment.indexReader.close();
      } catch (IOException ex) {
        LOG.error("Exception while closing IndexReader.", ex);
      }
    }
    clear();
  }

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
   * Exception to indicate that there's no index set in the {@link
   * Environment}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class NoIndexException
      extends Exception {

    /**
     * Static exception to indicate that no index is set.
     */
    private static final NoIndexException EXCEPTION = new NoIndexException();
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = -2233836956269660319L;

    private NoIndexException() {
      super("Environment is running without index.");
    }
  }

  /**
   * Builder to initialize the {@link Environment}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder {

    /**
     * Default cache name.
     */
    public static final String DEFAULT_CACHE_NAME = "temp-";
    /**
     * Timestamp to create temporary files.
     */
    private static final long TS = (System.currentTimeMillis() / 1000L);
    /**
     * Default {@link IndexDataProvider} class to use.
     */
    private static final Class<? extends IndexDataProvider> DP_DEFAULT_CLASS
        = DirectIndexDataProvider.class;
    /**
     * Index path.
     */
    private final String idxPath;
    /**
     * Data path.
     */
    private final String dataPath;
    /**
     * Index fields to use.
     */
    private String[] fields = null;
    /**
     * Stopwords to use.
     */
    private Collection<String> stopwords = Collections.emptySet();
    /**
     * Testing flag.
     */
    private boolean isTest = false;
    /**
     * Flag indicating, if warmUp should be called for the {@link
     * IndexDataProvider}.
     */
    private boolean warmUp = false;
    /**
     * {@link IndexDataProvider} class to use.
     */
    private Class<? extends IndexDataProvider> dpClass = null;
    /**
     * {@link IndexDataProvider} to use.
     */
    private IndexDataProvider dpInstance = null;
    /**
     * Named cache to use by {@link IndexDataProvider}.
     */
    private String cacheName = DEFAULT_CACHE_NAME + TS;
    /**
     * Signals {@link IndexDataProvider} to try to create the named cache.
     */
    private boolean createCache = false;
    /**
     * Signals {@link IndexDataProvider} to try load the named cache.
     */
    private boolean loadCache = false;
    /**
     * Signals {@link IndexDataProvider} to try to load or create the named
     * cache.
     */
    private boolean loadOrCreateCache = false;

    /**
     * Builder initializer.
     *
     * @param newIdxPath Index path, may be null, to run without an index
     * @param newDataPath Data path
     * @param allowNoIndex If true, empty value for index path is allowed
     * @throws IOException Thrown on low-level I/O errors
     */
    private Builder(final String newIdxPath, final String newDataPath,
        final boolean allowNoIndex)
        throws IOException {
      if (!allowNoIndex && (newIdxPath == null || newIdxPath.isEmpty())) {
        throw new IllegalArgumentException("Empty index path.");
      }
      if (newDataPath == null || newDataPath.isEmpty()) {
        throw new IllegalArgumentException("Empty data path.");
      }
      if (Environment.isInitialized()) {
        throw new IllegalStateException("Environment already initialized.");
      }
      checkPathes(newIdxPath, newDataPath);
      if (newIdxPath == null || newIdxPath.isEmpty()) {
        this.idxPath = null;
      } else {
        this.idxPath = newIdxPath + (newIdxPath.endsWith(File.separator)
            ? "" : File.separator);
      }
      this.dataPath = newDataPath + (newDataPath.endsWith(File.separator)
          ? "" : File.separator);
    }

    /**
     * Initialize the builder with a given index and data path.
     *
     * @param newIdxPath Index path
     * @param newDataPath Data path
     * @throws IOException Thrown on low-level I/O errors
     */
    public Builder(final String newIdxPath, final String newDataPath)
        throws
        IOException {
      this(newIdxPath, newDataPath, false);
    }

    /**
     * Initializes the builder using no index. You should use a appropriate
     * {@link IndexDataProvider} that supplies the needed informations without a
     * separate Lucene index.
     *
     * @param newDataPath Data path
     * @throws IOException Thrown on low-level I/O errors
     */
    public Builder(final String newDataPath)
        throws IOException {
      this(null, newDataPath, true);
    }

    /**
     * Check if the configured pathes are available. Tries to create some of
     * them, if it's not the case.
     *
     * @throws IOException Thrown if a path is not there or it cannot be
     * created
     */
    private void checkPathes(final String iPath, final String dPath)
        throws IOException {
      final File dataDir = new File(dPath);
      if (dataDir.exists()) {
        if (!dataDir.isDirectory()) {
          throw new IOException("Data path '" + dPath
                                + "' exists, but is not a directory.");
        }
      } else if (!dataDir.mkdirs()) {
        throw new IOException("Error while creating data directories '"
                              + dPath + "'.");
      }

      if (iPath != null) { // may be null, if we run without any index
        final File idxDir = new File(iPath);
        if (idxDir.exists()) {
          if (!idxDir.isDirectory()) {
            throw new IOException("Index path '" + iPath
                                  + "' exists, but is not a directory.");
          }
        } else if (!idxDir.mkdirs()) {
          throw new IOException("Error while creating index directories '"
                                + iPath + "'.");
        }
      }
    }

    /**
     * Set how the {@link IndexDataProvider} should create it's cache.
     *
     * @param create If true, a new cache will be created
     * @param load If true, a cache will be loaded
     * @param loadOrCreate If true, a cache will be created, if not exist
     */
    private void setCacheInstruction(final boolean create, final boolean load,
        final boolean loadOrCreate) {
      this.createCache = false;
      this.loadCache = false;
      this.loadOrCreateCache = false;
      if (create) {
        this.createCache = true;
      } else if (load) {
        this.loadCache = true;
      } else if (loadOrCreate) {
        this.loadOrCreateCache = true;
      }
    }

    /**
     * Instructs the the {@link IndexDataProvider} to load the named cache.
     *
     * @param name Cache name
     * @return Self reference
     */
    public Builder loadCache(final String name) {
      this.cacheName = name;
      setCacheInstruction(false, true, false);
      return this;
    }

    /**
     * Instructs the the {@link IndexDataProvider} to create the named cache.
     *
     * @param name Cache name
     * @return Self reference
     */
    public Builder createCache(final String name) {
      this.cacheName = name;
      setCacheInstruction(true, false, false);
      return this;
    }

    /**
     * Instructs the the {@link IndexDataProvider} to try load the named cache
     * and create it, if not found.
     *
     * @param name Cache name
     * @return Self reference
     */
    public Builder loadOrCreateCache(final String name) {
      this.cacheName = name;
      setCacheInstruction(false, false, true);
      return this;
    }

    /**
     * Setup the environment.
     *
     * @throws Exception Thrown, if creating the {@link Environment} or {@link
     * IndexDataProvider} fails
     */
    public void build()
        throws Exception {
      Environment.create(this.idxPath, this.dataPath, this.isTest,
          this.fields, this.stopwords);
      if (this.dpClass != null) {
        try {
          this.dpInstance = this.dpClass.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
          LOG.error("Error creating IndexDataProvider instance.", ex);
          throw new IllegalStateException(
              "Error creating IndexDataProvider instance.");
        }
      } else if (this.dpInstance == null) {
        try {
          this.dpInstance = Builder.DP_DEFAULT_CLASS.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
          LOG.error("Error creating default IndexDataProvider instance.", ex);
          throw new IllegalStateException(
              "Error creating default IndexDataProvider instance.");
        }
      }
      Environment.setDataProvider(this.dpInstance);
      if (this.cacheName.equals(Builder.DEFAULT_CACHE_NAME)) {
        this.dpInstance.createCache(Builder.DEFAULT_CACHE_NAME);
      } else {
        if (this.loadCache) {
          this.dpInstance.loadCache(this.cacheName);
        } else if (this.loadOrCreateCache) {
          this.dpInstance.loadOrCreateCache(this.cacheName);
        } else {
          this.dpInstance.createCache(this.cacheName);
        }
      }
      if (this.warmUp) {
        dpInstance.warmUp();
      }
    }

    /**
     * Set the index document fields that get searched.
     *
     * @param newFields List of fields to use for searching
     * @return Self reference
     */
    public Builder fields(final String[] newFields) {
      this.fields = newFields.clone();
      return this;
    }

    /**
     * Set the list of stop-words to use.
     *
     * @param words List of stop-words
     * @return Self reference
     */
    public Builder stopwords(final Collection<String> words) {
      this.stopwords = new HashSet<>(words.size());
      this.stopwords.addAll(words);
      return this;
    }

    /**
     * Set the testing flag for debugging.
     *
     * @return Self reference
     */
    public Builder testRun() {
      this.isTest = true;
      return this;
    }

    /**
     * Set the {@link IndexDataProvider} class to instantiate.
     *
     * @param dpc {@link IndexDataProvider} class to use
     * @return Self reference
     */
    public Builder dataProvider(final Class<? extends IndexDataProvider> dpc) {
      this.dpClass = dpc;
      return this;
    }

    /**
     * Set the {@link IndexDataProvider} to use.
     *
     * @param dp {@link IndexDataProvider}
     * @return Self reference
     */
    public Builder dataProvider(final IndexDataProvider dp) {
      this.dpInstance = dp;
      return this;
    }

    /**
     * Call the warmUp function of the {@link IndexDataProvider} after the
     * {@link Environment} has loaded.
     *
     * @return Self reference
     */
    public Builder autoWarmUp() {
      this.warmUp = true;
      return this;
    }
  }
}
