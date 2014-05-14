/*
 * Copyright (C) 2014 Jens Bertram
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
package de.unihildesheim.iw;

import de.unihildesheim.iw.util.FileUtils;
import de.unihildesheim.iw.util.RandomValue;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * @author Jens Bertram
 */
public final class Persistence {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(Persistence.class);
  /**
   * Database reference.
   */
  public DB db;
  /**
   * Object storing meta information about the current Lucene index state and
   * the runtime configuration.
   */
  private StorageMeta meta;

  private Persistence() {
  }

  /**
   * Tries to create a file directory to store the persistent data.
   *
   * @param filePath Path to use for storing
   * @return File instance of the path
   * @throws IOException Thrown, if the path is not a directory, if the path
   * does not exist an could not be created or if reading/writing to this
   * directory is not allowed.
   */
  public static final File tryCreateDataPath(final String filePath)
      throws IOException {
    if (filePath == null || filePath.trim().isEmpty()) {
      throw new IllegalArgumentException("Data path was empty.");
    }

    final File dataDir = new File(filePath);
    if (dataDir.exists()) {
      // check, if path is a directory
      if (!dataDir.isDirectory()) {
        throw new IOException("Data path '" + dataDir.getCanonicalPath()
            + "' exists, but is not a directory.");
      }
    } else if (!dataDir.mkdirs()) {
      throw new IOException("Error while creating data directories '"
          + dataDir.getCanonicalPath() + "'.");
    }
    if (!dataDir.canWrite() || !dataDir.canRead()) {
      throw new IOException("Insufficient rights for data directory '"
          + dataDir.getCanonicalPath() + "'.");
    }
    return dataDir;
  }

  protected static final Persistence build(final Builder builder,
      final boolean empty) {
    if (builder == null) {
      throw new IllegalArgumentException("Builder was null.");
    }
    final Persistence instance = new Persistence();
    instance.db = builder.getMaker().make();
    instance.meta = new StorageMeta();

    if (empty) {
      instance.getMetaData(true);
      instance.updateMetaData(builder.getDocumentFields(),
          builder.getStopwords());
      if (builder.getLastCommitGeneration() != null) {
        instance.meta.setIndexCommitGen(builder.getLastCommitGeneration());
      }
    } else {
      instance.getMetaData(false);
    }

    return instance;
  }

  /**
   * Get the meta-information about the store.
   *
   * @param create If true, current meta information will be written, before
   * returning the data.
   */
  private void getMetaData(final boolean create) {
    meta.indexCommitGen = this.db.getAtomicLong(
        Storage.PERSISTENCE_IDX_COMMIT_GEN.name());
    if (create && this.db.exists(Storage.PERSISTENCE_FIELDS.name())) {
      this.db.delete(Storage.PERSISTENCE_FIELDS.name());
    }
    //noinspection RedundantTypeArguments
    meta.fields = this.db.createHashSet(Storage.PERSISTENCE_FIELDS.name()).
        serializer(Serializer.STRING).<String>makeOrGet();

    if (create && this.db.exists(Storage.PERSISTENCE_STOPWORDS.name())) {
      this.db.delete(Storage.PERSISTENCE_STOPWORDS.name());
    }
    //noinspection RedundantTypeArguments
    meta.stopWords = this.db.createHashSet(Storage.PERSISTENCE_STOPWORDS.
        name()).serializer(Serializer.STRING).<String>makeOrGet();
  }

  /**
   * Update the meta-data records. This does not commit data to the database.
   */
  public void updateMetaData(final Set<String> fields,
      final Set<String> stopwords) {
    if (fields == null) {
      throw new IllegalArgumentException("Fields were null.");
    }
    if (stopwords == null) {
      throw new IllegalArgumentException("Stopwords were null.");
    }
    LOG.debug("Updating meta-data.");
    meta.setFields(fields);
    meta.setStopWords(stopwords);
  }

  /**
   * Get the storage meta-data object.
   *
   * @return Storage meta data
   */
  public StorageMeta getMetaData() {
    return this.meta;
  }

  /**
   * Keys to stored values in database.
   */
  private enum Storage {
    /**
     * Latest commit generation of the Lucene index.
     */
    PERSISTENCE_IDX_COMMIT_GEN,
    /**
     * List of stopwords used.
     */
    PERSISTENCE_STOPWORDS,
    /**
     * List of document fields used.
     */
    PERSISTENCE_FIELDS
  }

  /**
   * Object wrapping storage meta-information.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class StorageMeta {

    /**
     * Latest index commit generation visible when last updating this store.
     */
    Atomic.Long indexCommitGen;
    /**
     * Fields set when last updating this store.
     */
    Set<String> fields;
    /**
     * Stopwords set when last updating this store.
     */
    Set<String> stopWords;

    /**
     * Set the Lucene index commit generation.
     *
     * @param newIndexCommitGen Generation
     */
    protected void setIndexCommitGen(final Long newIndexCommitGen) {
      this.indexCommitGen.set(newIndexCommitGen);
    }

    /**
     * Set the current active Lucene document fields.
     *
     * @param newFields Fields list
     */
    protected void setFields(final Set<String> newFields) {
      if (newFields == null) {
        throw new IllegalArgumentException("Fields were null.");
      }
      this.fields.clear();
      this.fields.addAll(newFields);
    }

    /**
     * Set the current active stopwords.
     *
     * @param newStopwords List of stopwords
     */
    protected void setStopWords(final Set<String> newStopwords) {
      if (newStopwords == null) {
        throw new IllegalArgumentException("Stopwords were null.");
      }
      this.stopWords.clear();
      this.stopWords.addAll(newStopwords);
    }

    /**
     * Checks the index fields currently set against those stored in the
     * meta-information.
     *
     * @param currentFields List of fields to check
     * @return True, if both contain the same field names.
     */
    public boolean fieldsCurrent(final Set<String> currentFields) {
      if (this.fields == null) {
        throw new IllegalStateException("Fields meta information not set.");
      }
      if (currentFields == null) {
        throw new IllegalArgumentException("Fields were null.");
      }
      return this.fields.size() == currentFields.size() &&
          this.fields.containsAll(
              currentFields);
    }

    /**
     * Checks the stopwords currently set against those stored in the
     * meta-information.
     *
     * @param currentWords List of stopwords to check
     * @return True, if both contain the same list of stopwords.
     */
    public boolean stopWordsCurrent(final Set<String> currentWords) {
      if (this.stopWords == null) {
        throw new IllegalStateException("Stopword meta information not set.");
      }
      if (currentWords == null) {
        throw new IllegalArgumentException("Stopwords were null.");
      }
      return this.stopWords.size() == currentWords.size()
          && this.stopWords.containsAll(currentWords);
    }

    /**
     * Checks the current index commit generation against the version stored in
     * the meta-information.
     *
     * @return True, if both generation numbers are the same.
     */
    public boolean generationCurrent(final Long currentGen) {
      if (currentGen == null) {
        throw new IllegalArgumentException("Index commit generation was null.");
      }
      if (this.indexCommitGen == null) {
        throw new IllegalStateException("Commit generation "
            + "meta information not set.");
      }
      return this.indexCommitGen.get() == currentGen;
    }
  }

  /**
   * Builder to create a new persistent store.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      implements Buildable<Persistence> {

    /**
     * Instructions on how to load/create the persistent cache.
     */
    public enum LoadInstruction {
      /**
       * Tries to create a new cache.
       */
      MAKE,
      /**
       * Tries to load a cache.
       */
      GET,
      /**
       * Tries to load or create a cache.
       */
      MAKE_OR_GET
    }

    /**
     * Instruction on how to handle the cache.
     */
    private LoadInstruction cacheInstruction = LoadInstruction.MAKE_OR_GET;

    /**
     * Database async write flush delay.
     */
    private static final int DB_ASYNC_WRITEFLUSH_DELAY = 100;

    /**
     * Database file prefix.
     */
    private static final String PREFIX = "persist_";

    /**
     * Builder used to create a new database.
     */
    private final ExtDBMaker dbMkr = new ExtDBMaker();

    /**
     * Name of the database to create.
     */
    private String name;

    private String dataPath;

    /**
     * List of stopwords to use.
     */
    private Set<String> stopwords = Collections.<String>emptySet();

    /**
     * List of document fields to use.
     */
    private Set<String> documentFields = Collections.<String>emptySet();

    /**
     * Last commit generation id of the Lucene index.
     */
    private Long lastCommitGeneration;

    /**
     * Flag indicating, if the new instance will be temporary. If it's temporary
     * any data may be deleted on JVM exit.
     */
    private boolean isTemporary = false;

    /**
     * Random string to prefix a temporary storage with.
     */
    private final String randNameSuffix = RandomValue.getString(32);

    /**
     * Simple extension of MapDB's {@link DBMaker} to allow specifying the
     * database file after creating the maker instance.
     */
    private static final class ExtDBMaker
        extends DBMaker {
      private ExtDBMaker() {
        super();
      }

      private ExtDBMaker dbFile(final File file) {
        props.setProperty(Keys.file, file.getPath());
        return this;
      }
    }

    public Builder() {
    }

    public Builder dataPath(final String newDataPath) {
      if (newDataPath == null || newDataPath.trim().isEmpty()) {
        throw new IllegalArgumentException("Empty data path.");
      }
      this.dataPath = newDataPath;
      return this;
    }

    public Builder name(final String newName) {
      if (newName == null || newName.trim().isEmpty()) {
        throw new IllegalArgumentException("Empty cache name.");
      }
      if (this.isTemporary) {
        this.name = newName + "-" + this.randNameSuffix;
      } else {
        this.name = newName;
      }
      return this;
    }

    /**
     * Return a {@link DBMaker} with the current configuration.
     *
     * @return Maker instance
     */
    public DBMaker getMaker() {
      return this.dbMkr;
    }

    /**
     * Set flag, indicating that this instance is temporary and may be deleted
     * after closing the JVM.
     *
     * @return Self reference
     */
    public Builder temporary() {
      if (!this.isTemporary) {
        this.dbMkr.deleteFilesAfterClose();
        this.isTemporary = true;
        if (this.name != null) {
          this.name += "-" + this.randNameSuffix;
        }
      }
      return this;
    }

    /**
     * Initializes the internal {@link DBMaker} with default settings.
     *
     * @return Self reference
     */
    public Builder setDbDefaults() {
      this.dbMkr
          .transactionDisable()
          .commitFileSyncDisable()
          .asyncWriteEnable()
          .asyncWriteFlushDelay(DB_ASYNC_WRITEFLUSH_DELAY)
          .mmapFileEnableIfSupported()
          .compressionEnable()
          .closeOnJvmShutdown();
      return this;
    }

    /**
     * Set the last commit generation id of the Lucene index.
     *
     * @param cGen Commit generation id
     */
    public Builder lastCommitGeneration(final long cGen) {
      this.lastCommitGeneration = cGen;
      return this;
    }

    protected Long getLastCommitGeneration() {
      return this.lastCommitGeneration;
    }

    /**
     * Set a list of stopwords to use by this instance.
     *
     * @param words List of stopwords. May be empty.
     */
    public Builder stopwords(final Set<String> words) {
      if (words == null) {
        throw new IllegalArgumentException("Stopwords were null.");
      }
      this.stopwords = words;
      return this;
    }

    protected Set<String> getStopwords() {
      return this.stopwords;
    }

    /**
     * Set a list of document fields to use by this instance.
     *
     * @param fields List of field names. May be empty.
     * @return self reference
     */
    public Builder documentFields(
        final Set<String> fields) {
      if (fields == null) {
        throw new IllegalArgumentException("Fields were null.");
      }
      this.documentFields = fields;
      return this;
    }

    protected Set<String> getDocumentFields() {
      return this.documentFields;
    }

    /**
     * Creates a database with the current configuration.
     *
     * @return Self reference
     */
    public Builder make() {
      this.cacheInstruction = LoadInstruction.MAKE;
      return this;
    }

    /**
     * Loads a database with the current configuration.
     *
     * @return Self reference
     */
    public Builder get() {
      this.cacheInstruction = LoadInstruction.GET;
      return this;
    }

    /**
     * Loads or creates a database with the current configuration.
     *
     * @return Self reference
     */
    public Builder makeOrGet() {
      this.cacheInstruction = LoadInstruction.MAKE_OR_GET;
      return this;
    }

    /**
     * Tries to load an existing database and create the instance.
     *
     * @return Instance with current builder configuration
     * @throws FileNotFoundException Thrown, if the database could not be found
     */
    private Persistence getInstance(final File dbFile)
        throws FileNotFoundException {
      if (!dbFile.exists()) {
        throw new FileNotFoundException("Database file not found.");
      }
      return Persistence.build(this, false);
    }

    /**
     * Tries to create a new database and create the instance.
     *
     * @return Instance with current builder configuration
     * @throws IOException Thrown on low-level I/O errors
     */
    private Persistence makeInstance(final File dbFile)
        throws IOException {
      if (dbFile.exists()) {
        throw new IOException("Database file exists: " + dbFile.toString());
      }
      LOG.debug("New fileDB @ {}", dbFile);
      return Persistence.build(this, true);
    }

    public LoadInstruction getCacheLoadInstruction() {
      return this.cacheInstruction;
    }

    public boolean dbExists()
        throws ConfigurationException {
      if (this.dataPath == null || this.dataPath.trim().isEmpty()) {
        throw new ConfigurationException("Data path not set.");
      }
      if (this.name == null || this.name.trim().isEmpty()) {
        throw new ConfigurationException("Empty storage name.");
      }
      return new File(
          FileUtils.makePath(this.dataPath) + Builder.PREFIX + "_" + this
              .name
      ).exists();
    }

    /**
     * Creates a new instance with the current builder configuration.
     *
     * @return New instance
     * @throws ConfigurationException Thrown, if a mandatory configuration
     * option is unset
     */
    @Override
    public Persistence build()
        throws ConfigurationException, BuildException {
      validate();
      final File dbFile = new File(
          FileUtils.makePath(this.dataPath) + Builder.PREFIX + "_" + this.name);
      //this.dbMkr = DBMaker.newFileDB(dbFile);
      this.dbMkr.dbFile(dbFile);
      final Persistence p;
      try {
        switch (this.cacheInstruction) {
          case GET:
            p = getInstance(dbFile);
            break;
          case MAKE:
            p = makeInstance(dbFile);
            break;
          default:
            if (dbFile.exists()) {
              p = getInstance(dbFile);
            } else {
              p = makeInstance(dbFile);
            }
            break;
        }
      } catch (IOException e) {
        throw new BuildException(e);
      }
      if (this.isTemporary) {
        LOG.warn("Caches are temporary!");
      }
      return p;
    }

    @Override
    public void validate()
        throws ConfigurationException {
      if (this.dataPath == null || this.dataPath.trim().isEmpty()) {
        throw new ConfigurationException("Data path not set.");
      }
      if (this.name == null || this.name.trim().isEmpty()) {
        throw new ConfigurationException("Empty storage name.");
      }
      if (this.documentFields == null) {
        throw new ConfigurationException("Document fields are null.");
      }
      if (this.stopwords == null) {
        throw new ConfigurationException("Stopwords are null.");
      }
    }
  }
}
