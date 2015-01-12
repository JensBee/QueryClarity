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
package de.unihildesheim.iw;

import de.unihildesheim.iw.util.FileUtils;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.StringUtils;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Wrapper for database creation.
 *
 * @author Jens Bertram
 */
public final class Persistence {

  /**
   * Logger instance for this class.
   */
  static final Logger LOG = LoggerFactory.getLogger(Persistence.class);
  /**
   * List of open database names.
   */
  private static final Collection<Path> OPEN_DB_FILES = new ArrayList<>(10);
  /**
   * Global identifier of this class. Used for configurations.
   */
  private static final String IDENTIFIER = "Persistence";
  /**
   * Prefix for {@link GlobalConfiguration configuration} items.
   */
  static final String CONF_PREFIX = GlobalConfiguration.mkPrefix
      (IDENTIFIER);
  /**
   * Database reference.
   */
  private final DB db;
  /**
   * File name of the database.
   */
  private final String dbFileName;
  /**
   * {@link Path} representation of the database.
   */
  private final Path dbFilePath;
  /**
   * Object storing meta information about the current Lucene index state and
   * the runtime {@link GlobalConfiguration configuration}.
   */
  private final StorageMeta meta;
  /**
   * Flag indicating, if the {@link DB} instance uses transactions.
   */
  private final boolean supportsTransaction;
  /**
   * Commit generation of the Lucene index.
   */
  private final Long indexCommitGen;
  /**
   * Flag indicating, if memory mapped files should be used by db instances.
   */
  public static final boolean USE_MMAP_FILES = GlobalConfiguration.conf()
      .getAndAddBoolean(CONF_PREFIX + "_useMMapFiles", true);

  /**
   * Constructor using builder object.
   *
   * @param builder Builder
   * @param empty If true, the database is expected as being empty (new)
   * @throws IOException Thrown on low-level I/O errors
   */
  Persistence(final Builder builder, final boolean empty)
      throws IOException {
    Objects.requireNonNull(builder, "Builder was null.");

    this.dbFileName = builder.dbMkr.getDbFileName();
    LOG.info("Opening database '{}'.", this.dbFileName);
    LOG.debug("Use MMapFiles: {}", USE_MMAP_FILES);

    this.dbFilePath = builder.dbMkr.getDbFile().toPath();
    if (builder.dbMkr.getDbFile().exists()) {
      for (final Path p : OPEN_DB_FILES) {
        if (Files.isSameFile(p, this.dbFilePath)) {
          throw new IllegalStateException(
              "Database '" + this.dbFileName + "' already in use.");
        }
      }
    }
    OPEN_DB_FILES.add(this.dbFilePath);
    LOG.debug("Open databases ({}): {}", OPEN_DB_FILES.size(), OPEN_DB_FILES);

    this.db = builder.dbMkr.make();
    this.meta = new StorageMeta();
    this.supportsTransaction = builder.dbMkr.supportsTransaction();
    this.indexCommitGen = builder.lastCommitGeneration;

    if (empty) {
      this.getMetaData(true);
      this.updateMetaData(builder.documentFields, builder.stopwords);
    } else {
      this.getMetaData(false);
    }
  }

  /**
   * Get the meta-information about the store.
   *
   * @param create If true, current meta information will be written, before
   * returning the data.
   */
  private void getMetaData(final boolean create) {
    if (this.db.exists(Storage.PERSISTENCE_IDX_COMMIT_GEN.name())) {
      if (create) {
        if (this.indexCommitGen == null) {
          this.db.getAtomicLong(Storage.PERSISTENCE_IDX_COMMIT_GEN.name())
              .set(-1L);
        } else {
          this.db.getAtomicLong(Storage.PERSISTENCE_IDX_COMMIT_GEN.name())
              .set(this.indexCommitGen);
        }
      }
    } else {
      this.db
          .createAtomicLong(Storage.PERSISTENCE_IDX_COMMIT_GEN.name(), -1L);
    }

    this.meta.setIndexCommitGen(
        this.db.getAtomicLong(Storage.PERSISTENCE_IDX_COMMIT_GEN.name()));

    if (create && this.db.exists(Storage.PERSISTENCE_FIELDS.name())) {
      this.db.delete(Storage.PERSISTENCE_FIELDS.name());
    }

    this.meta.setFields(this.db
        .createHashSet(Storage.PERSISTENCE_FIELDS.name()).
            serializer(Serializer.STRING)
        .<String>makeOrGet());

    if (create && this.db.exists(Storage.PERSISTENCE_STOPWORDS.name())) {
      this.db.delete(Storage.PERSISTENCE_STOPWORDS.name());
    }
    this.meta.setStopwords(this.db
        .createHashSet(Storage.PERSISTENCE_STOPWORDS.
            name()).serializer(Serializer.STRING)
        .<String>makeOrGet());
  }

  /**
   * Update the field and stopwords meta-data records. This does not commit data
   * to the database.
   *
   * @param fields Field names to set
   * @param stopwords Stopwords to set
   */
  public void updateMetaData(final Set<String> fields,
      final Set<String> stopwords) {
    LOG.info("Updating meta-data.");
    this.meta.resetFields(Objects.requireNonNull(fields, "Fields were null."));
    this.meta.resetStopwords(Objects.requireNonNull(stopwords,
        "Stopwords were null."));
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
  public static File tryCreateDataPath(final String filePath)
      throws IOException {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(filePath, "Path was null."))) {
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

  /**
   * Close the underlying database.
   */
  public void closeDb() {
    LOG.debug("Try closing database '{}'.", this.dbFileName);
    if (this.db.isClosed()) {
      LOG.warn("Database '{}' already closed.", this.dbFileName);
    } else {
      LOG.info("Closing database '{}'.", this.dbFileName);
      this.db.close();
    }
    OPEN_DB_FILES.remove(this.dbFilePath);
    LOG.debug("Open databases ({}): {}", OPEN_DB_FILES.size(), OPEN_DB_FILES);
  }

  /**
   * Clear the meta-data after loading the database.
   */
  public void clearMetaData() {
    getMetaData(true);
  }

  /**
   * Check, if the database supports transactions (keeps a transaction log).
   *
   * @return True, if transaction log is used
   */
  public boolean isTransactionSupported() {
    return this.supportsTransaction;
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
   * Get the database reference.
   *
   * @return Database instance
   */
  public DB getDb() {
    return this.db;
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
   * Object wrapping storage meta-information. Values are initialized by {@link
   * #getMetaData(boolean)}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class StorageMeta {

    /**
     * Index commit generation value.
     */
    private Atomic.Long indexCommitGen;
    /**
     * List of document fields.
     */
    private Set<String> fields;
    /**
     * List of stopwords.
     */
    private Set<String> stopwords;

    /**
     * Set the current active Lucene document fields.
     *
     * @param newFields Fields list
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    void setFields(final Set<String> newFields) {
      Objects.requireNonNull(newFields, "Fields were null.");
      this.fields = newFields;
    }

    /**
     * Reset the current active Lucene document fields.
     *
     * @param newFields New fields list
     */
    void resetFields(final Set<String> newFields) {
      Objects.requireNonNull(newFields, "Fields were null.");
      this.fields.clear();
      this.fields.addAll(newFields);
    }

    /**
     * Set the current active stopwords.
     *
     * @param newStopwords List of stopwords
     */
    @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
    void setStopwords(final Set<String> newStopwords) {
      Objects.requireNonNull(newStopwords, "Stopwords were null.");
      this.stopwords = newStopwords;
    }

    /**
     * Set the current active stopwords.
     *
     * @param newStopwords List of stopwords
     */
    void resetStopwords(final Set<String> newStopwords) {
      Objects.requireNonNull(newStopwords, "Stopwords were null.");
      this.stopwords.clear();
      this.stopwords.addAll(newStopwords);
    }

    /**
     * Checks the index fields currently set against those stored in the
     * meta-information.
     *
     * @param currentFields List of fields to check
     * @return True, if both contain the same field names.
     */
    public boolean areFieldsCurrent(final Set<String> currentFields) {
      Objects.requireNonNull(currentFields, "Fields were null.");

      if (this.fields == null) {
        throw new IllegalStateException("Fields meta information not set.");
      }
      LOG.debug("Fields stored={} new={}", this.fields, currentFields);
      return this.fields.size() == currentFields.size() &&
          this.fields.containsAll(currentFields);
    }

    /**
     * Checks the stopwords currently set against those stored in the
     * meta-information.
     *
     * @param currentWords List of stopwords to check
     * @return True, if both contain the same list of stopwords.
     */
    public boolean areStopwordsCurrent(final Set<String> currentWords) {
      Objects.requireNonNull(currentWords, "Stopwords were null.");

      if (this.stopwords == null) {
        throw new IllegalStateException("Stopword meta information not set.");
      }
      LOG.debug("Stopwords: old={} now={}", this.stopwords, currentWords);
      return this.stopwords.size() == currentWords.size()
          && this.stopwords.containsAll(currentWords);
    }

    /**
     * Check, if a index commit generation value is set.
     *
     * @return True, if a value is set
     */
    public boolean hasGenerationValue() {
      return this.indexCommitGen.get() != -1;
    }

    /**
     * Checks the current index commit generation against the version stored in
     * the meta-information.
     *
     * @param currentGen Generation value to test
     * @return True, if both generation numbers are the same.
     */
    public boolean isGenerationCurrent(final Long currentGen) {
      Objects.requireNonNull(currentGen, "Commit generation was null.");

      if (this.indexCommitGen == null) {
        throw new IllegalStateException("Commit generation "
            + "meta information not set.");
      }
      LOG.debug("Generations: this={} that={}", this.indexCommitGen.get(),
          currentGen);
      return this.indexCommitGen.get() == currentGen;
    }

    /**
     * Set the index commit generation value.
     *
     * @param newIndexCommitGen New value
     */
    void setIndexCommitGen(final Atomic.Long newIndexCommitGen) {
      this.indexCommitGen = newIndexCommitGen;
    }
  }

  /**
   * Builder to create a new persistent store.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      implements Buildable<Persistence> {

    /**
     * Database file prefix.
     */
    private static final String PREFIX = "persist_";

    /**
     * Builder used to create a new database.
     */
    @SuppressWarnings("PackageVisibleField")
    public final ExtDBMaker dbMkr;

    /**
     * Random string to prefix a temporary storage with.
     */
    private final String randNameSuffix;
    /**
     * List of stopwords to use.
     */
    @SuppressWarnings("PackageVisibleField")
    Set<String> stopwords;
    /**
     * List of document fields to use.
     */
    @SuppressWarnings("PackageVisibleField")
    Set<String> documentFields;
    /**
     * Last commit generation id of the Lucene index.
     */
    @SuppressWarnings("PackageVisibleField")
    Long lastCommitGeneration;
    /**
     * Directory where the database files should be stored.
     */
    private String dataPath;
    /**
     * Instruction on how to handle the cache.
     */
    private LoadInstruction cacheInstruction;
    /**
     * Name of the database to create.
     */
    private String name;
    /**
     * Flag indicating, if the new instance will be temporary. If it's temporary
     * any data may be deleted on JVM exit.
     */
    private boolean isTemporary;
    /**
     * Database async write flush delay.
     */
    public static final int DB_ASYNC_WRITEFLUSH_DELAY = GlobalConfiguration
        .conf()
        .getAndAddInteger(CONF_PREFIX + "db-async-writeflush-delay", 100);

    /**
     * Initializes the builder with default values.
     */
    public Builder() {
      this.dbMkr = new ExtDBMaker();
      this.dbMkr
          .transactionDisable()
          .compressionEnable()
          .strictDBGet()
          .checksumEnable()
          .closeOnJvmShutdown();
      if (USE_MMAP_FILES) {
        this.dbMkr.mmapFileEnableIfSupported();
      }
      this.cacheInstruction = LoadInstruction.MAKE_OR_GET;
      this.stopwords = Collections.emptySet();
      this.documentFields = Collections.emptySet();
      this.randNameSuffix = RandomValue.getString(32);
    }

    /**
     * Sets the path for storing database files.
     *
     * @param newDataPath Storage path
     * @return Self reference
     */
    public Builder dataPath(final String newDataPath) {
      if (StringUtils.isStrippedEmpty(Objects.requireNonNull(newDataPath,
          "Path was null."))) {
        throw new IllegalArgumentException("Empty data path.");
      }
      this.dataPath = newDataPath;
      return this;
    }

    /**
     * Sets the database name.
     *
     * @param newName Name
     * @return Self reference
     */
    public Builder name(final String newName) {
      if (StringUtils.isStrippedEmpty(
          Objects.requireNonNull(newName, "Name was null."))) {
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
     * Set the instruction to load the database in read-only mode.
     *
     * @return Self reference
     */
    public Builder readOnly() {
      this.dbMkr.readOnly(true);
      return this;
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
     * Set the last commit generation id of the Lucene index.
     *
     * @param cGen Commit generation id
     * @return Self reference
     */
    public Builder lastCommitGeneration(final long cGen) {
      this.lastCommitGeneration = cGen;
      return this;
    }

    /**
     * Set a list of stopwords to use by this instance.
     *
     * @param words List of stopwords. May be empty.
     * @return Self reference
     */
    public Builder stopwords(final Set<String> words) {
      this.stopwords = Objects.requireNonNull(words, "Stopwords were null.");
      return this;
    }

    /**
     * Set a list of document fields to use by this instance.
     *
     * @param fields List of field names. May be empty.
     * @return self reference
     */
    public Builder documentFields(
        final Set<String> fields) {
      this.documentFields = Objects.requireNonNull(fields, "Fields were null.");
      return this;
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
     * Get the instruction on how to load the cache
     *
     * @return Cache loading instruction
     */
    public LoadInstruction getCacheLoadInstruction() {
      return this.cacheInstruction;
    }

    /**
     * Checks, if a database with the current filename already exists.
     *
     * @return True, if it exist
     * @throws ConfigurationException Thrown, if no data-path or filename is
     * set
     */
    @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
    public boolean dbExists()
        throws ConfigurationException {
      if (this.dataPath == null || StringUtils.isStrippedEmpty(this.dataPath)) {
        throw new ConfigurationException("Data path not set.");
      }
      if (this.name == null || StringUtils.isStrippedEmpty(this.name)) {
        throw new ConfigurationException("Empty storage name.");
      }
      return new File(
          FileUtils.makePath(this.dataPath) + PREFIX + "_" + this.name
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
      try {
        tryCreateDataPath(this.dataPath);
      } catch (final IOException e) {
        throw new BuildException("Failed to create data path.", e);
      }
      final File dbFile = new File(
          FileUtils.makePath(this.dataPath) + PREFIX + "_" + this.name);
      this.dbMkr.dbFile(dbFile);

      // debug dump configuration
      if (LOG.isDebugEnabled()) {
        this.dbMkr.debugDump();
      }

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
      } catch (final IOException e) {
        throw new BuildException(e);
      }
      if (this.isTemporary) {
        LOG.warn("Caches are temporary!");
      }
      return p;
    }

    /**
     * Tries to load an existing database and create the instance.
     *
     * @param dbFile Database file to create
     * @return Instance with current builder configuration
     * @throws FileNotFoundException Thrown, if the database could not be found
     */
    private Persistence getInstance(final File dbFile)
        throws IOException {
      if (!dbFile.exists()) {
        throw new FileNotFoundException("Database file not found.");
      }
      return new Persistence(this, false);
    }

    /**
     * Tries to create a new database and create the instance.
     *
     * @param dbFile Database file to create
     * @return Instance with current builder configuration
     * @throws IOException Thrown on low-level I/O errors
     */
    private Persistence makeInstance(final File dbFile)
        throws IOException {
      if (dbFile.exists()) {
        throw new IOException("Database file exists: " + dbFile);
      }
      LOG.debug("New fileDB @ {}", dbFile);
      return new Persistence(this, true);
    }

    @Override
    public void validate()
        throws ConfigurationException {
      if (this.dataPath == null || StringUtils.isStrippedEmpty(this.dataPath)) {
        throw new ConfigurationException("Data path not set.");
      }
      if (this.name == null || StringUtils.isStrippedEmpty(this.name)) {
        throw new ConfigurationException("Empty storage name.");
      }
      if (this.documentFields == null) {
        throw new ConfigurationException("Document fields are null.");
      }
      if (this.stopwords == null) {
        throw new ConfigurationException("Stopwords are null.");
      }
    }

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
     * Simple extension of MapDB's {@link DBMaker} to allow specifying the
     * database file after creating the maker instance.
     */
    public static final class ExtDBMaker
        extends DBMaker {

      /**
       * Database file.
       */
      private File dbFile;

      /**
       * Empty constructor for parent class access.
       */
      ExtDBMaker() {
      }

      /**
       * Debug dump the current {@link DBMaker} configuration.
       */
      void debugDump() {
        for (final Object k : this.props.keySet()) {
          LOG.debug("Prop k={} v={}", k, this.props.get(k));
        }
      }

      /**
       * Allows to open the database in read-only mode.
       *
       * @param state True, if it should be opened read-only
       */
      void readOnly(final boolean state) {
        if (state) {
          this.props.setProperty(Keys.readOnly, this.TRUE);
        } else {
          this.props.setProperty(Keys.readOnly, "false");
        }
      }

      public ExtDBMaker compressionDisable() {
        this.props.remove(Keys.compression);
        return this;
      }

      public ExtDBMaker transactionEnable() {
        this.props.remove(Keys.transactionDisable);
        return this;
      }

      public ExtDBMaker noChecksum() {
        this.props.remove(Keys.checksum);
        return this;
      }

      /**
       * Check, if this db instance uses transactions.
       *
       * @return True, if transactions are supported
       */
      @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
      boolean supportsTransaction() {
        return !this.TRUE.equals(this.props.get(Keys.transactionDisable));
      }

      /**
       * Sets the database file
       *
       * @param file Database file
       * @return Self reference
       */
      ExtDBMaker dbFile(final File file) {
        this.props.setProperty(Keys.file, file.getPath());
        this.dbFile = file;
        return this;
      }

      /**
       * Get the database file.
       *
       * @return Database file
       */
      File getDbFile() {
        return this.dbFile;
      }

      /**
       * Gets the database file name
       *
       * @return Database file name
       */
      String getDbFileName() {
        return this.props.getProperty(Keys.file);
      }
    }
  }
}
