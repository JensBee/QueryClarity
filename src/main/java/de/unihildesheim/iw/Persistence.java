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

import de.unihildesheim.iw.lucene.Environment;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
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
  public final DB db;
  /**
   * Object storing meta information about the Lucene index and {@link
   * Environment} configuration.
   */
  private final StorageMeta meta;

  /**
   * Create a new persistent store.
   *
   * @param newDb Database reference
   * @param empty True if the store should be treated as being empty
   * @throws Environment.NoIndexException Thrown, if no index is provided in the
   * {@link Environment}
   */
  public Persistence(final DB newDb, final boolean empty)
      throws
      Environment.NoIndexException {
    this.db = newDb;

    this.meta = new StorageMeta();

    if (empty) {
      getMetaData(true);
      updateMetaData();
    } else {
      getMetaData(false);
    }
  }

  /**
   * Get the meta-information about the store.
   *
   * @param create If true, current meta information will be written, before
   * returning the data.
   */
  private void getMetaData(final boolean create) {
    meta.indexPath = this.db.getAtomicString(Storage.PERSISTENCE_IDX_PATH.
        name());
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
   *
   * @throws Environment.NoIndexException Thrown, if no index is provided in the
   * {@link Environment}
   */
  public void updateMetaData()
      throws Environment.NoIndexException {
    LOG.debug("Updating meta-data.");
    meta.setIndexPath(Environment.getIndexPath());
    meta.setIndexCommitGen(Environment.getIndexGeneration());
    meta.setFields(Arrays.asList(Environment.getFields()));
    meta.setStopWords(Environment.getStopwords());
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
     * Path to the Lucene index.
     */
    PERSISTENCE_IDX_PATH,
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
     * Index path used, when creating this store.
     */
    @SuppressWarnings({"ProtectedField", "checkstyle:visibilitymodifier"})
    protected Atomic.String indexPath;
    /**
     * Latest index commit generation visible when creating this store.
     */
    @SuppressWarnings({"ProtectedField", "checkstyle:visibilitymodifier"})
    protected Atomic.Long indexCommitGen;
    /**
     * Fields active in the {@link Environment} when creating this store.
     */
    @SuppressWarnings({"ProtectedField", "checkstyle:visibilitymodifier"})
    protected Set<String> fields;
    /**
     * Stopwords active in the {@link Environment} when creating this store.
     */
    @SuppressWarnings({"ProtectedField", "checkstyle:visibilitymodifier"})
    protected Set<String> stopWords;

    /**
     * Set the index path.
     *
     * @param newIndexPath Current path to Lucene index
     */
    protected void setIndexPath(final String newIndexPath) {
      this.indexPath.set(newIndexPath);
    }

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
    protected void setFields(final Collection<String> newFields) {
      this.fields.clear();
      this.fields.addAll(new HashSet<>(newFields));
    }

    /**
     * Set the current active stopwords.
     *
     * @param newStopWords List of stopwords
     */
    protected void setStopWords(final Collection<String> newStopWords) {
      this.stopWords.clear();
      this.stopWords.addAll(new HashSet<>(newStopWords));
    }

    /**
     * Checks the index fields currently set in the {@link Environment} against
     * those stored in the meta-information.
     *
     * @return True, if both contain the same field names.
     */
    public boolean fieldsCurrent() {
      final Collection<String> eFields = Arrays.
          asList(Environment.getFields());
      if (this.fields == null) {
        throw new IllegalStateException("Fields meta information not set.");
      }
      return this.fields.size() == eFields.size() && this.fields.containsAll(
          eFields);
    }

    /**
     * Checks the stopwords currently set in the {@link Environment} against
     * those stored in the meta-information.
     *
     * @return True, if both contain the same list of stopwords.
     */
    public boolean stopWordsCurrent() {
      if (this.stopWords == null) {
        throw new IllegalStateException("Stopword meta information not set.");
      }
      return this.stopWords.size() == Environment.getStopwords().size()
             && this.stopWords.containsAll(Environment.getStopwords());
    }

    /**
     * Checks the current index commit generation against the version stored in
     * the meta-information.
     *
     * @return True, if both generation numbers are the same.
     * @throws Environment.NoIndexException Thrown, if no index is provided in
     * the {@link Environment}
     */
    public boolean generationCurrent()
        throws Environment.NoIndexException {
      if (this.indexCommitGen == null) {
        throw new IllegalStateException("Commit generation "
                                        + "meta information not set.");
      }
      return Environment.getIndexGeneration().
          equals(this.indexCommitGen.get());
    }
  }

  /**
   * Builder to create a new persistent store.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder {

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
    private final DBMaker dbMkr;
    /**
     * Name of the database to create.
     */
    private final String name;
    /**
     * Resulting filename of the new database.
     */
    private final File dbFile;

    /**
     * Initialize the builder.
     *
     * @param newName Name of the storage to create
     */
    public Builder(final String newName) {
      this.name = newName;
      this.dbFile = new File(Environment.getDataPath() + Builder.PREFIX
                             + "_" + this.name);
      this.dbMkr = DBMaker.newFileDB(this.dbFile);
    }

    /**
     * Get the database file name.
     *
     * @return Database file name
     */
    public String getFileName() {
      return this.dbFile.toString();
    }

    /**
     * Check, if a database with the given name already exists.
     *
     * @return True, if exists.
     */
    public boolean exists() {
      return this.dbFile.exists();
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
      this.dbMkr.deleteFilesAfterClose();
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
          .closeOnJvmShutdown();
      return this;
    }

    /**
     * Creates a database with the current configuration.
     *
     * @return {@link} Persistence} with a database created using the current
     * configuration
     * @throws IOException Thrown on low-level I/O errors
     * @throws Environment.NoIndexException Thrown, if no index is provided in
     * the {@link Environment}
     */
    public Persistence make()
        throws IOException, Environment.NoIndexException {
      if (exists()) {
        throw new IOException("Database file exists: " + getFileName());
      }
      LOG.debug("New fileDB @ {}", this.dbFile);
      return new Persistence(this.dbMkr.make(), true);
    }

    /**
     * Loads a database with the current configuration.
     *
     * @return {@link} Persistence} with a database created using the current
     * configuration
     * @throws FileNotFoundException Thrown, if the database could not be found
     * @throws Environment.NoIndexException Thrown, if no index is provided in
     * the {@link Environment}
     */
    public Persistence get()
        throws FileNotFoundException,
               Environment.NoIndexException {
      if (!exists()) {
        throw new FileNotFoundException("Database file not found.");
      }
      return new Persistence(this.dbMkr.make(), false);
    }

    /**
     * Loads or creates a database with the current configuration.
     *
     * @return {@link} Persistence} with a database created using the current
     * configuration
     * @throws Environment.NoIndexException Thrown, if no index is provided in
     * the {@link Environment}
     */
    public Persistence makeOrGet()
        throws Environment.NoIndexException {
      try {
        if (exists()) {
          return get();
        }
        return make();
      } catch (IOException ex) {
        LOG.error("Error getting database.", ex);
        throw new IllegalStateException("Error creating database.");
      }
    }
  }
}
