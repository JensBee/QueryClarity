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
package de.unihildesheim;

import de.unihildesheim.lucene.Environment;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
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
   * Object storing meta information about the Lucene index and
   * {@link Environment} configuration.
   */
  private StorageMeta meta = new StorageMeta();

  /**
   * Keys to stored values in database.
   */
  private enum Storage {

    PERSISTENCE_IDX_PATH,
    PERSISTENCE_IDX_COMMIT_GEN,
    PERSISTENCE_STOPWORDS,
    PERSISTENCE_FIELDS
  }

  /**
   * Object wrapping storage meta-information.
   */
  public static final class StorageMeta {

    /**
     * Index path used, when creating this store.
     */
    protected Atomic.String indexPath;
    /**
     * Latest index commit generation visible when creating this store.
     */
    protected Atomic.Long indexCommitGen;
    /**
     * Fields active in the {@link Environment} when creating this store.
     */
    protected Set<String> fields;
    /**
     * Stopwords active in the {@link Environment} when creating this store.
     */
    protected Set<String> stopWords;

    /**
     * Checks the index fields currently set in the {@link Environment}
     * against those stored in the meta-information.
     *
     * @return True, if both contain the same field names.
     */
    public boolean fieldsCurrent() {
      final Collection<String> eFields = Arrays.
              asList(Environment.getFields());
      LOG.debug("FIELDS: eF={} cF={}", eFields, this.fields);
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
      return this.stopWords.size() == Environment.getStopwords().size()
              && this.stopWords.containsAll(Environment.getStopwords());
    }

    /**
     * Checks the current index commit generation against the version stored
     * in the meta-information.
     *
     * @return True, if both generation numbers are the same.
     * @throws de.unihildesheim.lucene.Environment.NoIndexException Thrown, if
     * no index is provided in the {@link Environment}
     */
    public boolean generationCurrent() throws Environment.NoIndexException {
      return Environment.getIndexGeneration().
              equals(this.indexCommitGen.get());
    }
  }

  /**
   * Create a new persistent store.
   *
   * @param newDb Database reference
   * @param empty True if the store should be treated as being empty
   * @throws de.unihildesheim.lucene.Environment.NoIndexException Thrown, if
   * no index is provided in the {@link Environment}
   */
  public Persistence(final DB newDb, final boolean empty) throws
          Environment.NoIndexException {
    this.db = newDb;
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
    meta.fields = this.db.createHashSet(Storage.PERSISTENCE_FIELDS.name()).
            serializer(Serializer.STRING).makeOrGet();

    if (create && this.db.exists(Storage.PERSISTENCE_STOPWORDS.name())) {
      this.db.delete(Storage.PERSISTENCE_STOPWORDS.name());
    }
    meta.stopWords = this.db.createHashSet(Storage.PERSISTENCE_STOPWORDS.
            name()).serializer(Serializer.STRING).makeOrGet();
  }

  /**
   * Update the meta-data records. This does not commit data to the database.
   *
   * @throws de.unihildesheim.lucene.Environment.NoIndexException Thrown, if
   * no index is provided in the {@link Environment}
   */
  public void updateMetaData() throws Environment.NoIndexException {
    LOG.debug("Updating meta-data.");
    meta.indexPath.set(Environment.getIndexPath());
    meta.indexCommitGen.set(Environment.getIndexGeneration());
    meta.fields.clear();
    meta.fields.addAll(Arrays.asList(Environment.getFields()));
    meta.stopWords.clear();
    meta.stopWords.addAll(Environment.getStopwords());
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
   * Builder to create a new persistent store.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder {

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

    public boolean exists() {
      return this.dbFile.exists();
    }

    public DBMaker getMaker() {
      return this.dbMkr;
    }

    public Builder temporary() {
      this.dbMkr.deleteFilesAfterClose();
      return this;
    }

    public Builder setDbDefaults() {
      this.dbMkr
              .transactionDisable()
              .commitFileSyncDisable()
              .asyncWriteEnable()
              .asyncWriteFlushDelay(100)
              .mmapFileEnableIfSupported()
              .closeOnJvmShutdown();
      return this;
    }

    /**
     *
     * @return @throws IOException
     * @throws de.unihildesheim.lucene.Environment.NoIndexException Thrown, if
     * no index is provided in the {@link Environment}
     */
    public Persistence make() throws IOException, Environment.NoIndexException {
      if (exists()) {
        throw new IOException("Database file exists: " + getFileName());
      }
      LOG.debug("New fileDB @ {}", this.dbFile);
      return new Persistence(this.dbMkr.make(), true);
    }

    /**
     *
     * @return @throws FileNotFoundException
     * @throws de.unihildesheim.lucene.Environment.NoIndexException Thrown, if
     * no index is provided in the {@link Environment}
     */
    public Persistence get() throws FileNotFoundException,
            Environment.NoIndexException {
      if (!exists()) {
        throw new FileNotFoundException("Database file not found.");
      }
      return new Persistence(this.dbMkr.make(), false);
    }

    /**
     *
     * @return @throws FileNotFoundException
     * @throws de.unihildesheim.lucene.Environment.NoIndexException Thrown, if
     * no index is provided in the {@link Environment}
     */
    public Persistence makeOrGet() throws FileNotFoundException,
            Environment.NoIndexException {
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
