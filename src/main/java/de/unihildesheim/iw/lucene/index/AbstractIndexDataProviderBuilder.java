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

package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.util.FileUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * Abstract builder to create an {@link IndexDataProvider} instance.
 *
 * @author Jens Bertram
 */
public abstract class AbstractIndexDataProviderBuilder<T extends
    AbstractIndexDataProvider>
    implements Buildable<T> {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractIndexDataProviderBuilder.class);

  /**
   * List of stopwords to use.
   */
  protected Set<String> stopwords = Collections.<String>emptySet();

  /**
   * List of document fields to use.
   */
  protected Set<String> documentFields = Collections.<String>emptySet();

  /**
   * Flag indicating, if the new instance will be temporary. How to handle this
   * state is up to the specific implementation.
   */
  protected boolean isTemporary = false;

  /**
   * {@link IndexReader} to use for accessing the Lucene index.
   */
  protected IndexReader idxReader = null;

  /**
   * File path where the Lucene index resides in.
   */
  private File idxPath = null;

  /**
   * File path where the working data will be stored.
   */
  private File dataPath = null;

  /**
   * {@link Directory} instance pointing at the Lucene index.
   */
  private Directory luceneDir = null;

  /**
   * Builder used to create a proper caching backend.
   */
  protected Persistence.Builder persistenceBuilder = new Persistence.Builder();

  /**
   * Implementation identifier used for proper cache naming.
   */
  private String identifier = null;

  /**
   * Warm-up the instance right after building it?
   */
  protected boolean doWarmUp = false;

  protected Long lastCommitGeneration = null;

  /**
   * Builds the instance.
   *
   * @return New instance
   */
  public abstract T build()
      throws Exception;

  /**
   * Constructor setting the implementation identifier for the cache.
   *
   * @param newIdentifier Implementation identifier for the cache
   */
  protected AbstractIndexDataProviderBuilder(final String newIdentifier) {
    if (newIdentifier == null || newIdentifier.isEmpty()) {
      throw new IllegalArgumentException("Empty identifier name.");
    }
    this.identifier = newIdentifier;
  }

  /**
   * Create a cache name prefixed with the identifier of the implementing
   * class.
   *
   * @param name Cache name
   * @return Cache name prefixed with current identifier
   */
  private String createCacheName(final String name) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Empty cache name.");
    }
    return this.identifier + "_" + name;
  }

  /**
   * Instruction to load the named cache.
   *
   * @param name Cache name
   * @return Self reference
   */
  public AbstractIndexDataProviderBuilder loadCache(final String name) {
    this.persistenceBuilder.name(createCacheName(name));
    this.persistenceBuilder.get();
    return this;
  }

  /**
   * Set the instruction to newly create the named cache.
   *
   * @param name Cache name
   * @return Self reference
   */
  public AbstractIndexDataProviderBuilder createCache(final String name) {
    this.persistenceBuilder.name(createCacheName(name));
    this.persistenceBuilder.make();
    return this;
  }

  /**
   * Instruction to try load the named cache and create it, if not found.
   *
   * @param name Cache name
   * @return Self reference
   */
  public AbstractIndexDataProviderBuilder loadOrCreateCache(final String name) {
    this.persistenceBuilder.name(createCacheName(name));
    this.persistenceBuilder.makeOrGet();
    return this;
  }

  /**
   * Set a list of stopwords to use by this instance.
   *
   * @param words List of stopwords. May be empty.
   * @return self reference
   */
  public AbstractIndexDataProviderBuilder stopwords(final Set<String> words) {
    if (words == null) {
      throw new IllegalArgumentException("Stopwords were null.");
    }
    this.stopwords = words;
    this.persistenceBuilder.stopwords(stopwords);
    return this;
  }

  /**
   * Set a list of document fields to use by this instance.
   *
   * @param fields List of field names. May be empty.
   * @return self reference
   */
  public AbstractIndexDataProviderBuilder documentFields(
      final Set<String> fields) {
    if (fields == null) {
      throw new IllegalArgumentException("Fields were null.");
    }
    this.documentFields = fields;
    this.persistenceBuilder.documentFields(fields);
    return this;
  }

  /**
   * Set the instance a being temporary.
   *
   * @return self reference
   */
  public AbstractIndexDataProviderBuilder temporary() {
    this.isTemporary = true;
    return this;
  }

  /**
   * Set and validate the Lucene index directory.
   *
   * @param filePath Path to the Lucene index
   * @return self reference
   * @throws IOException Thrown, if the path is not a directory, no Lucene index
   * was found in the directory or if reading from this directory is not
   * allowed.
   */
  public AbstractIndexDataProviderBuilder indexPath(final String filePath)
      throws IOException {
    if (filePath == null || filePath.trim().isEmpty()) {
      throw new IllegalArgumentException("Index path was empty.");
    }

    final File idxDir = new File(filePath);
    if (idxDir.exists()) {
      // check, if path is a directory
      if (!idxDir.isDirectory()) {
        throw new IOException("Index path '" + idxDir.getCanonicalPath()
            + "' exists, but is not a directory.");
      }
      // check, if there's a Lucene index in the path
      this.luceneDir = FSDirectory.open(idxDir);
      if (!DirectoryReader.indexExists(this.luceneDir)) {
        throw new IOException("No index found at index path '" + idxDir
            .getCanonicalPath() + "'.");
      }
    } else {
      // path does not exist
      throw new IOException(
          "Index path '" + idxDir.getCanonicalPath() + "' does " +
              "not exist."
      );
    }
    if (!idxDir.canRead()) {
      throw new IOException("Insufficient rights for index directory '"
          + idxDir.getCanonicalPath() + "'.");
    }

    this.idxPath = idxDir;
    return this;
  }

  /**
   * Set and validate the working directory.
   *
   * @param filePath Path to store working data
   * @return self reference
   * @throws IOException Thrown, if the path is not a directory, if the path
   * does not exist an could not be created or if reading/writing to this
   * directory is not allowed.
   * @see Persistence#tryCreateDataPath(String)
   */
  public final AbstractIndexDataProviderBuilder dataPath(
      final String filePath)
      throws IOException {
    this.dataPath = null;
    this.dataPath = Persistence.tryCreateDataPath(filePath);
    this.persistenceBuilder.dataPath(FileUtils.getPath(this.dataPath));
    return this;
  }

  /**
   * Set the {@link IndexReader} to access the Lucene index.
   *
   * @param reader
   * @return
   */
  public AbstractIndexDataProviderBuilder indexReader(final IndexReader
      reader) {
    if (reader == null) {
      throw new IllegalArgumentException("IndexReader was null.");
    }
    this.idxReader = reader;
    return this;
  }

  public AbstractIndexDataProviderBuilder warmup() {
    this.doWarmUp = true;
    return this;
  }

  /**
   * Validates the settings for the {@link Persistence} storage.
   *
   * @throws Buildable.BuilderConfigurationException Thrown, if any mandatory
   * configuration is not set
   */
  public void validatePersistenceBuilder()
      throws Buildable.BuilderConfigurationException {
    if (this.dataPath == null) {
      throw new Buildable.BuilderConfigurationException("No data-path set.");
    }
  }

  @Override
  public void validate()
      throws Buildable.BuilderConfigurationException, IOException {
    // index reader
    if (this.idxReader == null && this.luceneDir == null) {
      throw new IllegalStateException("No IndexReader and no index path was " +
          "set. Could not open an IndexReader.");
    }
    this.idxReader = DirectoryReader.open(this.luceneDir);

    this.lastCommitGeneration = SegmentInfos.getLastCommitGeneration(this
        .luceneDir);
  }
}
