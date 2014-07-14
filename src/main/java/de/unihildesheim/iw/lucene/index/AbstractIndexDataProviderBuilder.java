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
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Abstract builder to create an {@link IndexDataProvider} instance.
 *
 * @author Jens Bertram
 */
public abstract class AbstractIndexDataProviderBuilder<T extends
    AbstractIndexDataProviderBuilder<T>>
    implements Buildable {

  /**
   * Builder used to create a proper caching backend.
   */
  @SuppressWarnings("PackageVisibleField")
  final Persistence.Builder persistenceBuilder = new Persistence.Builder();
  /**
   * Implementation identifier used for proper cache naming.
   */
  private final String identifier;
  /**
   * List of stopwords to use.
   */
  @SuppressWarnings("PackageVisibleField")
  Set<String> stopwords = Collections.emptySet();
  /**
   * List of document fields to use.
   */
  @SuppressWarnings("PackageVisibleField")
  Set<String> documentFields = Collections.emptySet();
  /**
   * Flag indicating, if the new instance will be temporary. How to handle this
   * state is up to the specific implementation.
   */
  @SuppressWarnings("PackageVisibleField")
  boolean isTemporary;
  /**
   * {@link IndexReader} to use for accessing the Lucene index.
   */
  @SuppressWarnings("PackageVisibleField")
  IndexReader idxReader;
  /**
   * Warm-up the instance right after building it?
   */
  @SuppressWarnings("PackageVisibleField")
  boolean doWarmUp;

  /**
   * Last commit generation of the Lucene index (if it's a {@link Directory}
   * index). May be {@code null}.
   */
  @SuppressWarnings("PackageVisibleField")
  Long lastCommitGeneration;

  /**
   * File path where the working data will be stored.
   */
  @SuppressWarnings("PackageVisibleField")
  File dataPath;

  /**
   * (File-)Name of the cache to create.
   */
  @SuppressWarnings("PackageVisibleField")
  String cacheName;

  /**
   * {@link Directory} instance pointing at the Lucene index.
   */
  private Directory luceneDir;

  /**
   * Constructor setting the implementation identifier for the cache.
   *
   * @param newIdentifier Implementation identifier for the cache
   */
  AbstractIndexDataProviderBuilder(final String newIdentifier) {
    if (Objects.requireNonNull(newIdentifier, "Identifier was null.").isEmpty
        ()) {
      throw new IllegalArgumentException("Empty identifier name.");
    }
    this.identifier = newIdentifier;
  }

  /**
   * Instruction to load the named cache.
   *
   * @param name Cache name
   * @return Self reference
   */
  public final T loadCache(final String name) {
    this.cacheName = name;
    this.persistenceBuilder.name(createCacheName(name));
    this.persistenceBuilder.get();
    return getThis();
  }

  /**
   * Create a cache name prefixed with the identifier of the implementing
   * class.
   *
   * @param name Cache name
   * @return Cache name prefixed with current identifier
   */
  final String createCacheName(final String name) {
    return createCacheName(this.identifier, name);
  }

  /**
   * Get a self-reference of the implementing class.
   *
   * @return Self reference of implementing class instance
   */
  abstract T getThis();

  public static final String createCacheName(final String identifier, final
  String name) {
    if (StringUtils.isStrippedEmpty(Objects.requireNonNull(name,
        "Cache name was null."))) {
      throw new IllegalArgumentException("Empty cache name.");
    }
    if (StringUtils.isStrippedEmpty(Objects.requireNonNull(identifier,
        "Identifier was null."))) {
      throw new IllegalArgumentException("Empty identifier name.");
    }
    return identifier + "_" + name;
  }

  /**
   * Set the instruction to newly create the named cache.
   *
   * @param name Cache name
   * @return Self reference
   */
  public final T createCache(final String name) {
    this.cacheName = name;
    this.persistenceBuilder.name(createCacheName(name));
    this.persistenceBuilder.make();
    return getThis();
  }

  /**
   * Instruction to try load the named cache and create it, if not found.
   *
   * @param name Cache name
   * @return Self reference
   */
  public final T loadOrCreateCache(final String name) {
    this.cacheName = name;
    this.persistenceBuilder.name(createCacheName(name));
    this.persistenceBuilder.makeOrGet();
    return getThis();
  }

  /**
   * Set a list of stopwords to use by this instance.
   *
   * @param words List of stopwords. May be empty.
   * @return self reference
   */
  public final T stopwords(final Set<String> words) {
    this.stopwords = Objects.requireNonNull(words);
    this.persistenceBuilder.stopwords(this.stopwords);
    return getThis();
  }

  /**
   * Set a list of document fields to use by this instance.
   *
   * @param fields List of field names. May be empty.
   * @return self reference
   */
  public final T documentFields(
      final Set<String> fields) {
    Objects.requireNonNull(fields, "Field were null.");
    this.documentFields = new HashSet<>(fields);
    this.persistenceBuilder.documentFields(this.documentFields);
    return getThis();
  }

  /**
   * Set the instance a being temporary.
   *
   * @return self reference
   */
  public final T temporary() {
    this.isTemporary = true;
    return getThis();
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
  public final T indexPath(final String filePath)
      throws IOException {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(filePath, "Index path was null"))) {
      throw new IllegalArgumentException("Index path was empty.");
    }

    final File newIdxDir = new File(filePath);
    if (newIdxDir.exists()) {
      // check, if path is a directory
      if (!newIdxDir.isDirectory()) {
        throw new IOException("Index path '" + newIdxDir.getCanonicalPath()
            + "' exists, but is not a directory.");
      }
      // check, if there's a Lucene index in the path
      this.luceneDir = FSDirectory.open(newIdxDir);
      if (!DirectoryReader.indexExists(this.luceneDir)) {
        throw new IOException("No index found at index path '" + newIdxDir
            .getCanonicalPath() + "'.");
      }
    } else {
      // path does not exist
      throw new IOException(
          "Index path '" + newIdxDir.getCanonicalPath() + "' does " +
              "not exist."
      );
    }
    if (!newIdxDir.canRead()) {
      throw new IOException("Insufficient rights for index directory '"
          + newIdxDir.getCanonicalPath() + "'.");
    }

    return getThis();
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
  @SuppressWarnings("AssignmentToNull")
  public final T dataPath(final String filePath)
      throws IOException {
    this.dataPath = null;
    this.dataPath = Persistence.tryCreateDataPath(filePath);
    this.persistenceBuilder.dataPath(FileUtils.getPath(this.dataPath));
    return getThis();
  }

  /**
   * Set the {@link IndexReader} to access the Lucene index.
   *
   * @param reader Reader to access the Lucene index
   * @return Self reference
   */
  public final T indexReader(final IndexReader reader) {
    this.idxReader = Objects.requireNonNull(reader, "IndexReader was null.");
    return getThis();
  }

  /**
   * Instruct the instance to pre-load (warmUp) caches after initialization.
   *
   * @return Self reference
   */
  public final T warmUp() {
    this.doWarmUp = true;
    return getThis();
  }

  /**
   * Validates the settings for the {@link Persistence} storage.
   *
   * @throws ConfigurationException Thrown, if any mandatory configuration is
   * not set
   */
  public final void validatePersistenceBuilder()
      throws ConfigurationException {
    if (this.dataPath == null) {
      throw new ConfigurationException("No data-path set.");
    }
  }

  @Override
  public void validate()
      throws ConfigurationException {
    // index reader
    if (this.idxReader == null) {
      if (this.luceneDir == null) {
        throw new IllegalStateException(
            "No IndexReader and no index path was set. Could not open an " +
                "IndexReader."
        );
      }
      try {
        this.idxReader = DirectoryReader.open(this.luceneDir);
      } catch (final IOException e) {
        throw new ConfigurationException("Filed to open Lucene index.", e);
      }
    }

    if (this.idxReader instanceof DirectoryReader) {
      this.luceneDir = ((DirectoryReader) this.idxReader).directory();
      try {
        this.lastCommitGeneration = SegmentInfos.getLastCommitGeneration(this
            .luceneDir);
        this.persistenceBuilder.lastCommitGeneration(this.lastCommitGeneration);
      } catch (final IOException e) {
        throw new ConfigurationException("Filed to get Lucene segment " +
            "information.", e);
      }
    }
  }
}
