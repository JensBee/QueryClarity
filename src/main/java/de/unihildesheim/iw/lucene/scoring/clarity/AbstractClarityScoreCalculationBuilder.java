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

package de.unihildesheim.iw.lucene.scoring.clarity;

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.util.FileUtils;
import org.apache.lucene.index.IndexReader;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * @author Jens Bertram
 */
public abstract class AbstractClarityScoreCalculationBuilder<T extends
    AbstractClarityScoreCalculationBuilder<T>>
    implements Buildable {
  protected Persistence.Builder persistenceBuilder = new Persistence.Builder();

  /**
   * Implementation identifier used for proper cache naming.
   */
  private final String identifier;

  /**
   * {@link IndexDataProvider} to use.
   */
  protected IndexDataProvider idxDataProvider;

  /**
   * {@link IndexReader} to access the Lucene index.
   */
  protected IndexReader idxReader;

  /**
   * Flag indicating, if the new instance will be temporary.
   */
  protected boolean isTemporary;

  /**
   * File path where the working data will be stored.
   */
  private File dataPath;

  protected abstract T getThis();

  /**
   * Constructor setting the implementation identifier for the cache.
   *
   * @param newIdentifier Implementation identifier for the cache
   */
  protected AbstractClarityScoreCalculationBuilder(final String newIdentifier) {
    if (Objects.requireNonNull(newIdentifier).trim().isEmpty()) {
      throw new IllegalArgumentException("Identifier was empty.");
    }
    this.identifier = newIdentifier;
  }

  /**
   * Set the {@link IndexDataProvider} to use by this instance.
   *
   * @param dataProv Data provider
   * @return Self reference
   */
  public T indexDataProvider(final IndexDataProvider dataProv) {
    this.idxDataProvider = Objects.requireNonNull(dataProv);
    return getThis();
  }

  public T indexReader(final IndexReader newIdxReader) {
    this.idxReader = Objects.requireNonNull(newIdxReader);
    return getThis();
  }

  /**
   * Set the instance a being temporary.
   *
   * @return self reference
   */
  public T temporary()
      throws IOException {
    this.isTemporary = true;
    return getThis();
  }

  /**
   * Create a cache name prefixed with the identifier of the implementing
   * class.
   *
   * @param name Cache name
   * @return Cache name prefixed with current identifier
   */
  private String createCacheName(final String name) {
    if (Objects.requireNonNull(name).trim().isEmpty()) {
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
  public T loadCache(final String name) {
    this.persistenceBuilder.name(createCacheName(name));
    this.persistenceBuilder.get();
    return getThis();
  }

  /**
   * Set the instruction to newly create the named cache.
   *
   * @param name Cache name
   * @return Self reference
   */
  public T createCache(final String name) {
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
  public T loadOrCreateCache(
      final String name) {
    this.persistenceBuilder.name(createCacheName(name));
    this.persistenceBuilder.makeOrGet();
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
  public final T dataPath(final String filePath)
      throws IOException {
    Objects.requireNonNull(filePath);
    this.dataPath = null;
    this.dataPath = Persistence.tryCreateDataPath(filePath);
    this.persistenceBuilder.dataPath(FileUtils.getPath(this.dataPath));
    return getThis();
  }

  /**
   * Validates the settings for the {@link Persistence} storage.
   *
   * @throws ConfigurationException Thrown, if any mandatory configuration is
   * not set
   */
  public void validatePersistenceBuilder()
      throws ConfigurationException {
    if (this.dataPath == null) {
      throw new ConfigurationException("No data-path set.");
    }
  }

  @Override
  public void validate()
      throws ConfigurationException {
    if (this.idxReader == null) {
      throw new ConfigurationException("No IndexReader" +
          " set.");
    }
    if (this.idxDataProvider == null) {
      throw new ConfigurationException("No IndexDataProvider" +
          " set.");
    }
  }
}
