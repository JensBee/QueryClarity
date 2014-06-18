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
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
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
  /**
   * Wrapped builder to create the persistent data storage.
   */
  @SuppressWarnings("PackageVisibleField")
  final Persistence.Builder persistenceBuilder = new Persistence.Builder();
  /**
   * Implementation identifier used for proper cache naming.
   */
  private final String identifier;
  /**
   * {@link IndexDataProvider} to use.
   */
  @SuppressWarnings("PackageVisibleField")
  IndexDataProvider idxDataProvider;

  /**
   * {@link IndexReader} to access the Lucene index.
   */
  @SuppressWarnings("PackageVisibleField")
  IndexReader idxReader;

  /**
   * Flag indicating, if the new instance will be temporary.
   */
  @SuppressWarnings("PackageVisibleField")
  boolean isTemporary;

  /**
   * Analyzer to use for parsing queries.
   */
  @SuppressWarnings("PackageVisibleField")
  Analyzer analyzer;

  /**
   * File path where the working data will be stored.
   */
  private File dataPath;

  /**
   * Constructor setting the implementation identifier for the cache.
   *
   * @param newIdentifier Implementation identifier for the cache
   */
  AbstractClarityScoreCalculationBuilder(final String newIdentifier) {
    if (Objects.requireNonNull(newIdentifier, "Identifier was null.").trim()
        .isEmpty()) {
      throw new IllegalArgumentException("Identifier was empty.");
    }
    this.identifier = newIdentifier;
  }

  /**
   * Build the {@link ClarityScoreCalculation} instance.
   *
   * @return New instance
   * @throws BuildableException Thrown, if building the instance fails
   */
  @Override
  public abstract ClarityScoreCalculation build()
      throws BuildableException;

  @SuppressWarnings("CanBeFinal")
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
    if (this.analyzer == null) {
      throw new ConfigurationException("No query analyzer set.");
    }
  }

  /**
   * Set the {@link IndexDataProvider} to use by this instance.
   *
   * @param dataProv Data provider
   * @return Self reference
   */
  public final T indexDataProvider(final IndexDataProvider dataProv) {
    this.idxDataProvider = Objects.requireNonNull(dataProv,
        "IndexDataProvider was null.");
    return getThis();
  }

  /**
   * Get a self reference.
   *
   * @return Self reference
   */
  abstract T getThis();

  /**
   * Set the reader to access the Lucene index.
   *
   * @param newIdxReader Reader
   * @return Self reference
   */
  public final T indexReader(final IndexReader newIdxReader) {
    this.idxReader = Objects.requireNonNull(newIdxReader,
        "IndexReader was null.");
    return getThis();
  }

  /**
   * Set the analyzer to use for parsing queries.
   *
   * @param newAnalyzer Analyzer
   * @return Self reference
   */
  public final T analyzer(final Analyzer newAnalyzer) {
    this.analyzer = newAnalyzer;
    return getThis();
  }

  /**
   * Set the instance a being temporary.
   *
   * @return Self reference
   */
  public final T temporary() {
    this.isTemporary = true;
    return getThis();
  }

  /**
   * Instruction to load the named cache.
   *
   * @param name Cache name
   * @return Self reference
   */
  public final T loadCache(final String name) {
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
  private String createCacheName(final String name) {
    if (StringUtils.isStrippedEmpty(Objects.requireNonNull(name,
        "Cache name was null."))) {
      throw new IllegalArgumentException("Empty cache name.");
    }
    return this.identifier + "_" + name;
  }

  /**
   * Set the instruction to newly create the named cache.
   *
   * @param name Cache name
   * @return Self reference
   */
  public final T createCache(final String name) {
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
  public final T loadOrCreateCache(
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
  @SuppressWarnings("AssignmentToNull")
  public final T dataPath(final String filePath)
      throws IOException {
    Objects.requireNonNull(filePath, "Data-path was null.");
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
  public final void validatePersistenceBuilder()
      throws ConfigurationException {
    if (this.dataPath == null) {
      throw new ConfigurationException("No data-path set.");
    }
  }
}
