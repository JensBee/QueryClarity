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

import java.io.File;
import java.io.IOException;

/**
 * @author Jens Bertram
 */
public abstract class AbstractClarityScoreCalculationBuilder<I extends
    AbstractClarityScoreCalculationBuilder, T>
    implements Buildable<T> {
  protected Persistence.Builder persistenceBuilder = new Persistence.Builder();

  /**
   * Implementation identifier used for proper cache naming.
   */
  private String identifier = null;

  /**
   * {@link IndexDataProvider} to use.
   */
  protected IndexDataProvider idxDataProvider = null;

  /**
   * Flag indicating, if the new instance will be temporary.
   */
  protected boolean isTemporary = false;

  /**
   * File path where the working data will be stored.
   */
  private File dataPath = null;

  /**
   * Constructor setting the implementation identifier for the cache.
   *
   * @param newIdentifier Implementation identifier for the cache
   */
  protected AbstractClarityScoreCalculationBuilder(final String newIdentifier) {
    if (newIdentifier == null || newIdentifier.isEmpty()) {
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
  public I indexDataProvider(
      final IndexDataProvider dataProv) {
    if (dataProv == null) {
      throw new IllegalArgumentException("Data provider was null.");
    }
    this.idxDataProvider = dataProv;
    return (I) this;
  }

  /**
   * Set the instance a being temporary.
   *
   * @return self reference
   */
  public I temporary()
      throws IOException {
    this.isTemporary = true;
    return (I) this;
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
  public I loadCache(final String name) {
    this.persistenceBuilder.name(createCacheName(name));
    this.persistenceBuilder.get();
    return (I) this;
  }

  /**
   * Set the instruction to newly create the named cache.
   *
   * @param name Cache name
   * @return Self reference
   */
  public I createCache(final String name) {
    this.persistenceBuilder.name(createCacheName(name));
    this.persistenceBuilder.make();
    return (I) this;
  }

  /**
   * Instruction to try load the named cache and create it, if not found.
   *
   * @param name Cache name
   * @return Self reference
   */
  public I loadOrCreateCache(
      final String name) {
    this.persistenceBuilder.name(createCacheName(name));
    this.persistenceBuilder.makeOrGet();
    return (I) this;
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
  public final I dataPath(
      final String filePath)
      throws IOException {
    this.dataPath = null;
    this.dataPath = Persistence.tryCreateDataPath(filePath);
    this.persistenceBuilder.dataPath(FileUtils.getPath(this.dataPath));
    return (I) this;
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
      throws BuilderConfigurationException {
    if (this.idxDataProvider == null) {
      throw new Buildable.BuilderConfigurationException("No IndexDataProvider" +
          " set.");
    }
  }
}
