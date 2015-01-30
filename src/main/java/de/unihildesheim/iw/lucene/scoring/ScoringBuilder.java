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

package de.unihildesheim.iw.lucene.scoring;

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.VocabularyProvider;
import de.unihildesheim.iw.util.Configuration;
import de.unihildesheim.iw.util.FileUtils;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * General builder interface for all scoring types. Should provide methods that
 * satisfies all scoring implementations to provide a general interface.
 * Specific implementations may override {@link ScoringBuilderBase} to provide
 * methods as needed. A basic {@link Validator} is also provided.
 *
 * @author Jens Bertram
 */
public interface ScoringBuilder<T extends ScoringBuilder,
    C extends Configuration> {

  /**
   * Return a self reference of the implementing class.
   *
   * @return Self reference
   */
  T getThis();

  /**
   * Set the {@link IndexDataProvider} to use by this instance.
   *
   * @param dataProv Data provider
   * @return Self reference
   */
  T indexDataProvider(final IndexDataProvider dataProv);

  /**
   * Get the {@link IndexDataProvider} to use by this instance.
   *
   * @return Index data provider instance
   */
  IndexDataProvider getIndexDataProvider();

  /**
   * Set the analyzer to use for parsing queries.
   *
   * @param analyzer Analyzer
   * @return Self reference
   */
  T analyzer(final Analyzer analyzer);

  /**
   * Get the analyzer to use for parsing queries.
   *
   * @return Analyzer
   */
  Analyzer getAnalyzer();

  /**
   * Set the reader to access the Lucene index.
   *
   * @param indexReader Reader
   * @return Self reference
   */
  T indexReader(final IndexReader indexReader);

  /**
   * Get the reader to access the Lucene index.
   *
   * @return Index reader
   */
  IndexReader getIndexReader();

  /**
   * Instruction to load the named cache.
   *
   * @param cacheName Cache name
   * @return Self reference
   */
  T loadCache(final String cacheName);

  /**
   * Create a cache name prefixed with the identifier of the implementing
   * class.
   *
   * @param cacheName Cache name
   * @return Self reference
   */
  T createCache(final String cacheName);

  /**
   * Instruction to try load the named cache and create it, if not found.
   *
   * @param cacheName Cache name
   * @return Self reference
   */
  T loadOrCreateCache(final String cacheName);

  /**
   * Get the name of the cache to use.
   *
   * @return Cache name
   */
  String getCacheName();

  /**
   * Get the {@link CacheInstruction} telling how to load/create the cache.
   *
   * @return Instruction
   */
  CacheInstruction getCacheInstruction();

  /**
   * Set the working directory.
   *
   * @param dataPath Directory name
   * @return Self reference
   * @throws IOException Thrown, if the path could not be found, created or
   * written to
   */
  T dataPath(final String dataPath)
      throws IOException;

  /**
   * Get the working directory.
   *
   * @return Working directory
   */
  File getDataPath();

  /**
   * Set the instance as being temporary.
   *
   * @return Self reference
   */
  T temporary();

  /**
   * Get the flag indicating, if this instance is temporary.
   *
   * @return True, if temporary
   */
  boolean isTemporary();

  /**
   * Set the {@link Configuration].
   *
   * @param configuration Configuration to use
   * @return Self reference
   */
  T configuration(C configuration);

  /**
   * Get the current {@link Configuration].
   *
   * @return Configuration
   */
  C getConfiguration();

  /**
   * Get the identifier of this instance.
   *
   * @return Instance identifier
   */
  String getIdentifier();

  /**
   * Get the cache builder instance.
   *
   * @return Cache builder instance
   */
  Persistence.Builder getCache();

  /**
   * Set the provider for feedback documents.
   *
   * @param feedbackProvider Feedback documents provider
   * @return Self reference
   */
  T feedbackProvider(final FeedbackProvider feedbackProvider);

  /**
   * Get the provider for feedback documents.
   *
   * @return Feedback documents provider
   */
  FeedbackProvider getFeedbackProvider();

  /**
   * Set the provider for feedback vocabulary.
   *
   * @param vocabularyProvider Feedback vocabulary provider
   * @return Self reference
   */
  T vocabularyProvider(final VocabularyProvider vocabularyProvider);

  /**
   * Get the provider for feedback vocabulary.
   *
   * @return Feedback vocabulary provider
   */
  VocabularyProvider getVocabularyProvider();

  /**
   * Provides constants for features that may be provided by specific
   * implementations overriding {@link ScoringBuilderBase}. Used to feed the
   * {@link Validator}.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum Feature {
    /**
     * Implementation makes use of an {@link Analyzer}.
     */
    ANALYZER,
    /**
     * Implementation makes use of a cache.
     */
    CACHE,
    /**
     * Implementation makes use of a {@link Configuration}.
     */
    CONFIGURATION,
    /**
     * Implementation makes use of a dedicated working directory.
     */
    DATA_PATH,
    /**
     * Implementation makes use of an {@link IndexDataProvider}.
     */
    DATA_PROVIDER,
    /**
     * Implementation makes use of a {@link FeedbackProvider}.
     */
    FB_PROVIDER,
    /**
     * Implementation makes use of an {@link IndexReader}.
     */
    INDEX_READER,
    /**
     * Implementation makes use of a {@link VocabularyProvider}.
     */
    VOC_PROVIDER
  }

  /**
   * Possible instructions on how to handle a named cache.
   */
  @SuppressWarnings("PublicInnerClass")
  public enum CacheInstruction {
    /**
     * Create a new cache. Should fail, if a cache with the given name already
     * exists.
     */
    CREATE,
    /**
     * Loads a cache. Should fail, if no cache with the given name exists.
     */
    LOAD,
    /**
     * Loads an existing or creates a new cache. Should automatically create a
     * new cache, if no cache with the given name exists.
     */
    LOAD_OR_CREATE
  }

  /**
   * Base class implementing all methods of {@link ScoringBuilder}. All set
   * methods simply ignore the passed in values and return a self reference. All
   * get operations return {@code null}. Specific implementations should
   * override this class and implement methods as needed.
   *
   * @param <I> Specific {@link ScoringBuilderBase} implementation
   * @param <C> Implementation specific {@link Configuration} object
   */
  @SuppressWarnings({"ReturnOfNull", "PublicInnerClass"})
  public abstract class ScoringBuilderBase<I extends ScoringBuilderBase,
      C extends Configuration>
      implements ScoringBuilder<I, C> {

    /**
     * Implementation identifier used for proper cache naming.
     */
    private final String identifier;
    /**
     * Data provider for index related term-data.
     */
    private IndexDataProvider dataProv;
    /**
     * Analyzer to parse queries & terms.
     */
    private Analyzer analyzer;
    /**
     * Reader to access Lucene index.
     */
    private IndexReader indexReader;
    /**
     * Builder for persistent caches.
     */
    private volatile Persistence.Builder persistenceBuilder;
    /**
     * Name of the cache to create.
     */
    private String cacheName;
    /**
     * Path to store working data.
     */
    @Nullable
    private File dataPath;
    /**
     * If true, instance should provide a temporary state.
     */
    private boolean temporary;
    /**
     * Implementation specific {@link Configuration} object
     */
    private C configuration;
    /**
     * Provider for feedback documents.
     */
    private FeedbackProvider feedbackProvider;
    /**
     * Provider for feedback vocabulary.
     */
    private VocabularyProvider vocabularyProvider;

    /**
     * Initialize the builder with the given implementation identifier.
     *
     * @param newIdentifier Implementation identifier
     */
    protected ScoringBuilderBase(final String newIdentifier) {
      this.identifier = newIdentifier;
    }

    @Override
    public I indexDataProvider(final IndexDataProvider newDataProv) {
      this.dataProv = newDataProv;
      return getThis();
    }

    @Override
    public IndexDataProvider getIndexDataProvider() {
      return this.dataProv;
    }

    @Override
    public I analyzer(final Analyzer newAnalyzer) {
      this.analyzer = newAnalyzer;
      return getThis();
    }

    @Override
    public Analyzer getAnalyzer() {
      return this.analyzer;
    }

    @Override
    public I indexReader(final IndexReader newIndexReader) {
      this.indexReader = newIndexReader;
      return getThis();
    }

    @Override
    public IndexReader getIndexReader() {
      return this.indexReader;
    }

    @Override
    public I loadCache(final String newCacheName) {
      this.cacheName = createCacheName(newCacheName);
      getPBuilder().name(this.cacheName);
      getPBuilder().makeOrGet();
      return getThis();
    }

    @Override
    public I createCache(final String newCacheName) {
      this.cacheName = createCacheName(newCacheName);
      getPBuilder().name(this.cacheName);
      getPBuilder().make();
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
     * Get or initializes the persistent storage provider.
     *
     * @return Bulder interface for persistent storage provider
     */
    private Persistence.Builder getPBuilder() {
      if (this.persistenceBuilder == null) {
        this.persistenceBuilder = new Persistence.Builder();
      }
      return this.persistenceBuilder;
    }

    @Override
    public I loadOrCreateCache(final String newCacheName) {
      this.cacheName = createCacheName(newCacheName);
      getPBuilder().name(this.cacheName);
      getPBuilder().makeOrGet();
      return getThis();
    }

    @Override
    public String getCacheName() {
      return this.cacheName;
    }

    @Override
    public CacheInstruction getCacheInstruction() {
      if (this.persistenceBuilder == null) {
        return null;
      }
      switch (this.persistenceBuilder.getCacheLoadInstruction()) {
        case GET:
          return CacheInstruction.LOAD;
        case MAKE:
          return CacheInstruction.CREATE;
        case MAKE_OR_GET:
          return CacheInstruction.LOAD_OR_CREATE;
        default:
          return null;
      }
    }

    @Override
    public I dataPath(final String newDataPath)
        throws IOException {
      Objects.requireNonNull(newDataPath, "Data-path was null.");
      this.dataPath = null;
      this.dataPath = Persistence.tryCreateDataPath(newDataPath);
      getPBuilder().dataPath(FileUtils.getPath(this.dataPath));
      return getThis();
    }

    @Nullable
    @Override
    public File getDataPath() {
      return this.dataPath;
    }

    @Override
    public I temporary() {
      this.temporary = true;
      return getThis();
    }

    @Override
    public boolean isTemporary() {
      return this.temporary;
    }

    @Override
    public I configuration(final C newConfiguration) {
      this.configuration = newConfiguration;
      return getThis();
    }

    @Override
    public C getConfiguration() {
      return this.configuration;
    }

    @Override
    public String getIdentifier() {
      return this.identifier;
    }

    @Override
    public Persistence.Builder getCache() {
      return this.persistenceBuilder;
    }

    @Override
    public I feedbackProvider(final FeedbackProvider newFeedbackProvider) {
      this.feedbackProvider = newFeedbackProvider;
      return getThis();
    }

    @Override
    public FeedbackProvider getFeedbackProvider() {
      return this.feedbackProvider;
    }

    @Override
    public I vocabularyProvider(final VocabularyProvider
        newVocabularyProvider) {
      this.vocabularyProvider = newVocabularyProvider;
      return getThis();
    }

    @Override
    public VocabularyProvider getVocabularyProvider() {
      return this.vocabularyProvider;
    }
  }

  /**
   * Basic validator for all possible {@link Feature}s that may be implemented
   * by specific {@link ScoringBuilderBase} implementations. Provides only basic
   * validation by checking the returned values for not being {@code null}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class Validator {
    /**
     * Run basic checks for all supported features by the specific specific
     * {@link ScoringBuilderBase} implementation. Only basic validation by
     * checking the returned values for not being {@code null} is provided. If a
     * value evaluates to {@code null} a {@link Buildable
     * .ConfigurationException} is thrown.
     *
     * @param sb ScoringBuilder implementation instance
     * @param features Features to check
     * @throws Buildable.ConfigurationException Thrown, if any value of the
     * provided {@link Feature}s evaluates to {@code null}.
     */
    public Validator(final ScoringBuilder sb,
        final Feature[] features)
        throws Buildable.ConfigurationException {

      if (sb.getIdentifier() == null) {
        throw new Buildable.ConfigurationException("No identifier set.");
      }

      for (final Feature f : features) {
        switch (f) {
          case ANALYZER:
            if (sb.getAnalyzer() == null) {
              throw new Buildable.ConfigurationException("No analyzer set.");
            }
            break;
          case CACHE:
            if (sb.getCacheName() == null) {
              throw new Buildable.ConfigurationException(
                  "No cache name set.");
            }
            if (sb.getCacheInstruction() == null) {
              throw new Buildable.ConfigurationException(
                  "No cache instruction set.");
            }
            if (sb.getCache() == null) {
              throw new Buildable.ConfigurationException(
                  "No cache builder set.");
            }
            break;
          case CONFIGURATION:
            if (sb.getConfiguration() == null) {
              throw new Buildable.ConfigurationException(
                  "No configuration set.");
            }
            break;
          case DATA_PATH:
            if (sb.getDataPath() == null) {
              throw new Buildable.ConfigurationException(
                  "No data path set.");
            }
            break;
          case DATA_PROVIDER:
            if (sb.getIndexDataProvider() == null) {
              throw new Buildable.ConfigurationException(
                  "No indexDataProvider set.");
            }
            break;
          case FB_PROVIDER:
            if (sb.getFeedbackProvider() == null) {
              throw new Buildable.ConfigurationException(
                  "No feedbackProvider set.");
            }
            break;
          case INDEX_READER:
            if (sb.getIndexReader() == null) {
              throw new Buildable.ConfigurationException(
                  "No indexReader set.");
            }
            break;
          case VOC_PROVIDER:
            if (sb.getVocabularyProvider() == null) {
              throw new Buildable.ConfigurationException(
                  "No vocabularyProvider set.");
            }
            break;
        }
      }
    }
  }
}
