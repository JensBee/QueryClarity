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
import de.unihildesheim.iw.Buildable.ConfigurationException;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.scoring.data.DefaultFeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.DefaultVocabularyProvider;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.VocabularyProvider;
import de.unihildesheim.iw.util.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;

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
     * Implementation makes use of a {@link Configuration}.
     */
    CONFIGURATION,
    /**
     * Implementation makes use of an {@link IndexDataProvider}.
     */
    DATA_PROVIDER,
    /**
     * Implementation makes use of an {@link IndexReader}.
     */
    INDEX_READER
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
    public I feedbackProvider(final FeedbackProvider newFeedbackProvider) {
      this.feedbackProvider = newFeedbackProvider;
      return getThis();
    }

    @Override
    public FeedbackProvider getFeedbackProvider() {
      if (this.feedbackProvider == null) {
        return new DefaultFeedbackProvider();
      }
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
      if (this.vocabularyProvider == null) {
        return new DefaultVocabularyProvider();
      }
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
     * @throws ConfigurationException Thrown, if any value of the
     * provided {@link Feature}s evaluates to {@code null}.
     */
    public Validator(final ScoringBuilder sb,
        final Feature[] features)
        throws ConfigurationException {

      if (sb.getIdentifier() == null) {
        throw new ConfigurationException("No identifier set.");
      }

      for (final Feature f : features) {
        switch (f) {
          case ANALYZER:
            if (sb.getAnalyzer() == null) {
              throw new ConfigurationException("No analyzer set.");
            }
            break;
          case CONFIGURATION:
            if (sb.getConfiguration() == null) {
              throw new ConfigurationException(
                  "No configuration set.");
            }
            break;
          case DATA_PROVIDER:
            if (sb.getIndexDataProvider() == null) {
              throw new ConfigurationException(
                  "No indexDataProvider set.");
            }
            break;
          case INDEX_READER:
            if (sb.getIndexReader() == null) {
              throw new ConfigurationException(
                  "No indexReader set.");
            }
            break;
        }
      }
    }
  }
}
