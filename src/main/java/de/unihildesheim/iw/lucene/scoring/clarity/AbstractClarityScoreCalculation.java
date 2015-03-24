/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

import de.unihildesheim.iw.Buildable.BuildableException;
import de.unihildesheim.iw.Buildable.ConfigurationException;
import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.scoring.data.DefaultFeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.DefaultVocabularyProvider;
import de.unihildesheim.iw.lucene.scoring.data.FeedbackProvider;
import de.unihildesheim.iw.lucene.scoring.data.VocabularyProvider;
import de.unihildesheim.iw.util.Configuration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public abstract class AbstractClarityScoreCalculation
    implements ClarityScoreCalculation {

  /**
   * Scoring identifier.
   */
  private final String id;

  /**
   * Create a new instance using an identifier.
   * @param identifier Identifier of the implementation
   */
  AbstractClarityScoreCalculation(@NotNull final String identifier) {
    this.id = identifier;
  }

  @Override
  public String getIdentifier() {
    return this.id;
  }

  /**
   * Abstract builder for {@link ClarityScoreCalculation} builder classes.
   * @param <B> Builder instance type
   * @param <S> ClarityScoreCalculation instance type
   */
  @SuppressWarnings("PublicInnerClass")
  public abstract static class AbstractCSCBuilder<
      B extends AbstractCSCBuilder<B, S>, S extends ClarityScoreCalculation> {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(
        AbstractCSCBuilder.class);
    /**
     * Provides constants for features that may be provided by specific
     * implementations.
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    enum Feature {
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
     * Implementation specific {@link Configuration} object
     */
    @Nullable
    Configuration conf;
    /**
     * Data provider for index related term-data.
     */
    @Nullable
    private IndexDataProvider dataProv;
    /**
     * Reader to access Lucene index.
     */
    @Nullable
    private IndexReader indexReader;
    /**
     * Provider for feedback documents.
     */
    @Nullable
    private FeedbackProvider feedbackProvider;
    /**
     * Provider for feedback vocabulary.
     */
    @Nullable
    private VocabularyProvider vocabularyProvider;
    /**
     * Analyzer to parse queries & terms.
     */
    @Nullable
    private Analyzer analyzer;

    /**
     * Build the calculation instance.
     * @return New calculation instance
     * @throws BuildableException Thrown on errors encountered while building
     * the instance
     */
    public abstract S build()
        throws BuildableException;

    /**
     * Get a self reference to the implementing object-
     * @return Self reference
     */
    abstract B getThis();

    /**
     * Check, if all required features have a value set. Does not check the
     * integrity of the values. NOTE: CONFIGURATION feature is NOT checked.
     * Use {@link #validateConfiguration(Class)} if you need this feature.
     *
     * @param features Features to check
     * @throws ConfigurationException Thrown, if a feature is not configured
     */
    final void validateFeatures(
        @NotNull final Feature... features)
        throws ConfigurationException {
      for (final Feature f : features) {
        boolean fail = false;
        switch (f) {
          case ANALYZER:
            fail = this.analyzer == null;
            break;
          case CONFIGURATION:
            LOG.warn("Configuration cannot be validated. " +
                "Use dedicated method instead.");
            break;
          case DATA_PROVIDER:
            fail = this.dataProv == null;
            break;
          case INDEX_READER:
            fail = this.indexReader == null;
            break;
        }
        if (fail) {
          throw new ConfigurationException("Feature '" + f.name() + "' not " +
              "configured.");
        }
      }
    }

    /**
     * Validate the {@link Configuration} object.
     *
     * @param c Configuration suitable for this builder
     * @throws ConfigurationException Thrown if the currently set configuration
     * does not match the passed in class or no configuration is set
     */
    final void validateConfiguration(
        @NotNull final Class<? extends Configuration> c)
        throws ConfigurationException {
      if (this.conf == null) {
        throw new ConfigurationException("Configuration not set.");
      }
      if (!c.equals(this.conf.getClass())) {
        throw new ConfigurationException("Wrong configuration format " +
            "specified. Expecting '" + c.getCanonicalName() + '\'');
      }
    }

    /**
     * Set the {@link Configuration} object for this builder.
     *
     * @param configuration Configuration
     * @return Self reference
     */
    public final B configuration(
        @NotNull final Configuration configuration) {
      this.conf = configuration;
      return getThis();
    }

    /**
     * Get the {@link Configuration} object for this builder.
     *
     * @return Currently set {@link Configuration} or {@code null} if there was
     * none
     */
    @Nullable
    Configuration getConfiguration() {
      return this.conf;
    }

    /**
     * Set the {@link IndexDataProvider} to use by this builder.
     *
     * @param newDataProv IndexDataProvider
     * @return Self reference
     */
    public final B indexDataProvider(
        @NotNull final IndexDataProvider newDataProv) {
      this.dataProv = newDataProv;
      return getThis();
    }

    /**
     * Get the {@link IndexDataProvider} currently set.
     *
     * @return Currently set {@link IndexDataProvider} or {@code null} if there
     * was none
     */
    @Nullable
    final IndexDataProvider getIndexDataProvider() {
      return this.dataProv;
    }

    /**
     * Set the {@link IndexReader} to use by this builder.
     *
     * @param newIndexReader IndexReader
     * @return Self reference
     */
    public final B indexReader(
        @NotNull final IndexReader newIndexReader) {
      this.indexReader = newIndexReader;
      return getThis();
    }

    /**
     * Get the {@link IndexReader} currently set.
     *
     * @return Currently set {@link IndexReader} or {@code null} if there
     * was none
     */
    @Nullable
    final IndexReader getIndexReader() {
      return this.indexReader;
    }

    /**
     * Set the {@link FeedbackProvider} to use by this builder.
     *
     * @param newFeedbackProvider FeedbackProvider
     * @return Self reference
     */
    public final B feedbackProvider(
        @NotNull final FeedbackProvider newFeedbackProvider) {
      this.feedbackProvider = newFeedbackProvider;
      return getThis();
    }

    /**
     * Get the {@link FeedbackProvider} currently set.
     *
     * @return Currently set {@link FeedbackProvider} or {@code null} if there
     * was none
     */
    final FeedbackProvider getFeedbackProvider() {
      if (this.feedbackProvider == null) {
        return new DefaultFeedbackProvider();
      }
      return this.feedbackProvider;
    }

    /**
     * Set the {@link VocabularyProvider} to use by this builder.
     *
     * @param newVocabularyProvider VocabularyProvider
     * @return Self reference
     */
    public final B vocabularyProvider(
        @NotNull final VocabularyProvider newVocabularyProvider) {
      this.vocabularyProvider = newVocabularyProvider;
      return getThis();
    }

    /**
     * Get the {@link VocabularyProvider} currently set.
     *
     * @return Currently set {@link VocabularyProvider} or {@code null} if there
     * was none
     */
    final VocabularyProvider getVocabularyProvider() {
      if (this.vocabularyProvider == null) {
        return new DefaultVocabularyProvider();
      }
      return this.vocabularyProvider;
    }

    /**
     * Set the {@link Analyzer} to use by this builder.
     *
     * @param newAnalyzer Analyzer
     * @return Self reference
     */
    public final B analyzer(
        @NotNull final Analyzer newAnalyzer) {
      this.analyzer = newAnalyzer;
      return getThis();
    }

    /**
     * Get the {@link Analyzer} currently set.
     *
     * @return Currently set {@link Analyzer} or {@code null} if there
     * was none
     */
    @Nullable
    final Analyzer getAnalyzer() {
      return this.analyzer;
    }
  }
}
