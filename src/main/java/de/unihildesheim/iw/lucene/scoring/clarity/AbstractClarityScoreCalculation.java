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
import org.jetbrains.annotations.Nullable;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public abstract class AbstractClarityScoreCalculation
    implements ClarityScoreCalculation {

  private final String id;

  AbstractClarityScoreCalculation(final String identifier) {
    this.id = identifier;
  }

  @Override
  public String getIdentifier() {
    return this.id;
  }

  public abstract static class AbstractBuilder<
      S extends AbstractClarityScoreCalculation,
      B extends AbstractBuilder> {
    /**
     * Provides constants for features that may be provided by specific
     * implementations.
     */
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

    public abstract S build()
        throws BuildableException;

    abstract B getThis();

    /**
     * Check, if all required features have a value set. Does not check the
     * integrity of the values. NOTE: CONFIGURATION feature is NOT checked.
     *
     * @param features Features to check
     * @throws ConfigurationException Thrown, if a feature is not configured
     */
    final B validateFeatures(final Feature[] features)
        throws ConfigurationException {
      for (final Feature f : features) {
        boolean fail = false;
        switch (f) {
          case ANALYZER: fail = this.analyzer == null; break;
          case DATA_PROVIDER: fail = this.dataProv == null; break;
          case INDEX_READER: fail = this.indexReader == null; break;
        }
        if (fail) {
          throw new ConfigurationException("Feature '" + f.name() + "' not " +
              "configured.");
        }
      }
      return getThis();
    }

    final B validateConfiguration(final Class<? extends Configuration> c)
        throws ConfigurationException {
      if (this.conf == null) {
        throw new ConfigurationException("Configuration not set.");
      }
      if (!c.equals(this.conf.getClass())) {
        throw new ConfigurationException("Wrong configuration format " +
            "specified. Expecting '" + c.getCanonicalName() + "'");
      }
      return getThis();
    }

    public final B configuration(final Configuration configuration) {
      this.conf = configuration;
      return getThis();
    }

    Configuration getConfiguration() {
      return this.conf;
    }

    public final B indexDataProvider(final IndexDataProvider newDataProv) {
      this.dataProv = newDataProv;
      return getThis();
    }

    final IndexDataProvider getIndexDataProvider() {
      return this.dataProv;
    }

    public final B indexReader(final IndexReader newIndexReader) {
      this.indexReader = newIndexReader;
      return getThis();
    }

    @Nullable
    final IndexReader getIndexReader() {
      return this.indexReader;
    }

    public final B feedbackProvider(
        final FeedbackProvider newFeedbackProvider) {
      this.feedbackProvider = newFeedbackProvider;
      return getThis();
    }

    final FeedbackProvider getFeedbackProvider() {
      if (this.feedbackProvider == null) {
        return new DefaultFeedbackProvider();
      }
      return this.feedbackProvider;
    }

    public final B vocabularyProvider(final VocabularyProvider
        newVocabularyProvider) {
      this.vocabularyProvider = newVocabularyProvider;
      return getThis();
    }

    final VocabularyProvider getVocabularyProvider() {
      if (this.vocabularyProvider == null) {
        return new DefaultVocabularyProvider();
      }
      return this.vocabularyProvider;
    }

    public final B analyzer(final Analyzer newAnalyzer) {
      this.analyzer = newAnalyzer;
      return getThis();
    }

    @Nullable
    final Analyzer getAnalyzer() {
      return this.analyzer;
    }
  }
}
