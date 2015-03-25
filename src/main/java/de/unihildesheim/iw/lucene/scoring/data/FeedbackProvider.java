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

package de.unihildesheim.iw.lucene.scoring.data;

import de.unihildesheim.iw.lucene.index.IndexDataProvider;
import de.unihildesheim.iw.lucene.query.RelaxableQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;

/**
 * @author Jens Bertram
 */
public interface FeedbackProvider {
  /**
   * Get a set of documents to use for feedback.
   *
   * @return Set of document ids
   * @throws Exception Any exception may be thrown by implementing class
   */
  DocIdSet get()
      throws Exception;

  /**
   * Set the query string to use for getting feedback documents.
   *
   * @param query Query string
   * @return Self reference
   */
  FeedbackProvider query(final String query);

  /**
   * Set the number of feedback documents to get.
   *
   * @param min Minimum number to get
   * @param max Maximum number to get
   * @return Self reference
   */
  FeedbackProvider amount(final int min, final int max);

  /**
   * Set the number of feedback documents to a fixed value.
   *
   * @param fixed Number of documents to get.
   * @return Self reference
   */
  FeedbackProvider amount(final int fixed);

  /**
   * Set the {@link IndexReader} that may be used to retrieve feedback documents
   * from the Lucene index.
   *
   * @param indexReader Reader to access the Lucene index
   * @return Self reference
   */
  FeedbackProvider indexReader(final IndexReader indexReader);

  /**
   * Set the {@link IndexDataProvider} that may be used to retrieve feedback
   * documents.
   *
   * @param indexDataProvider Data provider
   * @return Self reference
   */
  FeedbackProvider dataProvider(final IndexDataProvider indexDataProvider);

  /**
   * Set the {@link Analyzer} that may be used to parse a query string.
   *
   * @param analyzer Analyzer
   * @return Self reference
   */
  FeedbackProvider analyzer(final Analyzer analyzer);

  /**
   * Set the document fields to query.
   *
   * @param fields Set of document field names
   * @return Self reference
   */
  FeedbackProvider fields(final String... fields);

  /**
   * Set the query parser to use for getting feedback documents.
   *
   * @param rtq Query parser
   * @return Self reference
   */
  FeedbackProvider queryParser(final Class<? extends RelaxableQuery> rtq);
}
