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

import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.Tuple.Tuple2;
import de.unihildesheim.iw.lucene.scoring.ScoringResult;
import de.unihildesheim.iw.lucene.util.StreamUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Wrapper class enclosing the results of a clarity score calculation.
 *
 * @author Jens Bertram
 */
public abstract class ClarityScoreResult
    extends ScoringResult {
  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
      ClarityScoreResult.class);
  /**
   * Used implementation of {@link ClarityScoreCalculation}.
   */
  private final Class<? extends ClarityScoreCalculation> type;
  /**
   * Collect messages to include in result XML.
   */
  private final List<Tuple2<String, String>> messages = new
      ArrayList<>(10);
  /**
   * Query terms used for calculation.
   */
  @Nullable
  private BytesRefArray queryTerms;
  /**
   * Flag indicating, if this result is empty.
   */
  private boolean isEmpty;

  /**
   * Create a new calculation result of the given type with no result.
   *
   * @param cscType Class implementing {@link ClarityScoreCalculation}
   */
  ClarityScoreResult(
      @NotNull final Class<? extends ClarityScoreCalculation> cscType) {
    this.type = cscType;
  }

  /**
   * Checks, if this result is empty.
   *
   * @return True, if it's empty
   */
  public final boolean isEmpty() {
    return this.isEmpty;
  }

  /**
   * Set this result as being empty (sets the score to zero). The message should
   * explain the reason. The message provided is also exposed to the result
   * XML.
   *
   * @param message Reason why this result is empty
   */
  public final void setEmpty(final String message) {
    this.messages.add(Tuple.tuple2("EMPTY", message));
    this.isEmpty = true;
    setScore(0d);
    LOG.warn("Score will be empty. reason={}", message);
  }

  /**
   * Set the value of the calculated score.
   *
   * @param score Calculated score
   */
  final void setScore(final double score) {
    _setScore(score);
  }

  /**
   * Get the type of calculation that created this result.
   *
   * @return Class implementing {@link ClarityScoreCalculation} that created
   * this result instance
   */
  @Override
  public final Class<? extends ClarityScoreCalculation> getType() {
    return this.type;
  }

  /**
   * Set the query terms used. Will be referenced only.
   *
   * @param qTerms Query terms
   */
  @SuppressWarnings("NullableProblems")
  final void setQueryTerms(@NotNull final BytesRefArray qTerms) {
    this.queryTerms = qTerms;
  }

  /**
   * Set the query terms used. Will be referenced only.
   *
   * @param qTerms Query terms
   */
  final void setQueryTerms(@NotNull final Collection<BytesRef> qTerms) {
    this.queryTerms = new BytesRefArray(Counter.newCounter(false));
    qTerms.stream().forEach(this.queryTerms::append);
  }

  /**
   * Get the XML representation of (some) results stored.
   *
   * @param xml XML results instance
   * @return Provided xml results instance updated in place
   */
  final ScoringResultXml getXml(final ScoringResultXml xml) {
    if (this.queryTerms != null) {
      // number of query terms
      xml.getItems().put("queryTerms",
          Integer.toString(this.queryTerms.size()));

      // query terms
      final String termStr = StreamUtils.stream(this.queryTerms)
          .map(BytesRef::utf8ToString)
          .collect(Collectors.joining(" "));
      if (!termStr.isEmpty()) {
        xml.getItems().put("queryTerms", termStr);
      }
    }

    // messages
    if (!this.messages.isEmpty()) {
      xml.getLists().put("messages", this.messages);
    }

    return xml;
  }

  /**
   * Result XML message types.
   */
  @SuppressWarnings("ProtectedInnerClass")
  protected enum MessageType {
    /**
     * Information message.
     */
    INFO,
    /**
     * Warning message.
     */
    WARNING,
    /**
     * Error message.
     */
    ERROR,
    /**
     * Debugging message.
     */
    DEBUG
  }
}
