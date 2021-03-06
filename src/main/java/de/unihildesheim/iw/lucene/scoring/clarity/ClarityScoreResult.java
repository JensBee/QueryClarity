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

import de.unihildesheim.iw.lucene.scoring.ScoringResult;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Optional;

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
   * Reason for this result being empty (optional).
   */
  private String emptyMessage;
  /**
   * Query terms used for calculation.
   */
  @Nullable
  private BytesRefArray queryTerms;
  /**
   * Flag indicating, if this result is empty.
   */
  private boolean isEmpty;

  public enum EmptyReason {
    NO_QUERY_TERMS("No query terms"),
    TOO_MANY_BOOLCLAUSES("Boolean query too large."),
    NO_FEEDBACK("No feedback documents.");

    private final String msg;

    EmptyReason(final String msg) {
      this.msg = msg;
    }

    @Override
    public String toString() {
      return this.msg;
    }
  }

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
    this.emptyMessage = message;
    this.isEmpty = true;
    setScore(0d);
    LOG.warn("Score will be empty. reason={}", message);
  }

  public final void setEmpty(final EmptyReason msg) {
    setEmpty(msg.toString());
  }

  /**
   * Get the (optional) message stored when this result is empty.
   * @return Message or empty string.
   */
  public final Optional<String> getEmptyReason() {
    if (this.isEmpty && this.emptyMessage != null &&
        !this.emptyMessage.trim().isEmpty()) {
        return Optional.of(this.emptyMessage);
    }
    return Optional.empty();
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
