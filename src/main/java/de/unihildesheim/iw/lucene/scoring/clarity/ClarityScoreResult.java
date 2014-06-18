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

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.lucene.scoring.ScoringResult;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.StringUtils;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
  private final List<Tuple.Tuple2<String, String>> messages = new
      ArrayList<>(10);
  /**
   * Query terms used for calculation.
   */
  private Collection<ByteArray> queryTerms;
  /**
   * Flag indicating, if this result is empty.
   */
  private boolean isEmpty;
  /**
   * If this result is empty, this should contain a message for the user to
   * explain why it's empty.
   */
  private String emptyReasonMessage = "";

  /**
   * Create a new calculation result of the given type with no result.
   *
   * @param cscType Class implementing {@link ClarityScoreCalculation}
   */
  ClarityScoreResult(final Class<? extends ClarityScoreCalculation>
      cscType) {
    this.type = Objects.requireNonNull(cscType, "Score type was null.");
    this.queryTerms = Collections.emptyList();
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
    this.emptyReasonMessage = Objects.requireNonNull(message);
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
   * If this result is empty the message describing the reason may be received
   * here.
   *
   * @return Reason message
   */
  public final String getEmptyReasonMessage() {
    return this.emptyReasonMessage;
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
   * Get the query terms used for calculation.
   *
   * @return Query terms
   */
  public final Collection<ByteArray> getQueryTerms() {
    return Collections.unmodifiableCollection(this.queryTerms);
  }

  /**
   * Set the query terms used.
   *
   * @param qTerms Query terms
   */
  final void setQueryTerms(final Collection<ByteArray> qTerms) {
    Objects.requireNonNull(qTerms);
    this.queryTerms = new ArrayList<>(qTerms.size());
    this.queryTerms.addAll(qTerms);
  }

  /**
   * Add a message to the result.
   *
   * @param mType Message type
   * @param msg Content
   */
  final void addMessage(final MessageType mType, final String msg) {
    this.messages.add(Tuple.tuple2(mType.name(), msg));
  }

  /**
   * Get the XML representation of (some) results stored.
   *
   * @param xml XML results instance
   * @return Provided xml results instance updated in place
   */
  final ScoringResultXml getXml(final ScoringResultXml xml) {
    // number of query terms
    xml.getItems().put("queryTerms",
        Integer.toString(this.queryTerms.size()));

    // query terms
    if (!this.queryTerms.isEmpty()) {
      final Collection<String> termStr = new ArrayList<>(
          this.queryTerms.size());
      for (final ByteArray term : this.queryTerms) {
        termStr.add(ByteArrayUtils.utf8ToString(term));
      }
      xml.getItems().put("queryTerms", StringUtils.join(termStr, " "));
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
