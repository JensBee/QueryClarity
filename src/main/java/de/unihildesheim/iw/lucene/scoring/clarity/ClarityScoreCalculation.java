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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;

/**
 * Generic interface for various clarity score calculation implementations.
 *
 * @author Jens Bertram
 */
public interface ClarityScoreCalculation {

  /**
   * Calculate the clarity score based on the given query terms.
   *
   * @param query Query used for term extraction
   * @return Calculated clarity score for the given terms, or <tt>null</tt> on
   * errors.
   * @throws ClarityScoreCalculationException Thrown on errors by implementing
   * class, if calculation fails
   */
  ClarityScoreResult calculateClarity(final String query)
      throws ClarityScoreCalculationException;

  /**
   * Get a short identifier for this calculation implementation.
   *
   * @return Identifier
   */
  String getIdentifier();

  /**
   * Basic class wrapping errors occurring while calculating the Clarity Score.
   * May be extended by implementing classes to provide finer grained error
   * tracing.
   */
  @SuppressWarnings("PublicInnerClass")
  final class ClarityScoreCalculationException
      extends Exception {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = 3480986541044908191L;

    /**
     * Creates a new Exception, forwarding another exception and a custom
     * message.
     *
     * @param msg Message
     * @param ex Throwable to forward
     */
    public ClarityScoreCalculationException(
        @Nullable final String msg,
        @NotNull final Throwable ex) {
      super(msg, ex);
    }
  }

  /**
   * Store low-precision model calculation results.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class ScoreTupleLowPrecision {
    /**
     * Query model value.
     */
    public final double qModel;
    /**
     * Collection model value.
     */
    public final double cModel;

    /**
     * Store low-precision model calculation results.
     * @param qModel Query model value
     * @param cModel Collection model value
     */
    public ScoreTupleLowPrecision(final double qModel, final double cModel) {
      this.qModel = qModel;
      this.cModel = cModel;
    }
  }

  /**
   * Store high-precision model calculation results.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class ScoreTupleHighPrecision {
    /**
     * Query model value.
     */
    @NotNull
    public final BigDecimal qModel;
    /**
     * Collection model value.
     */
    @NotNull
    public final BigDecimal cModel;

    /**
     * Store high-precision model calculation results.
     * @param qModel Query model value
     * @param cModel Collection model value
     */
    public ScoreTupleHighPrecision(
        @NotNull final BigDecimal qModel,
        @NotNull final BigDecimal cModel) {
      this.qModel = qModel;
      this.cModel = cModel;
    }
  }
}
