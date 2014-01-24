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
package de.unihildesheim.lucene.index;

import java.io.Serializable;
import java.util.Objects;

/**
 * Simple wrapper for storing general term frequency values.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class TermFreqData implements Serializable {

  /**
   * Serialization class version id.
   */
  private static final long serialVersionUID = 0L;

  /**
   * Total frequency value of a term in relation to the whole index.
   */
  private final long totalFreq;

  /**
   * Relative frequency value of a term in relation to the whole index.
   */
  private final Double relFreq;

  /**
   * Constructor taking both values ass initial parameter.
   *
   * @param tFreq Total frequency
   * @param rFreq Relative frequency
   */
  public TermFreqData(final long tFreq, final double rFreq) {
    this.totalFreq = tFreq;
    this.relFreq = rFreq;
  }

  /**
   * Constructor passing the relative frequency only.
   *
   * @param rFreq Relative frequency
   */
  TermFreqData(final double rFreq) {
    this.relFreq = rFreq;
    this.totalFreq = 0L;
  }

  /**
   * Constructor passing the total frequency only.
   *
   * @param tFreq Total frequency
   */
  TermFreqData(final long tFreq) {
    this.totalFreq = tFreq;
    this.relFreq = 0d;
  }

  /**
   * Add the given value to the total frequency value.
   *
   * @param tFreq Value to add total frequency
   * @return New {@link TermFreqData} object with all properties of the current
   * object and the given value added to the total term frequency value.
   */
  public TermFreqData addToTotalFreq(final long tFreq) {
    return new TermFreqData(this.totalFreq + tFreq, this.relFreq);
  }

  /**
   * Get the currently set total frequency value.
   *
   * @return Total frequency value
   */
  public long getTotalFreq() {
    return this.totalFreq;
  }

  /**
   * Get the relative frequency value.
   *
   * @return Relative frequency value
   */
  public double getRelFreq() {
    if (this.relFreq == null) {
      return 0d;
    }
    return this.relFreq;
  }

  /**
   * Set the relative frequency value.
   *
   * @param rFreq Relative frequency value
   * @return New {@link TermFreqData} object with all properties of the current
   * object and the given value set for the relative term frequency.
   */
  public TermFreqData addRelFreq(final double rFreq) {
    return new TermFreqData(this.totalFreq, rFreq);
  }

  @Override
  @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TermFreqData tfData = (TermFreqData) o;

    if (this.totalFreq != tfData.totalFreq) {
      return false;
    }
    if (this.relFreq == null ? tfData.relFreq != null : !this.relFreq.equals(
            tfData.relFreq)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 73 * hash + (int) (this.totalFreq ^ (this.totalFreq >>> 32));
    hash = 73 * hash + Objects.hashCode(this.relFreq);
    return hash;
  }
}
