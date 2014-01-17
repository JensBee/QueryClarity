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

/**
 * Simple wrapper for storing general term frequency values.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
class TermFreqData implements Serializable {

  private static final long serialVersionUID = 7526472295328776147L;
  /**
   * Total frequency value of a term in relation to the whole index.
   */
  private long totalFreq = 0L;
  /**
   * Relative frequency value of a term in relation to the whole index.
   */
  private Double relFreq = 0d;

  /**
   * Constructor taking both values ass initial parameter.
   * @param tFreq Total frequency
   * @param rFreq Relative frequency
   */
  public TermFreqData(final long tFreq, final double rFreq) {
    this.totalFreq = tFreq;
    this.relFreq = rFreq;
  }

  /**
   * Constructor passing the relative frequency only.
   * @param rFreq Relative frequency
   */
  TermFreqData(final double rFreq) {
    this.relFreq = rFreq;
  }

  /**
   * Constructor passing the total frequency only.
   * @param tFreq Total frequency
   */
  TermFreqData(final long tFreq) {
    this.totalFreq = tFreq;
  }

  /**
   * Add the given value to the total frequency value.
   * @param tFreq Value to add total frequency
   * @return The updated total frequency value
   */
  public long addFreq(final long tFreq) {
    this.totalFreq += tFreq;
    return this.totalFreq;
  }

  /**
   * Get the currently set total frequency value
   * @return Total frequency value
   */
  public long getTotalFreq() {
    return totalFreq;
  }

  /**
   * Set the total frequency value
   * @param tFreq Total frequency value
   */
  public void setTotalFreq(final long tFreq) {
    this.totalFreq = tFreq;
  }

  /**
   * Get the relative frequency value
   * @return Relative frequency value
   */
  public double getRelFreq() {
    return relFreq;
  }

  /**
   * Set the relative frequency value
   * @return Relative frequency value
   */
  public void setRelFreq(final double rFreq) {
    this.relFreq = rFreq;
  }
}
