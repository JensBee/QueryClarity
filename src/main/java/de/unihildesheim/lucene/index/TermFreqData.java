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

import de.unihildesheim.util.Lockable;
import java.io.Serializable;

/**
 * Simple wrapper for storing general term frequency values.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class TermFreqData implements Serializable, Lockable {

  /**
   * Serialization class version id.
   */
  private static final long serialVersionUID = 0L;

  /**
   * Total frequency value of a term in relation to the whole index.
   */
  private long totalFreq;

  /**
   * Message to throw, if model is locked.
   */
  private static final String LOCKED_MSG = "Operation not supported. "
          + "Object is locked.";

  /**
   * Relative frequency value of a term in relation to the whole index.
   */
  private double relFreq;

  /**
   * If true the model is locked and immutable.
   */
  private boolean locked = false;

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
   */
  public void addToTotalFreq(final long tFreq) {
    if (this.locked) {
      throw new UnsupportedOperationException(LOCKED_MSG);
    }
    this.totalFreq += tFreq;
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
    return this.relFreq;
  }

  /**
   * Set the relative frequency value.
   *
   * @param rFreq Relative frequency value
   */
  public void setRelFreq(final double rFreq) {
    if (this.locked) {
      throw new UnsupportedOperationException(LOCKED_MSG);
    }
    this.relFreq = rFreq;
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

    if (this.totalFreq != tfData.totalFreq || this.relFreq != tfData.relFreq) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 17 * hash + (int) (this.totalFreq ^ (this.totalFreq >>> 32));
    hash = 17 * hash + (int) (Double.doubleToLongBits(this.relFreq)
            ^ (Double.doubleToLongBits(this.relFreq) >>> 32));
    return hash;
  }

  @Override
  public void lock() {
    this.locked = true;
  }

  @Override
  public void unlock() {
    this.locked = false;
  }

  @Override
  public boolean isLocked() {
    return this.isLocked();
  }
}
