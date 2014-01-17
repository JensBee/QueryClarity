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
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
class TermFreq implements Serializable {

  private static final long serialVersionUID = 7526472295328776147L;
  private long totalFreq = 0L;
  private Double relFreq = 0d;

  public TermFreq(final long tFreq, final double rFreq) {
    this.totalFreq = tFreq;
    this.relFreq = rFreq;
  }

  TermFreq(final double rFreq) {
    this.relFreq = rFreq;
  }

  TermFreq(final long tFreq) {
    this.totalFreq = tFreq;
  }

  public long addFreq(final long tFreq) {
    this.totalFreq += tFreq;
    return this.totalFreq;
  }

  public long getTotalFreq() {
    return totalFreq;
  }

  public void setTotalFreq(long tFreq) {
    this.totalFreq = tFreq;
  }

  public double getRelFreq() {
    return relFreq;
  }

  public void setRelFreq(double rFreq) {
    this.relFreq = rFreq;
  }
}
