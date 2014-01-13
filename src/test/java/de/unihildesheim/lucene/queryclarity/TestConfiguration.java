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
package de.unihildesheim.lucene.queryclarity;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class TestConfiguration {

  /**
   * Test lucene index instance.
   */
  private TestIndex index;

  /**
   * Maximum length of a random generated query. If the number of unique terms
   * in the index is lower than the maximum, then the number of unique terms
   * will be used as maximum.
   */
  private int maxQueryLength = 10;

  public TestIndex getIndex() {
    return index;
  }

  public void setIndex(final TestIndex newIndex) {
    this.index = newIndex;
  }

  public int getMaxQueryLength() {
    return maxQueryLength;
  }

  public void setMaxQueryLength(final int newMaxQueryLength) {
    this.maxQueryLength = newMaxQueryLength;
  }
}
