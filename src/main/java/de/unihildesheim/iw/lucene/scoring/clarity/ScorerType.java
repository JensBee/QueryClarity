/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

import org.jetbrains.annotations.Nullable;

/**
 * Types of clarity score scorers available.
 */
public enum ScorerType {
  /**
   * {@link DefaultClarityScore} scorer.
   */
  DCS("Default Clarity Score"),
  /**
   * {@link ImprovedClarityScore} scorer.
   */
  ICS("Improved Clarity Score"),
  /**
   * {@link SimplifiedClarityScore} scorer.
   */
  SCS("Simplified Clarity Score");

  /**
   * Current scorer name.
   */
  private final String name;

  /**
   * Create scorer type instance.
   *
   * @param sName Name of the scorer
   */
  ScorerType(final String sName) {
    this.name = sName;
  }

  /**
   * Get the name of the scorer.
   *
   * @return Scorer's name
   */
  public String toString() {
    return this.name;
  }

  /**
   * Get a instance by name.
   *
   * @param aName Name to identify the scorer to get
   * @return Scorer instance, or {@code null} if none was found
   */
  @Nullable
  public static ScorerType getByName(final String aName) {
    for (final ScorerType st : values()) {
      if (st.name().equalsIgnoreCase(aName)) {
        return st;
      }
    }
    return null;
  }
}
