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

package de.unihildesheim.iw.lucene.index.termfilter;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

/**
 * Abstract definition of a TermFilter.
 */
public abstract class TermFilter {
  /**
   * Top-reader having all sub-readers available.
   */
  @Nullable
  IndexReader topReader;

  /**
   * @param termsEnum TermsEnum currently in use. Be careful not to change the
   * current position of the enum while filtering.
   * @param term Current term
   * @return AcceptStatus indicating, if term is valid (should be returned)
   * @throws IOException Thrown on low-level I/O-errors
   */
  public abstract boolean isAccepted(
      @Nullable final TermsEnum termsEnum,
      @NotNull final BytesRef term)
      throws IOException;

  /**
   * Set the top composite reader.
   *
   * @param reader Top-reader
   */
  @SuppressWarnings("NullableProblems")
  public void setTopReader(@NotNull final IndexReader reader) {
    this.topReader = reader;
  }

}
