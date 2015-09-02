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

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;

/**
 * Filter based on a list of stopwords wrapping another filter.
 */
@SuppressWarnings("PublicInnerClass")
public final class StopwordTermFilter
    extends TermFilter {
  /**
   * Wrapped filter.
   */
  private final TermFilter in;
  /**
   * List of stopwords set.
   */
  private final BytesRefHash sWords;

  /**
   * Creates a new StopWordWrapper wrapping a given TermFilter.
   *
   * @param words List of stopwords
   * @param wrap TermFilter to wrap
   */
  @SuppressWarnings("ObjectAllocationInLoop")
  public StopwordTermFilter(
      @NotNull final Iterable<String> words,
      @NotNull final TermFilter wrap) {
    this.in = wrap;
    this.sWords = new BytesRefHash();
    for (final String sw : words) {
      this.sWords.add(new BytesRef(sw));
    }
  }

  @Override
  public boolean isAccepted(
      @Nullable final TermsEnum termsEnum,
      @NotNull final BytesRef term)
      throws IOException {
    return this.sWords.find(term) <= -1 && this.in.isAccepted(termsEnum,
        term);
  }
}
