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

package de.unihildesheim.iw.fiz;

import de.unihildesheim.iw.lucene.index.termfilter.HeuristicTermFilter.AlphaCount;
import de.unihildesheim.iw.lucene.index.termfilter.HeuristicTermFilter.ContainsGreek;
import de.unihildesheim.iw.lucene.index.termfilter.HeuristicTermFilter.DigitThreshold;
import de.unihildesheim.iw.lucene.index.termfilter.HeuristicTermFilter.NonASCIIThreshold;
import de.unihildesheim.iw.lucene.index.termfilter.HeuristicTermFilter.NumberWithUnit;
import de.unihildesheim.iw.lucene.index.termfilter.HeuristicTermFilter.Repetition;
import de.unihildesheim.iw.lucene.index.termfilter.HeuristicTermFilter.StartsWith;

import de.unihildesheim.iw.lucene.index.termfilter.HeuristicTermFilter
    .StartsWith.Type;
import de.unihildesheim.iw.lucene.index.termfilter.TermFilter;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class SimpleTermFilter
    extends TermFilter {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(SimpleTermFilter.class);

//  final Pattern matchPattern;
  final Collection<TermFilter> tFilters;

  public SimpleTermFilter() {
    this.tFilters = Arrays.asList(
        new StartsWith(Type.DIGIT),
        new StartsWith(Type.PUNCT),
        new AlphaCount(3),
        new Repetition(3),
        new DigitThreshold(0.5, true),
        new ContainsGreek(),
        new NonASCIIThreshold(0.3),
        new NumberWithUnit(0.2, true)
    );
  }

  @Override
  public boolean isAccepted(@Nullable final TermsEnum termsEnum,
      @NotNull final BytesRef term)
      throws IOException {
    for (final TermFilter t : this.tFilters) {
      if (!t.isAccepted(termsEnum, term)) {
        return false;
      }
    }
    return true;
  }
}
