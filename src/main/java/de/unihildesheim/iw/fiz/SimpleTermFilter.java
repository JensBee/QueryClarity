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

import de.unihildesheim.iw.lucene.index.termfilter.TermFilter;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

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

  final Pattern matchPattern;

  public SimpleTermFilter() {
    this.matchPattern = Pattern.compile(
        "^\\p{Digit}+(:?[\\p{Punct}\\p{InGreek}\\p{Alpha}º]{1,2}\\p{Digit}+)*" +
            "(:?[\\p{Punct}\\p{InGreek}\\p{Alpha}]{1,2}|mol|[µcdkmn]" +
            "(?:hz|pa|m[23]??)|º[c|f])??$",
        Pattern.CASE_INSENSITIVE);
  }

  @Override
  public boolean isAccepted(@Nullable final TermsEnum termsEnum,
      @NotNull final BytesRef term)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      final String termStr = term.utf8ToString();
      if (this.matchPattern.matcher(termStr).matches()) {
        LOG.debug("Removing term: '{}'", termStr);
        return false;
      }
      return true;
    } else {
      return !this.matchPattern.matcher(term.utf8ToString()).matches();
    }
  }
}
