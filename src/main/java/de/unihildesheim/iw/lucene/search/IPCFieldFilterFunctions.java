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

package de.unihildesheim.iw.lucene.search;

import de.unihildesheim.iw.data.IPCCode.IPCRecord.Field;
import de.unihildesheim.iw.data.IPCCode.Parser;
import de.unihildesheim.iw.lucene.search.IPCFieldFilter.IPCFieldFilterFunc;
import org.apache.lucene.index.IndexReader;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

/**
 * Filters to use with a {@link IPCFieldFilter}.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class IPCFieldFilterFunctions {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(IPCFieldFilterFunctions.class);

  /**
   * Get all unique section identifiers for all given IPC codes.
   *
   * @param ipcParser Parser for IPC codes
   * @param codes Codes to parse
   * @return Unique set of identifiers extracted from the given IPC codes
   */
  static Collection<Character> getSections(
      @NotNull final Parser ipcParser,
      @NotNull final String... codes) {
    final Collection<Character> secs = new HashSet<>(codes.length);

    for (final String c : codes) {
      final String sec = ipcParser.parse(c).get(Field.SECTION);
      if (sec.length() >= 1) {
        secs.add(sec.charAt(0));
      } else if (LOG.isDebugEnabled()) {
        LOG.debug("No section information for code {}", c);
      }
    }
    return secs;
  }

  /**
   * Filter documents based on the amount of different IPC-Sections.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class MaxIPCSections
      extends IPCFieldFilterFunc {

    /**
     * Maximum amount of different IPC-Sections allowed.
     */
    private final int maxSecs;

    /**
     * Initialize the filter with a maximum amount of different IPC-Sections
     * allowed.
     *
     * @param maxAmount Maximum diverse IPC-Sections allowed
     */
    public MaxIPCSections(final int maxAmount) {
      this.maxSecs = maxAmount;
    }

    @Override
    boolean isAccepted(
        @NotNull final IndexReader reader,
        final int docId,
        @NotNull final Parser ipcParser)
        throws IOException {
      final Collection<Character> sections =
          getSections(ipcParser, getCodes(reader, docId));
      return sections.size() <= this.maxSecs;
    }

    @Override
    public String toString() {
      return "FilterMaxSections(" + this.maxSecs + ')';
    }
  }
}
