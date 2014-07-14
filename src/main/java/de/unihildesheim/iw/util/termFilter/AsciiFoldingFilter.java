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

package de.unihildesheim.iw.util.termFilter;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.util.ByteArrayUtils;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * @author Jens Bertram
 */
public class AsciiFoldingFilter
    extends TermFilter {
  private static final long serialVersionUID = -8646518629851142686L;
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(ASCIIFoldingFilter.class);
  private char[] output = new char[512];

  @Override
  public ByteArray filter(final ByteArray term) {
    final String termStr = ByteArrayUtils.utf8ToString(term);
    final int length = termStr.length();
    final char[] buffer = termStr.toCharArray();

    // If no characters actually require rewriting then we
    // just return token as-is:
    for (int i = 0; i < length; ++i) {
      final char c = buffer[i];
      if (c >= '\u0080') {
        // Worst-case length required:
        final int maxSizeNeeded = 4 * length;
        if (output.length < maxSizeNeeded) {
          output = new char[ArrayUtil
              .oversize(maxSizeNeeded, RamUsageEstimator.NUM_BYTES_CHAR)];
        }

        final int finalLength = ASCIIFoldingFilter.foldToASCII(
            termStr.toCharArray(), 0, output, 0, length);

        final byte[] termBytes = String.valueOf(output, 0,
            finalLength).getBytes(StandardCharsets.UTF_8);

        LOG.debug("Folding {} to {}.", termStr, ByteArrayUtils.utf8ToString
            (termBytes));

        return new ByteArray(termBytes);
      }
    }
    return term;
  }

  @Override
  public boolean equals(final TermFilter other) {
    return (other instanceof AsciiFoldingFilter);
  }
}
