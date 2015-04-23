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

package de.unihildesheim.iw.lucene.query;

import de.unihildesheim.iw.data.IPCCode;
import de.unihildesheim.iw.data.IPCCode.IPCRecord;
import de.unihildesheim.iw.data.IPCCode.Parser;
import de.unihildesheim.iw.lucene.index.builder.IndexBuilder.LUCENE_CONF;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generate a {@link Query} from an (incomplete) IPC-code.
 *
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class IPCClassQuery {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(IPCClassQuery.class);

  /**
   * Get a Query object for matching all IPC-codes that are like the one passed
   * in. Uses the {@link IPCCode.Parser#DEFAULT_SEPARATOR default} separator
   * char.
   *
   * @param ipc (Partial) code to search for
   * @return Query to search for IPC-codes
   * @see #get(CharSequence, char)
   * @see #get(IPCRecord, char)
   */
  public static Query get(@NotNull final CharSequence ipc) {
    return get(ipc, Parser.DEFAULT_SEPARATOR);
  }

  /**
   * Get a Query object for matching all IPC-codes that are like the one passed
   * in. Uses the passed in separator char.
   *
   * @param ipc (Partial) code to search for
   * @param separator Separator for main- and sub-group
   * @return Query to search for IPC-codes
   * @see #get(IPCRecord, char)
   */
  public static Query get(
      @NotNull final CharSequence ipc,
      final char separator) {
    final IPCRecord record;
    try {
      record = IPCCode.parse(ipc, separator);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Cannot create a valid IPC code from '" + ipc + '.');
    }

    if (record.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot create a valid IPC code from '" + ipc + "'. Got '" +
              record.toFormattedString(separator) + '\'');
    }
    return get(record, separator);
  }

  /**
   * Get a Query object for matching all IPC-codes that are like the one passed
   * in. If a partial IPC-code (like {@code H05}) is given, all IPC-codes
   * matching {@code H05.*} will be matched by the query. If a full code (like
   * {@code H05K0005-04}) is given, the query will only search for IPC-codes
   * matching exactly this code.
   *
   * @param record IPC record to search for
   * @param separator Separator for main- and sub-group
   * @return Query to search for IPC-codes
   */
  public static Query get(@NotNull final IPCRecord record,
      final char separator) {
    final Query q = new RegexpQuery(new Term(
        LUCENE_CONF.FLD_IPC, record.toRegExpString(separator)));

    if (LOG.isDebugEnabled()) {
      LOG.debug("IPCq: {}", q);
    }

    return q;
  }
}
