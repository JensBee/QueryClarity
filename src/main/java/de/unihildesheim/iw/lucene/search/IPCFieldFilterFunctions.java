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

import de.unihildesheim.iw.data.IPCCode;
import de.unihildesheim.iw.data.IPCCode.IPCRecord;
import de.unihildesheim.iw.data.IPCCode.IPCRecord.Field;
import de.unihildesheim.iw.data.IPCCode.InvalidIPCCodeException;
import de.unihildesheim.iw.data.IPCCode.Parser;
import de.unihildesheim.iw.lucene.search.IPCFieldFilter.IPCFieldFilterFunc;
import org.apache.lucene.index.IndexReader;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.regex.Pattern;

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
      try {
        final String sec = ipcParser.parse(c).get(Field.SECTION);
        if (sec.length() >= 1) {
          secs.add(sec.charAt(0));
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("No section information for code {}", c);
        }
      } catch (final InvalidIPCCodeException e) {
        LOG.error("Invalid IPC-code: '{}'. Skipping.", c);
      }
    }
    return secs;
  }

  /**
   * Filter that matches a list of {@link Field IPC-Record fields} against a
   * base record. Note: Unset fields are interpretet as match.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class IPCFieldMatcher
      extends IPCFieldFilterFunc {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(IPCFieldMatcher.class);
    /**
     * IPC-Record used for filtering.
     */
    private final IPCRecord rec;
    /**
     * Fields to check while filtering.
     */
    private final Collection<Field> requiredFields;
    /**
     * Flag indicating, if a single match is sufficient to accept a document.
     */
    private boolean matchSingle;

    /**
     * Initialize the filter and uses the fields of the passed in {@link
     * IPCRecord record} for a matching. A match is only required for the
     * specified fields.
     *
     * @param record Base record
     * @param fields Fields to check
     */
    public IPCFieldMatcher(
        @NotNull final IPCRecord record,
        @NotNull final Field... fields) {
      if (record.isEmpty()) {
        throw new IllegalArgumentException("Base record is empty.");
      }
      if (fields.length <= 0) {
        throw new IllegalArgumentException("No fields specified.");
      }

      this.requiredFields = new ArrayList<>(fields.length);
      for (final Field f : fields) {
        if (record.get(f) == null) {
          LOG.warn("Skipping field {}, as it's empty in base record.", f);
        } else {
          this.requiredFields.add(f);
        }
      }

      this.rec = record;
      if (LOG.isDebugEnabled()) {
        LOG.debug(toString());
      }
    }

    @Override
    public IPCFieldMatcher requireAllMatch() {
      this.matchSingle = false;
      return this;
    }

    @Override
    public IPCFieldMatcher requireSingleMatch() {
      this.matchSingle = true;
      return this;
    }

    @Override
    boolean isAccepted(@NotNull final IndexReader reader, final int docId,
        @NotNull final Parser ipcParser)
        throws IOException {
      boolean hasMatch = false;
      for (final String c : getCodes(reader, docId)) {
        try {
          final IPCRecord r = ipcParser.parse(c);
          for (final Field f : this.requiredFields) {
            if (r.get(f) == null ||
                this.rec.get(f).equalsIgnoreCase(r.get(f))) {
              // match, skip to next code, or return if single match is enough
              if (this.matchSingle) {
                return true;
              }
              hasMatch = true;
              break;
            } else if (!this.matchSingle) {
              // non-match, skip document
              if (LOG.isTraceEnabled()) {
                LOG.trace("Non-matching IPC={} f={} {} != {}-> skip doc={}",
                    c, f, this.rec.get(f), r.get(f), docId);
              }
              return false;
            }
          }
        } catch (final InvalidIPCCodeException e) {
          LOG.error("Invalid IPC-code: '{}'. Skipping.", c);
        }
      }
      return hasMatch;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() +
          " ipc=" + this.rec.toFormattedString() +
          " ipc-rex=" + this.rec.toRegExpString() +
          " fields=" + this.requiredFields;
    }
  }

  /**
   * Filter that requires to matches at least one of the field values of a given
   * base record. Fields are parsed in the {@link IPCCode
   * .IPCRecord#FIELDS_ORDER order} they are used when printed out. An IPC-Code
   * is accepted when comparing fields generates the first match. Similar an
   * IPC-Code gets rejected if a non-match was generated before a match. Note:
   * Unset fields are interpreted as match.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class SloppyMatch
      extends IPCFieldFilterFunc {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(SloppyMatch.class);
    /**
     * IPC-Record used for filtering.
     */
    private final IPCRecord rec;
    /**
     * Fields to check while filtering.
     */
    private final Collection<Field> availableFields;
    /**
     * Flag indicating, if a single match is sufficient to accept a document.
     */
    private boolean matchSingle;

    /**
     * Initialize the filter and set the fields of the passed in {@link
     * IPCRecord record} as required for a match for every IPC-Code.
     *
     * @param record Base record
     */
    public SloppyMatch(@NotNull final IPCRecord record) {
      if (record.isEmpty()) {
        throw new IllegalArgumentException("Base record is empty.");
      }
      this.rec = record;

      this.availableFields = record.getSetFields();
      if (this.availableFields.size() <= 0) {
        throw new IllegalArgumentException("Base record has no fields set.");
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(toString());
      }
    }

    @Override
    boolean isAccepted(@NotNull final IndexReader reader, final int docId,
        @NotNull final Parser ipcParser)
        throws IOException {
      boolean hasMatch = false;
      for (final String c : getCodes(reader, docId)) {
        final IPCRecord r;
        try {
          r = ipcParser.parse(c);
          for (final Field f : this.availableFields) {
            if (r.get(f) == null ||
                this.rec.get(f).equalsIgnoreCase(r.get(f))) {
              // match, skip to next code, or return if single match is enough
              if (this.matchSingle) {
                return true;
              }
              hasMatch = true;
              break;
            } else if (!this.matchSingle) {
              // non-match, skip document
              if (LOG.isTraceEnabled()) {
                LOG.trace("Non-matching IPC={} f={} {} != {}-> skip doc={}",
                    c, f, this.rec.get(f), r.get(f), docId);
              }
              return false;
            }
          }
        } catch (final InvalidIPCCodeException e) {
          LOG.error("Invalid IPC-code: '{}'. Skipping.", c);
        }
      }
      return hasMatch;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() +
          " ipc=" + this.rec.toFormattedString() +
          " ipc-rex=" + this.rec.toRegExpString() +
          " fields=" + this.availableFields;
    }

    @Override
    public SloppyMatch requireAllMatch() {
      this.matchSingle = false;
      return this;
    }

    @Override
    public SloppyMatch requireSingleMatch() {
      this.matchSingle = true;
      return this;
    }
  }

  /**
   * Filter that requires all of the {@link Field fields} of the passed in
   * {@link IPCRecord record} are matched.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class GreedyMatch
      extends IPCFieldFilterFunc {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(GreedyMatch.class);
    /**
     * IPC-Record used for filtering.
     */
    private final IPCRecord rec;
    /**
     * Flag indicating, if a single match is sufficient to accept a document.
     */
    private boolean matchSingle;

    /**
     * Initialize the filter and set the fields of the passed in {@link
     * IPCRecord record} as required for a match for every IPC-Code.
     *
     * @param record Record
     */
    public GreedyMatch(@NotNull final IPCRecord record) {
      if (record.isEmpty()) {
        throw new IllegalArgumentException("Base record is empty.");
      }
      this.rec = record;

      if (LOG.isDebugEnabled()) {
        LOG.debug(toString());
      }
    }

    @Override
    boolean isAccepted(@NotNull final IndexReader reader, final int docId,
        @NotNull final Parser ipcParser)
        throws IOException {
      final Pattern ipcRx =
          Pattern.compile(this.rec.toRegExpString(ipcParser.getSeparator()));
      boolean hasMatch = false;
      for (final String c : getCodes(reader, docId)) {
        if (ipcRx.matcher(c).matches()) {
          // match, return if single match is enough
          if (this.matchSingle) {
            return true;
          }
          hasMatch = true;
        } else if (!this.matchSingle) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Non-matching IPC={} -> skip doc={}", c, docId);
          }
          return false;
        }
      }
      return hasMatch;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() +
          " ipc=" + this.rec.toFormattedString() +
          " ipc-rex=" + this.rec.toRegExpString();
    }

    @Override
    public GreedyMatch requireAllMatch() {
      this.matchSingle = false;
      return this;
    }

    @Override
    public GreedyMatch requireSingleMatch() {
      this.matchSingle = true;
      return this;
    }
  }

  /**
   * Filter documents based on the amount of different IPC-Sections.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class MaxIPCSections
      extends IPCFieldFilterFunc {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG =
        LoggerFactory.getLogger(MaxIPCSections.class);
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
      if (maxAmount <= 0) {
        throw new IllegalArgumentException("Amount must be greater than zero.");
      }
      this.maxSecs = maxAmount;
      if (LOG.isDebugEnabled()) {
        LOG.debug(toString());
      }
    }

    @Override
    boolean isAccepted(
        @NotNull final IndexReader reader,
        final int docId,
        @NotNull final Parser ipcParser)
        throws IOException {
      final Collection<Character> sections =
          getSections(ipcParser, getCodes(reader, docId));
      if (LOG.isTraceEnabled() && sections.size() > this.maxSecs) {
        LOG.trace("Section count exceeded. max={} current={} -> skip doc={}",
            this.maxSecs, sections.size(), docId);
      }
      return sections.size() <= this.maxSecs;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() +
          " maxSecs=" + this.maxSecs;
    }
  }
}
