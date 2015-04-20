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

package de.unihildesheim.iw.data;

import de.unihildesheim.iw.data.IPCCode.IPCRecord.Field;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.EnumMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public class IPCCode {
  /**
   * Regular expression to match a section identifier.
   */
  private static final Pattern RX_SECTION = Pattern.compile("^[a-hA-H]$");
  /**
   * Regular expression to match a class identifier.
   */
  private static final Pattern RX_CLASS = Pattern.compile("^[0-9]{2}$");
  /**
   * Regular expression to match a subclass identifier.
   */
  private static final Pattern RX_SUBCLASS = Pattern.compile("^[a-zA-Z]$");
  /**
   * Regular expression to match a main-group identifier.
   */
  private static final Pattern RX_MAINGROUP =
      Pattern.compile("^([0-9]{0,4}).{0,4}");
  /**
   * Regular expression to match a sub-group identifier.
   */
  private static final Pattern RX_SUBGROUP =
      Pattern.compile("^([0-9]{0,6}).{0,6}");
  /**
   * Default separator for main- and sub-group.
   */
  public static final char DEFAULT_SEPARATOR = '/';
  /**
   * Regular expression to remove any spaces.
   */
  private static final Pattern RX_SPACES = Pattern.compile("\\s");

  /**
   * Single IPC code data record.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class IPCRecord {
    /**
     * Fields available in the record.
     */
    @SuppressWarnings("PublicInnerClass")
    public enum Field {
      /**
       * Section.
       */
      SECTION,
      /**
       * Class.
       */
      CLASS,
      /**
       * Sub class.
       */
      SUBCLASS,
      /**
       * Main group.
       */
      MAINGROUP,
      /**
       * Sub group.
       */
      SUBGROUP;
    }

    /**
     * Record data storage.
     */
    private final Map<Field, Object> data = new EnumMap<>(Field.class);

    /**
     * Flag indication, if this code record looks valid.
     */
    private boolean isValid = true;

    /**
     * Set a record field. The passed in data is only checked for {@code
     * null}. No validation is done and should be handled before.
     * @param f Field to add to
     * @param value Value to add
     * @return True if data was not null or empty
     */
    @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
    boolean set(@NotNull final Field f, final Object value) {
      final boolean state;

      if (value == null) {
        this.data.put(f, null);
        state = false;
      } else {
        final String valueStr = value.toString();

        if (valueStr.trim().isEmpty()) {
          this.data.put(f, null);
          state = false;
        } else {
          switch (f) {
            case CLASS:
            case MAINGROUP:
            case SUBGROUP:
              this.data.put(f, valueStr);
              state = true;
              break;
            case SECTION:
            case SUBCLASS:
              this.data.put(f, valueStr.charAt(0));
              state = true;
              break;
            default:
              state = false;
              break;
          }
        }
      }

      return state;
    }

    /**
     * Set validation flag for this record.
     * @param flag State
     */
    void setValid(final boolean flag) {
      this.isValid = flag;
    }

    /**
     * Get the validation flag.
     *
     * @return True, if at minimum all required records are set
     */
    public boolean isValid() {
      return this.isValid &&
          // required fields
          this.data.get(Field.SECTION) != null &&
          this.data.get(Field.CLASS) != null &&
          this.data.get(Field.SUBCLASS) != null;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(100);
      if (this.data.get(Field.SECTION) != null) {
        sb.append("SECTION=").append(this.data.get(Field.SECTION)).append(' ');
      }
      if (this.data.get(Field.CLASS) != null) {
        sb.append("CLASS=").append(this.data.get(Field.CLASS)).append(' ');
      }
      if (this.data.get(Field.SUBCLASS) != null) {
        sb.append("SUBCLASS=").append(this.data.get(Field.SUBCLASS))
            .append(' ');
      }
      if (this.data.get(Field.MAINGROUP) != null) {
        sb.append("MAINGROUP=")
            .append(this.data.get(Field.MAINGROUP)).append(' ');
      }
      if (this.data.get(Field.SUBGROUP) != null) {
        sb.append("SUBGROUP=")
            .append(this.data.get(Field.SUBGROUP)).append(' ');
      }
      sb.append(this.isValid() ? " (valid)" : " (invalid)");
      return sb.toString();
    }

    /**
     * Get a field of this record.
     * @param f Field to get
     * @return Field value. Empty, if no field value was set.
     */
    public String get(@NotNull final Field f) {
      return this.data.get(f) == null ? "" : this.data.get(f).toString();
    }
  }

  /**
   * Tries to parse a IPC record from a string using the {@link
   * #DEFAULT_SEPARATOR default} separator char.
   * @param code IPC code
   * @return IPC data record extracted from the given string
   */
  public static IPCRecord parse(@NotNull final String code) {
    return parse(code, null);
  }

  /**
   * Tries to parse a given string as basic IPC-code with content as described
   * by WIPO Standard ST.8. Only the first 15 characters are parsed at maximum.
   * Further the parser is not strict, as it ignores any content that follows a
   * valid IPC-code.
   *
   * @param codeStr IPC-code as string
   * @param separator Character to use for separating main- and sub-group
   * @return IPC code record object with all symbols set that could be parsed
   * from the input string
   */
  public static IPCRecord parse(
      @NotNull final CharSequence codeStr,
      @Nullable final Character separator) {
    final char sep;
    if (separator == null) {
      sep = DEFAULT_SEPARATOR;
    } else {
      sep = separator;
    }

    if (Character.isDigit(sep)) {
      throw new IllegalArgumentException(
          "Digits are not allowed as separator character.");
    }

    // fold spaces
    final String code = RX_SPACES.matcher(codeStr).replaceAll("");
    // length of whole code
    final int codeLength = code.length();
    // current position in string
    int pointer = 0;
    // final record
    final IPCRecord record = new IPCRecord();
    // flag indicating, if code looks valid
    boolean valid;

    // section [1]
    valid = RX_SECTION.matcher(code).region(pointer, pointer + 1).matches() &&
        record.set(Field.SECTION,
            String.valueOf(code.charAt(pointer)));
    pointer += 1;

    if (valid && codeLength >= pointer + 1) { // class [2-3]
      valid =
          RX_CLASS.matcher(code).region(pointer, pointer + 2).matches() &&
              record.set(Field.CLASS, code.substring(pointer, pointer + 2));
      pointer += 2;
    } else {
      valid = false;
    }

    if (valid && codeLength >= pointer + 1) { // subclass [4]
      valid =
          RX_SUBCLASS.matcher(code).region(pointer, pointer + 1).matches() &&
              record.set(Field.SUBCLASS, code.substring(pointer, pointer + 1));
      pointer += 1;
    } else {
      valid = false;
    }

    // code may already be finished here
    if (codeLength > pointer) {
      if (valid && codeLength >= pointer + 1) { // main group [5-8] or blank
        if (Character.compare(code.charAt(pointer), sep) != 0) {
          final Matcher mgm = RX_MAINGROUP.matcher(code)
              .region(pointer, Math.min(pointer + 4, codeLength));
          if (mgm.matches()) {
            final String match = mgm.group(1);
            if (match != null && !match.isEmpty()) {
              valid = record.set(Field.MAINGROUP, match);
              pointer += match.length();
            }
          } else {
            valid = false;
          }
        }
      }

      if (valid && codeLength >= pointer + 1) { // separator char [9]
        valid = Character.compare(code.charAt(pointer), sep) == 0;
        pointer += 1;
      }

      if (valid && codeLength >= pointer + 1) { // subgroup [10-15] or blank
        final Matcher sgm = RX_SUBGROUP.matcher(code)
            .region(pointer, Math.min(pointer + 4, codeLength));
        if (sgm.matches()) {
          final String match = sgm.group(1);
          if (match != null && !match.isEmpty()) {
            valid = record.set(Field.SUBGROUP, match);
            //pointer += match.length();
          }
        } else {
          valid = false;
        }
      }
    }

    record.setValid(valid);
    return record;
  }
}
