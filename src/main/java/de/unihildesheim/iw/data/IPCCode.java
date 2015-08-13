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

import de.unihildesheim.iw.util.Buildable;
import de.unihildesheim.iw.data.IPCCode.IPCRecord.Field;
import de.unihildesheim.iw.util.StringUtils;
import org.apache.lucene.search.RegexpQuery;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class IPCCode {
  public static class InvalidIPCCodeException extends Exception {
    /**
     * Plain constructor without descriptional message.
     */
    public InvalidIPCCodeException() {}

    /**
     * Constructor accepting a message to infor about the error cause.
     * @param message Error cause or similar message
     */
    public InvalidIPCCodeException(final String message) {
      super(message);
    }
  }

  /**
   * Single IPC code data record.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class IPCRecord {
    /**
     * Maximum length of a full IPC-code as produced by this implementation.
     */
    public static final int MAX_LENGTH = 15;
    /**
     * Comparator for IPC-Records.
     */
    public static final Comparator<IPCRecord> COMPARATOR =
        new IPCRecordComparator();

    /**
     * Fields available in the record.
     */
    @SuppressWarnings("PublicInnerClass")
    public enum Field {
      /**
       * Section.
       */
      SECTION(1, null),
      /**
       * Class.
       */
      CLASS(2, '0'),
      /**
       * Sub class.
       */
      SUBCLASS(1, null),
      /**
       * Main group.
       */
      MAINGROUP(4, '0'),
      /**
       * Sub group.
       */
      SUBGROUP(6, '0');

      /**
       * Maximum number of chars this field can contain.
       */
      final int maxLength;
      /**
       * Character to use for padding the field to it's maximum length
       */
      @Nullable
      final Character padChar;

      /**
       * Create a new instance setting the maximum length (in chars) of the
       * field.
       *
       * @param numChars Maximum chars this field can take
       * @param padChar Character to use for padding the field to it's maximum
       * length
       */
      Field(final int numChars, @Nullable final Character padChar) {
        this.maxLength = numChars;
        this.padChar = padChar;
      }
    }

    /**
     * Record data storage.
     */
    private final Map<Field, Object> data = new EnumMap<>(Field.class);

    /**
     * Get the IPC-code {@link Field fields} in parsing order.
     *
     * @return List of {@link Field fields} in the order they are parsed
     */
    public static List<Field> getFieldOrder() {
      return Arrays.asList(FIELDS_ORDER);
    }

    /**
     * Order of fields in a IPC-code.
     */
    static final Field[] FIELDS_ORDER = {
        Field.SECTION, Field.CLASS, Field.SUBCLASS, Field.MAINGROUP,
        Field.SUBGROUP
    };
    /**
     * Number of fields used to build an IPC-code string.
     */
    private static final int FIELDS_COUNT = FIELDS_ORDER.length;
    /**
     * Position of the separator char in the {@link #FIELDS_ORDER} array. The
     * separator gets inserted after this array index.
     */
    private static final int SEPARATOR_AFTER = 3;

    /**
     * Set a record field. The passed in data is only checked for {@code null}.
     * No validation is done and should be handled before.
     *
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
            // integer type
            case CLASS:
            case MAINGROUP:
            case SUBGROUP:
              // Parsing as integer removes leading zeros. This simplifies
              // comparing values of two records.
              this.data.put(f, Integer.valueOf(valueStr));
              state = true;
              break;
            // char type
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
     * Get a list of all fields that contain a value.
     *
     * @return Lst of fields set for this record
     */
    public Set<Field> getSetFields() {
      final Set<Field> fields = EnumSet.allOf(Field.class);

      for (final Field f : Field.values()) {
        if (this.data.get(f) == null) {
          fields.remove(f);
        }
      }

      return fields;
    }

    /**
     * Utility function to build a string representation of the current IPC-code
     * data.
     *
     * @param sb StringBuilder to append to
     * @param f Field name
     * @param pre Pre-value (may be null)
     * @param post Post-value (may be null)
     * @param pad If true, all fields will be padded to their maximum width
     * @return True, if a value was stored for the field
     */
    @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
    private boolean appendIfExists(
        @NotNull final StringBuilder sb,
        @NotNull final Field f,
        @Nullable final CharSequence pre,
        @Nullable final CharSequence post,
        final boolean pad) {
      final String fData = String.valueOf(this.data.get(f));

      if ("null".equalsIgnoreCase(fData)) {
        return false;
      } else {
        if (pre != null) {
          sb.append(pre);
        }
        if (pad && fData.length() < f.maxLength) {
          for (int i = 0; i < f.maxLength - fData.length(); i++) {
            sb.append(f.padChar);
          }
          sb.append(fData);
        } else {
          sb.append(fData);
        }
        if (post != null) {
          sb.append(post);
        }
        return true;
      }
    }

    /**
     * Test if a value is set for any field or this record is empty (no value is
     * set for any field).
     *
     * @return True, if no value is present for any field
     */
    public boolean isEmpty() {
      return !Arrays.stream(Field.values())
          .filter(f -> this.data.get(f) != null)
          .findFirst().isPresent();
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(60);

      for (int i = 0; i < FIELDS_COUNT; i++) {
        if (!appendIfExists(sb, FIELDS_ORDER[i],
            FIELDS_ORDER[i].name() + '=', " ", true)) {
          break;
        }
      }
      return StringUtils.upperCase(sb.toString());
    }

    /**
     * Return as much information as possible as formatted IPC-code. Uses the
     * {@link Parser#DEFAULT_SEPARATOR default} separator char.
     *
     * @return IPC-code
     * @see #toFormattedString(char)
     */
    public String toFormattedString() {
      return toFormattedString(Parser.DEFAULT_SEPARATOR);
    }

    /**
     * Return as much information as possible as formatted IPC-code.
     *
     * @param separator Separator char
     * @return IPC-code
     */
    public String toFormattedString(final char separator) {
      final StringBuilder sb = new StringBuilder(MAX_LENGTH);
      for (int i = 0; i < FIELDS_COUNT; i++) {
        if (!appendIfExists(sb, FIELDS_ORDER[i],
            i == SEPARATOR_AFTER + 1 ? Character.toString(separator) : null,
            null, true)) {
          break;
        }
      }
      return StringUtils.upperCase(sb.toString());
    }

    /**
     * Get the IPC-code as regular expression usable in {@link RegexpQuery
     * regular expression} queries. Uses the default {@link
     * Parser#DEFAULT_SEPARATOR separator} char.
     *
     * @return Regular expression usable in {@link RegexpQuery regular
     * expression} queries
     * @see #toRegExpString(char)
     */
    public String toRegExpString() {
      return toRegExpString(Parser.DEFAULT_SEPARATOR);
    }

    /**
     * Get the IPC-code as regular expression usable in {@link RegexpQuery
     * regular expression} queries and {@link Pattern} expressions.
     *
     * @param separator Separator char
     * @return Regular expression usable in {@link RegexpQuery regular
     * expression} queries
     */
    @SuppressWarnings("ImplicitNumericConversion")
    public String toRegExpString(final char separator) {
      final StringBuilder sb = new StringBuilder(MAX_LENGTH << 1);

      // mask separator char for usage in RegExp, if needed
      final String sep;
      if (separator == '\\' ||
          separator == '+' || separator == '-' || separator == '!' ||
          separator == '(' || separator == ')' ||
          separator == '[' || separator == ']' ||
          separator == '{' || separator == '}' ||
          separator == '<' || separator == '>' ||
          separator == '\"' || separator == '~' || separator == '#' ||
          separator == '*' || separator == '?' || separator == '@' ||
          separator == '|' || separator == '&' || separator == '^') {
        sep = "\\" + separator;
      } else {
        sep = String.valueOf(separator);
      }

      for (int i = 0; i < FIELDS_COUNT; i++) {
        final Object fData = this.data.get(FIELDS_ORDER[i]);

        if (fData == null) {
          break;
        }

        if (i == SEPARATOR_AFTER + 1) {
          sb.append(sep);
        }

        final String fStr = String.valueOf(fData);
        final int padAmount = FIELDS_ORDER[i].maxLength - fStr.length();

        if (padAmount > 0) {
          sb.append(FIELDS_ORDER[i].padChar)
              .append('{').append(0).append(',').append(padAmount)
              .append('}');
        }
        sb.append(fStr);
      }
      if (this.getSetFields().size() != FIELDS_COUNT) {
        sb.append(".*");
      }
      return sb.toString();
    }

    /**
     * Get a field of this record.
     *
     * @param f Field to get
     * @return Field value. Empty, if no field value was set.
     */

    public String get(@NotNull final Field f) {
      return this.data.get(f) == null ? "" : this.data.get(f).toString();
    }

    @Override
    public boolean equals(final Object other) {
      return this == other ||
          other != null && IPCRecord.class.isInstance(other) &&
              equals((IPCRecord) other, null);
    }

    /**
     * Check, if both records are equal using only the specified fields.
     * Comparison of field values ignores case.
     *
     * @param other Other record
     * @param fields Fields to compare
     * @return True, if all requested field values are equal, ignoring case
     */
    public boolean equals(
        @NotNull final IPCRecord other,
        @Nullable final Set<Field> fields) {
      final Set<Field> fieldsToCheck = fields == null ?
          EnumSet.allOf(Field.class) : fields;

      for (final Field f : fieldsToCheck) {
        if (!get(f).equalsIgnoreCase(other.get(f))) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return this.data.hashCode();
    }

    /**
     * Comparator for {@link IPCRecord}s.
     */
    public static final class IPCRecordComparator
        implements Comparator<IPCRecord>, Serializable {

      @Override
      public int compare(final IPCRecord o1, final IPCRecord o2) {
        if (o1 == null || o2 == null) {
          throw new NullPointerException();
        }

        if (o1.equals(o2)) {
          return 0;
        }

        for (final Field f : FIELDS_ORDER) {
          switch (f) {
            // integer type
            case CLASS:
            case MAINGROUP:
            case SUBGROUP:
              final String o1Val = o1.get(f);
              final String o2Val = o2.get(f);

              if (!o1Val.equalsIgnoreCase(o2Val)) {
                if (o1Val.isEmpty()) {
                  return -1;
                } else if (o2Val.isEmpty()) {
                  return 1;
                }
                // Parsing as integer removes leading zeros. This simplifies
                // comparing values of two records.
                final int state =
                    Integer.valueOf(o1Val).compareTo(Integer.valueOf(o2Val));
                if (state != 0) {
                  return state;
                }
              }
              break;
            // char type
            case SECTION:
            case SUBCLASS:
              final int state = o1.get(f).compareToIgnoreCase(o2.get(f));
              if (state != 0) {
                return state;
              }
              break;
          }
        }
        return 0;
      }
    }
  }

  /**
   * Configurable IPC-code parser.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Parser {
    /**
     * Regular expression to match a section identifier.
     */
    static final Pattern RX_SECTION = Pattern.compile("^[a-hA-H]$");
    /**
     * Regular expression to match a class identifier.
     */
    private static final Pattern RX_CLASS = Pattern.compile("^[0-9]{2}$");
    /**
     * Regular expression to match a subclass identifier.
     */
    static final Pattern RX_SUBCLASS = Pattern.compile("^[a-zA-Z]$");
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
     * Regular expression to match zero padded strings.
     */
    static final Pattern RX_INVALID_SEPARATOR =
        Pattern.compile("^[a-zA-Z0-9]$");

    /**
     * Get the separator char.
     *
     * @return Char used as separator for main- and sub-group
     */
    public char getSeparator() {
      return this.separator;
    }

    /**
     * Check, if zero padding of missing values is allowed.
     *
     * @return True, if zero padding is allowed
     */
    public boolean isAllowZeroPad() {
      return this.allowZeroPad;
    }

    /**
     * Separator char to use.
     */
    private char separator = DEFAULT_SEPARATOR;
    /**
     * If true, missing values may be indicated by a sequence of zeros.
     */
    private boolean allowZeroPad = false;

    /**
     * Set the character to use for separating main- and sub-group. Defaults to
     * {@link #DEFAULT_SEPARATOR}. Digits are not allowed.
     *
     * @param sep Non-digit separator char
     * @return Self reference
     */
    public Parser separatorChar(final char sep)
        throws InvalidIPCCodeException {
      checkSeparator(sep);
      this.separator = sep;
      return this;
    }

    /**
     * Check, if the given separator char is valid.
     *
     * @param sep Separator char
     */
    private static void checkSeparator(final char sep)
        throws InvalidIPCCodeException {
      if (RX_INVALID_SEPARATOR.matcher(Character.toString(sep)).matches()) {
        throw new InvalidIPCCodeException("Invalid separator character " +
            '\'' + sep + "'.");
      }
    }

    /**
     * Allows padding of missing information with zeros. If true, a code like
     * {@code C08K0000} and {@code C08K0000-00} will be read as {@code C08K}.
     *
     * @param flag If true, zero padding is allowed
     * @return Self reference
     */
    public Parser allowZeroPad(final boolean flag) {
      this.allowZeroPad = flag;
      return this;
    }

    /**
     * Tries to parse a given string as basic IPC-code with content as described
     * by WIPO Standard ST.8. Only the first 15 characters are parsed at
     * maximum. Further the parser is not strict, as it ignores any content that
     * follows a valid IPC-code.
     *
     * @param codeStr IPC-code as string
     * @return IPC code record object with all symbols set that could be parsed
     * from the input string
     * @see #parse(CharSequence, char, boolean)
     * @see #separatorChar(char)
     * @throws InvalidIPCCodeException Thrown, if the given IPC-code is invalid
     */
    public IPCRecord parse(@NotNull final CharSequence codeStr)
        throws InvalidIPCCodeException {
      return parse(codeStr, this.separator, this.allowZeroPad);
    }

    /**
     * Tries to parse a given string as basic IPC-code with content as described
     * by WIPO Standard ST.8.
     *
     * @param codeStr IPC-code as string
     * @param sep Char to use for separating main- and sub-group
     * @return IPC code record object with all symbols set that could be parsed
     * from the input string
     * @see #parse(CharSequence)
     * @see #parse(CharSequence, char, boolean)
     * @see #separatorChar(char)
     * @throws InvalidIPCCodeException Thrown, if an invalid separator-char
     * was given
     */
    static IPCRecord parse(
        @NotNull final CharSequence codeStr, final char sep)
        throws InvalidIPCCodeException {
      return parse(codeStr, sep, false);
    }

    /**
     * Tries to parse a given string as basic IPC-code with content as described
     * by WIPO Standard ST.8.
     *
     * @param codeStr IPC-code as string
     * @param sep Char to use for separating main- and sub-group
     * @param allowZeroPad If true, zero padding of missing values is allowed
     * @return IPC code record object with all symbols set that could be parsed
     * from the input string
     * @see #parse(CharSequence)
     * @see #separatorChar(char)
     * @throws InvalidIPCCodeException Thrown, if the given IPC-code is
     * invalid or the given separator-char is invalid
     */
    @SuppressWarnings("ReuseOfLocalVariable")
    static IPCRecord parse(
        @NotNull final CharSequence codeStr,
        final char sep, final boolean allowZeroPad)
        throws InvalidIPCCodeException {
      checkSeparator(sep);

      // fold spaces
      final String code = RX_SPACES.matcher(codeStr).replaceAll("");
      // length of whole code
      final int codeLength = code.length();
      // current position in string
      int pointer = 0;
      // final record builder
      final Builder record = new Builder();
      // flag indicating, if parsing has finished (in case of zero padding or
      // a parsed field is not valid).
      boolean notFinished = true;
      // matcher to detect zero padding
      final Matcher zeroPadMatcher = Pattern
          .compile("^0*" + sep + "?0*$").matcher(code);

      // section [1]
      notFinished = RX_SECTION.matcher(code)
          .region(pointer, pointer + 1).matches();
      if (notFinished) {
        record.setSection(code.charAt(pointer));
        pointer += 1;
      }

      if (notFinished) {
        if (allowZeroPad &&
            zeroPadMatcher.region(pointer, codeLength).matches()) {
          notFinished = false;
        } else
          // class [2-3]
          if (codeLength >= pointer + 1) {
            notFinished = RX_CLASS.matcher(code)
                .region(pointer, Math.min(pointer + 2, codeLength)).matches();
            if (notFinished) {
              record.setClass(code.substring(pointer, pointer + 2));
              pointer += 2;
            }
          } else {
            notFinished = false;
          }
      }

      if (notFinished) {
        if (allowZeroPad &&
            zeroPadMatcher.region(pointer, codeLength).matches()) {
          notFinished = false;
        } else
          // subclass [4]
          if (codeLength >= pointer + 1) {
            notFinished = RX_SUBCLASS.matcher(code)
                .region(pointer, pointer + 1).matches();
            if (notFinished) {
              record.setSubclass(code.substring(pointer, pointer + 1));
            }
            pointer += 1;
          } else {
            notFinished = false;
          }
      }

      // code may already be complete here
      if (notFinished) {
        if (allowZeroPad &&
            zeroPadMatcher.region(pointer, codeLength).matches()) {
          notFinished = false;
        } else if (codeLength > pointer) {
          // main group [5-8] or blank
          if (codeLength >= pointer + 1) {
            if (Character.compare(code.charAt(pointer), sep) != 0) {
              final Matcher mgm = RX_MAINGROUP.matcher(code)
                  .region(pointer, Math.min(pointer + 4, codeLength));
              if (mgm.matches()) {
                final String match = mgm.group(1);
                if (match != null && !match.isEmpty()) {
                  record.setMainGroup(match);
                  pointer += match.length();
                }
              } else {
                notFinished = false;
              }
            }
          }

          // separator char [9]
          if (notFinished && codeLength >= pointer + 1) {
            notFinished = Character.compare(code.charAt(pointer), sep) == 0;
            pointer += 1;
          }

          // subgroup [10-15] or blank
          if (notFinished && codeLength >= pointer + 1) {
            final Matcher sgm = RX_SUBGROUP.matcher(code)
                .region(pointer, Math.min(pointer + 4, codeLength));
            if (sgm.matches()) {
              final String match = sgm.group(1);
              if (match != null && !match.isEmpty()) {
                record.setSubGroup(match);
                //pointer += match.length();
              }
            }
//        else {
//          finished = true;
//        }
          }
        }
      }

      return record.build();
    }
  }

  /**
   * Tries to parse a IPC record from a string using the {@link
   * Parser#DEFAULT_SEPARATOR default} separator char.
   *
   * @param code IPC code
   * @return IPC data record extracted from the given string
   * @see Parser#parse(CharSequence, char)
   * @throws InvalidIPCCodeException Thrown, if the given IPC-code is invalid
   */
  public static IPCRecord parse(@NotNull final CharSequence code)
      throws InvalidIPCCodeException {
    return Parser.parse(code, Parser.DEFAULT_SEPARATOR);
  }

  /**
   * Tries to parse a given string as basic IPC-code with content as described
   * by WIPO Standard ST.8. Only the first 15 characters are parsed at maximum.
   * Further the parser is not strict, as it ignores any content that follows a
   * valid IPC-code.
   *
   * @param code IPC-code as string
   * @param separator Character to use for separating main- and sub-group
   * @return IPC code record object with all symbols set that could be parsed
   * from the input string
   * @see Parser#parse(CharSequence, char)
   * @throws InvalidIPCCodeException Thrown, if the given IPC-code is invalid
   */
  public static IPCRecord parse(
      @NotNull final CharSequence code,
      final char separator)
      throws InvalidIPCCodeException {
    return Parser.parse(code, separator);
  }

  @Nullable
  public static Character detectSeparator(@NotNull final CharSequence code) {
    final Matcher m = Parser.RX_INVALID_SEPARATOR.matcher(code);
    for (int i = 0; i < code.length(); i++) {
      if (!m.region(i, i + 1).matches()) {
        return code.charAt(i);
      }
    }
    return null;
  }

  /**
   * Builder for {@link IPCRecord} instances.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder
      implements Buildable<IPCRecord> {
    /**
     * Final record.
     */
    private final IPCRecord rec = new IPCRecord();

    /**
     * Set the class identifier.
     *
     * @param cls Any object whose value can be parsed to an int between >=1 and
     * <= 99
     * @return Self reference
     * @throws InvalidIPCCodeException Thrown, if the given IPC-class is invalid
     */
    public Builder setClass(@NotNull final Object cls)
        throws InvalidIPCCodeException {
      return Number.class.isInstance(cls) ?
          setClass(((Number) cls).intValue()) :
          setClass(Integer.parseInt(cls.toString()));
    }

    /**
     * Set the class identifier.
     *
     * @param cls Number >=1 and <= 99
     * @return Self reference
     * @throws InvalidIPCCodeException Thrown, if the given IPC-class is invalid
     */
    public Builder setClass(final int cls)
        throws InvalidIPCCodeException {
      if (cls < 1 || cls > 99) {
        throw new InvalidIPCCodeException(
            "Class identifier must be >=1 and <= 99.");
      }
      this.rec.set(Field.CLASS, cls);
      return this;
    }

    /**
     * Set the main-group identifier.
     *
     * @param mg Number >=1 and <= 9999
     * @return Self reference
     * @throws InvalidIPCCodeException Thrown, if the given maingroup-code is
     * invalid
     */
    public Builder setMainGroup(final int mg)
        throws InvalidIPCCodeException {
      if (mg < 1 || mg > 9999) {
        throw new InvalidIPCCodeException(
            "Main-group identifier must be >=1 and <= 9999. Got " + mg + '.');
      }
      this.rec.set(Field.MAINGROUP, mg);
      return this;
    }

    /**
     * Set the main-group identifier.
     *
     * @param mg Any object whose value can be parsed to an int between >=1 and
     * <= 9999
     * @return Self reference
     * @throws InvalidIPCCodeException Thrown, if the given maingroup-code is
     * invalid
     */
    public Builder setMainGroup(@NotNull final Object mg)
        throws InvalidIPCCodeException {
      return Number.class.isInstance(mg) ?
          setMainGroup(((Number) mg).intValue()) :
          setMainGroup(Integer.parseInt(mg.toString()));
    }

    /**
     * Set the section identifier.
     *
     * @param sec Any object whose value can be parsed to an char between a-h.
     * @return Self reference
     * @throws InvalidIPCCodeException Thrown, if the given section-code is
     * invalid
     */
    public Builder setSection(@NotNull final Object sec)
        throws InvalidIPCCodeException {
      return setSection(sec.toString().charAt(0));
    }

    /**
     * Set the section identifier identifier.
     *
     * @param sec Char between a-h
     * @return Self reference
     * @throws InvalidIPCCodeException Thrown, if the given section-code is
     * invalid
     */
    public Builder setSection(final char sec)
        throws InvalidIPCCodeException {
      final String secStr = String.valueOf(sec);
      if (!Parser.RX_SECTION.matcher(secStr).matches()) {
        throw new InvalidIPCCodeException(
            "Section identifier must be between a-h. Got " + sec + '.');
      }
      this.rec.set(Field.SECTION, secStr);
      return this;
    }

    /**
     * Set the subclass identifier.
     *
     * @param scls Any object whose value can be parsed to an char between a-z.
     * @return Self reference
     * @throws InvalidIPCCodeException Thrown, if the given subclass-code is
     * invalid
     */
    public Builder setSubclass(@NotNull final Object scls)
        throws InvalidIPCCodeException {
      return setSubclass(scls.toString().charAt(0));
    }

    /**
     * Set the section identifier identifier.
     *
     * @param scls Char between a-h
     * @return Self reference
     * @throws InvalidIPCCodeException Thrown, if the given subclass-code is
     * invalid
     */
    public Builder setSubclass(final char scls)
        throws InvalidIPCCodeException {
      final String sclsStr = String.valueOf(scls);
      if (!Parser.RX_SUBCLASS.matcher(sclsStr).matches()) {
        throw new InvalidIPCCodeException(
            "Subclass identifier must be between a-z. Got " + scls + '.');
      }
      this.rec.set(Field.SUBCLASS, sclsStr);
      return this;
    }

    /**
     * Set the sub-group identifier.
     *
     * @param sg Number >=0 and <= 999999
     * @return Self reference
     * @throws InvalidIPCCodeException Thrown, if the given subgroup-code is
     * invalid
     */
    public Builder setSubGroup(final int sg)
        throws InvalidIPCCodeException {
      if (sg < 0 || sg > 999999) {
        throw new InvalidIPCCodeException(
            "Sub-group identifier must be >=0 and <= 999999. Got " + sg + '.');
      }
      this.rec.set(Field.SUBGROUP, sg);
      return this;
    }

    /**
     * Set the sub-group identifier.
     *
     * @param sg Any object whose value can be parsed to an int between >=0 and
     * <= 999999
     * @return Self reference
     * @throws InvalidIPCCodeException Thrown, if the given subgroup-code is
     * invalid
     */
    public Builder setSubGroup(@NotNull final Object sg)
        throws InvalidIPCCodeException {
      return Number.class.isInstance(sg) ?
          setSubGroup(((Number) sg).intValue()) :
          setSubGroup(Integer.parseInt(sg.toString()));
    }

    @NotNull
    @Override
    public IPCRecord build() {
      return this.rec;
    }
  }
}
