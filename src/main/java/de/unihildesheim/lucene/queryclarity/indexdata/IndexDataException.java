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
package de.unihildesheim.lucene.queryclarity.indexdata;

import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class IndexDataException extends Exception {

  /**
   * Type of this exception. See {@link Type}.
   */
  private final Type exceptionType;

  /**
   * Different types of exceptions and messages associated with them.
   */
  public enum Type {

    /**
     * One or more requested fields are not found in the current lucene index,
     * or are not indexed.
     */
    FIELD_NOT_PRESENT("Not all requested index fields ({}) "
            + "are present as in indexed fields in index ({}).");

    /**
     * Message associated with this exception type.
     */
    private final String message;

    /**
     * Create a new exception type with the given message for log output.
     *
     * @param msg Message to present to user
     */
    private Type(final String msg) {
      this.message = msg;
    }

    /**
     * Get the error message associated with this exception type.
     *
     * @return Error message associated with this exception type
     */
    public String getMessage() {
      return this.message;
    }
  }

  /**
   * Format the error message associated with this exception.
   *
   * @param msg Message to format
   * @param data Objects to use formatting this message
   * @return Formattet message string
   */
  private static String getFormattedMessage(final String msg,
          final Object... data) {
    final FormattingTuple tuple = MessageFormatter.arrayFormat(msg, data);
    return tuple.getMessage();
  }

  /**
   * Creates a new exception with the given type and data.
   *
   * @param type Exception type
   * @param data Data to feed to the error message displayed to user
   */
  public IndexDataException(final Type type, final Object... data) {
    super(IndexDataException.getFormattedMessage(type.getMessage(), data));
    this.exceptionType = type;
  }

  /**
   * Get the type of this exception.
   * @return Exception type
   */
  public final Type getType() {
    return this.exceptionType;
  }
}
