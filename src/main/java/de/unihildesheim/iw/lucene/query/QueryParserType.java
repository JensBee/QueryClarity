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

import org.jetbrains.annotations.Nullable;

/**
 * Types of query parsers available.
 */
public enum QueryParserType {
  /**
   * {@link TryExactTermsQuery} instance.
   */
  TEQ("Try Exact Terms Query", TryExactTermsQuery.class),
  /**
   * {@link RelaxableCommonTermsQuery} instance.
   */
  CTQ("Common Terms Query", RelaxableCommonTermsQuery.class);

  /**
   * Current parser name.
   */
  private final String name;
  /**
   * Current parsers class.
   */
  private final Class<? extends RelaxableQuery> clazz;

  /**
   * Create parser type instance.
   *
   * @param qName Name of the parser
   */
  QueryParserType(final String qName, Class<? extends RelaxableQuery> qClass) {
    this.name = qName;
    this.clazz = qClass;
  }

  /**
   * Get the name of the parser.
   *
   * @return Parser's name
   */
  public String toString() {
    return this.name;
  }

  /**
   * Get the parser's class
   * @return Class
   */
  public Class<? extends RelaxableQuery> getQClass() {
    return clazz;
  }

  /**
   * Get a {@link QueryParserType} by name.
   *
   * @param name Name to identify the parser to get
   * @return Parser instance, or {@code null} if none was found
   */
  @Nullable
  public static QueryParserType getByName(final String name) {
    for (final QueryParserType qpt : values()) {
      if (qpt.name().equalsIgnoreCase(name)) {
        return qpt;
      }
    }
    return null;
  }

  /**
   * Get a {@link QueryParserType} class by name.
   *
   * @param name Name to identify the parser to get
   * @return Parser class, or {@code null} if none was found
   */
  @Nullable
  public static Class<? extends RelaxableQuery> getClassByName(
      final String name) {
    for (final QueryParserType qpt : values()) {
      if (qpt.name().equalsIgnoreCase(name)) {
        return qpt.clazz;
      }
    }
    return null;
  }
}
