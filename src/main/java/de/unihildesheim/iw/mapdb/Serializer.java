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

package de.unihildesheim.iw.mapdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Classes wrapping the default MapDb serializers and all {@code null} checks
 * for values. Meant for debugging and should not be used in production.
 *
 * @author Jens Bertram
 */
public final class Serializer {

  /**
   * Wrapped MapDB {@link org.mapdb.Serializer Serializer} with added {@code
   * null check}.
   *
   * @see org.mapdb.Serializer#INTEGER
   */
  public static final org.mapdb.Serializer INTEGER = new IntegerSerializer();
  /**
   * Wrapped MapDB {@link org.mapdb.Serializer Serializer} with added {@code
   * null check}.
   *
   * @see org.mapdb.Serializer#LONG
   */
  public static final org.mapdb.Serializer LONG = new LongSerializer();
  /**
   * Wrapped MapDB {@link org.mapdb.Serializer Serializer} with added {@code
   * null check}.
   *
   * @see org.mapdb.Serializer#STRING
   */
  public static final org.mapdb.Serializer STRING = new StringSerializer();
  /**
   * Wrapped MapDB {@link org.mapdb.Serializer Serializer} with added {@code
   * null check}.
   *
   * @see org.mapdb.Serializer#STRING_INTERN
   */
  public static final org.mapdb.Serializer STRING_INTERN = new
      StringInternSerializer();

  /**
   * Logger instance for this class.
   */
  static final Logger LOG = LoggerFactory.getLogger(Serializer.class);

  /**
   * Wraps MapDB {@link org.mapdb.Serializer#INTEGER Integer} serializer. Throws
   * an exception, if {@code null} value is provided.
   */
  private static final class IntegerSerializer
      implements Serializable, org.mapdb.Serializer<Integer> {
    @Override
    public void serialize(final DataOutput out, final Integer value)
        throws IOException {
      if (value == null) {
        throw new IllegalArgumentException("Value was null.");
      }
      org.mapdb.Serializer.INTEGER.serialize(out, value);
    }

    @Override
    public Integer deserialize(final DataInput in, final int available)
        throws IOException {
      Integer value = org.mapdb.Serializer.INTEGER.deserialize(in, available);
      if (value == null) {
        LOG.warn("Deserialized null integer!");
      }
      return value;
    }

    @Override
    public int fixedSize() {
      return org.mapdb.Serializer.INTEGER.fixedSize();
    }
  }

  /**
   * Wraps MapDB {@link org.mapdb.Serializer#LONG Long} serializer. Throws an
   * exception, if {@code null} value is provided.
   */
  private static final class LongSerializer
      implements Serializable, org.mapdb.Serializer<Long> {
    @Override
    public void serialize(final DataOutput out, final Long value)
        throws IOException {
      if (value == null) {
        throw new IllegalArgumentException("Value was null.");
      }
      org.mapdb.Serializer.LONG.serialize(out, value);
    }

    @Override
    public Long deserialize(final DataInput in, final int available)
        throws IOException {
      final Long value = org.mapdb.Serializer.LONG.deserialize(in, available);
      if (value == null) {
        LOG.warn("Deserialized null long!");
      }
      return value;
    }

    @Override
    public int fixedSize() {
      return org.mapdb.Serializer.LONG.fixedSize();
    }
  }

  /**
   * Wraps MapDB {@link org.mapdb.Serializer#STRING String} serializer. Throws
   * an exception, if {@code null} value is provided.
   */
  private static final class StringSerializer
      implements Serializable, org.mapdb.Serializer<String> {
    @Override
    public void serialize(final DataOutput out, final String value)
        throws IOException {
      if (value == null) {
        throw new IllegalArgumentException("Value was null.");
      }
      org.mapdb.Serializer.STRING.serialize(out, value);
    }

    @Override
    public String deserialize(final DataInput in, final int available)
        throws IOException {
      final String value =
          org.mapdb.Serializer.STRING.deserialize(in, available);
      if (value == null) {
        LOG.warn("Deserialized null string!");
      }
      return value;
    }

    @Override
    public int fixedSize() {
      return org.mapdb.Serializer.STRING.fixedSize();
    }
  }

  /**
   * Wraps MapDB {@link org.mapdb.Serializer#STRING_INTERN String Intern}
   * serializer. Throws an exception, if {@code null} value is provided.
   */
  private static final class StringInternSerializer
      implements Serializable, org.mapdb.Serializer<String> {
    @Override
    public void serialize(final DataOutput out, final String value)
        throws IOException {
      if (value == null) {
        throw new IllegalArgumentException("Value was null.");
      }
      org.mapdb.Serializer.STRING_INTERN.serialize(out, value);
    }

    @Override
    public String deserialize(final DataInput in, final int available)
        throws IOException {
      final String value = org.mapdb.Serializer.STRING_INTERN.deserialize(in,
          available);
      if (value == null) {
        LOG.warn("Deserialized null string!");
      }
      return value;
    }

    @Override
    public int fixedSize() {
      return org.mapdb.Serializer.STRING_INTERN.fixedSize();
    }
  }

  ;
}
