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

package de.unihildesheim.iw;

import de.unihildesheim.iw.util.ByteArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

/**
 * Test for {@link ByteArray}.
 *
 * @author Jens Bertram
 */
public class ByteArrayTest
    extends TestCase {

  @SuppressWarnings("ArrayEquality")
  @Test
  public void testClone()
      throws Exception {
    final ByteArray original = new ByteArray("test".getBytes("UTF-8"));
    final ByteArray cloned = original.clone();

    Assert.assertTrue("Clone not equal to original.",
        original.equals(cloned));
    Assert.assertFalse("Clone and original are the same.",
        original == cloned);
    Assert.assertEquals("Clone and original are not same value (compare).",
        0L, (long) original.compareTo(cloned));
    Assert.assertEquals("Clone and original are not same value (comparator).",
        0L, (long) ByteArray.COMPARATOR.compare(original, cloned));
    Assert.assertFalse("Stored bytes are same reference.",
        original.bytes == cloned.bytes);
    Assert.assertEquals("String result differs.", ByteArrayUtils.utf8ToString
        (original), ByteArrayUtils.utf8ToString(cloned));
    Assert.assertEquals("Hash code differs.", (long) original.hashCode(),
        (long) cloned.hashCode());
  }

  @SuppressWarnings("ObjectEquality")
  @Test
  public void testSerialize()
      throws Exception {
    final ByteArray original = new ByteArray("test".getBytes("UTF-8"));

    final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    final DataOutput dos = new DataOutputStream(baos);

    ByteArray.SERIALIZER.serialize(dos, original);

    final DataInput dis =
        new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    final ByteArray deserialized = ByteArray.SERIALIZER.deserialize(dis,
        -1);

    Assert.assertTrue("Deserialized not equal to original.",
        original.equals(deserialized));
    Assert.assertFalse("Deserialized and original are the same.",
        original == deserialized);
    Assert
        .assertEquals("Deserialized and original are not same value (compare).",
            0L, (long) original.compareTo(deserialized));
    Assert.assertEquals(
        "Deserialized and original are not same value (comparator).",
        0L, (long) ByteArray.COMPARATOR.compare(original, deserialized));
    Assert.assertFalse("Stored bytes are same reference.",
        original.bytes == deserialized.bytes);
    Assert.assertEquals("String result differs.", ByteArrayUtils.utf8ToString
        (original), ByteArrayUtils.utf8ToString(deserialized));
    Assert.assertEquals("Hash code differs.", (long) original.hashCode(),
        (long) deserialized.hashCode());
  }
}