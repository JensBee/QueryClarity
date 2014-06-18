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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

/**
 * Test for {@link SerializableByte}.
 *
 * @author Jens Bertram
 */
public final class SerializableByteTest {

  @SuppressWarnings("ObjectEquality")
  @Test
  public void testSerialize()
      throws Exception {
    final SerializableByte original = new SerializableByte((byte) 1);

    final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    final DataOutput dos = new DataOutputStream(baos);

    SerializableByte.SERIALIZER.serialize(dos, original);

    final DataInput dis =
        new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    final SerializableByte deserialized =
        SerializableByte.SERIALIZER.deserialize(dis, -1);

    Assert.assertTrue("Deserialized not equal to original.",
        original.equals(deserialized));
    Assert.assertFalse("Deserialized and original are the same.",
        original == deserialized);
    Assert
        .assertEquals("Deserialized and original are not same value (compare).",
            0L, (long) original.compareTo(deserialized));
    Assert.assertEquals(
        "Deserialized and original are not same value (comparator).",
        0L, (long) SerializableByte.COMPARATOR.compare(original, deserialized));
    Assert.assertTrue("Stored bytes are not the same.",
        original.value == deserialized.value);
    Assert.assertEquals("Hash code differs.", (long) original.hashCode(),
        (long) deserialized.hashCode());
  }
}