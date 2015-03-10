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

package de.unihildesheim.iw.mapdb.serializer;

import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.mapdb.serializer.BytesRefSerializer.Serializer;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

/**
 * Test for {@link BytesRefSerializer}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class BytesRefSerializerTest
    extends TestCase {

  public BytesRefSerializerTest() {
    super(LoggerFactory.getLogger(BytesRefSerializerTest.class));
  }

  @Test
  public void testSerialize()
      throws Exception {
    final BytesRef br = new BytesRef("foo");
    @SuppressWarnings("TypeMayBeWeakened")
    final Serializer ser = new Serializer();
    final ByteArrayOutputStream bouts = new ByteArrayOutputStream();
    @SuppressWarnings("resource")
    final DataOutput out = new DataOutputStream(bouts);
    ser.serialize(out, br);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testSerialize_null()
      throws Exception {
    @SuppressWarnings("TypeMayBeWeakened")
    final Serializer ser = new Serializer();
    final ByteArrayOutputStream bouts = new ByteArrayOutputStream();
    @SuppressWarnings("resource")
    final DataOutput out = new DataOutputStream(bouts);

    try {
      ser.serialize(out, null);
      Assert.fail("No NullpointerException thrown.");
    } catch (final NullPointerException e) {
      // pass
    }
  }

  @Test
  public void testSerialize_empty()
      throws Exception {
    final BytesRef br = new BytesRef("");
    @SuppressWarnings("TypeMayBeWeakened")
    final Serializer ser = new Serializer();
    final ByteArrayOutputStream bouts = new ByteArrayOutputStream();
    @SuppressWarnings("resource")
    final DataOutput out = new DataOutputStream(bouts);

    ser.serialize(out, br);
  }

  @Test
  public void testDeserialize()
      throws Exception {
    final BytesRef brRef = new BytesRef("foo");
    @SuppressWarnings("TypeMayBeWeakened")
    final Serializer ser = new Serializer();
    final ByteArrayOutputStream bouts = new ByteArrayOutputStream();
    @SuppressWarnings("resource")
    final DataOutput out = new DataOutputStream(bouts);
    ser.serialize(out, brRef);

    final byte[] bytes = bouts.toByteArray();

    final ByteArrayInputStream bins = new ByteArrayInputStream(bytes);
    @SuppressWarnings({"TypeMayBeWeakened", "resource"})
    final DataInputStream in = new DataInputStream(bins);
    final BytesRef brDes = ser.deserialize(in, bytes.length);

    Assert.assertTrue("Equals mismatch.", brRef.bytesEquals(brDes));
  }

  @Test
  public void testDeserialize_empty()
      throws Exception {
    final BytesRef brRef = new BytesRef("");
    @SuppressWarnings("TypeMayBeWeakened")
    final Serializer ser = new Serializer();
    final ByteArrayOutputStream bouts = new ByteArrayOutputStream();
    @SuppressWarnings("resource")
    final DataOutput out = new DataOutputStream(bouts);
    ser.serialize(out, brRef);

    final byte[] bytes = bouts.toByteArray();

    final ByteArrayInputStream bins = new ByteArrayInputStream(bytes);
    @SuppressWarnings({"TypeMayBeWeakened", "resource"})
    final DataInputStream in = new DataInputStream(bins);
    final BytesRef brDes = ser.deserialize(in, bytes.length);

    Assert.assertTrue("Equals mismatch.", brRef.bytesEquals(brDes));
  }
}