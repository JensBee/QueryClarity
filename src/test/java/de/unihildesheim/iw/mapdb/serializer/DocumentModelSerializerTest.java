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
import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.document.DocumentModel.Builder;
import de.unihildesheim.iw.mapdb.serializer.DocumentModelSerializer.Serializer;
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
 * Test for {@link DocumentModelSerializer}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class DocumentModelSerializerTest
    extends TestCase {
  public DocumentModelSerializerTest() {
    super(LoggerFactory.getLogger(DocumentModelSerializerTest.class));
  }

  @Test
  public void testSerialize()
      throws Exception {
    @SuppressWarnings("TypeMayBeWeakened")
    final Serializer ser = new Serializer();
    final ByteArrayOutputStream bouts = new ByteArrayOutputStream();
    final DataOutput out = new DataOutputStream(bouts);
    final Builder dmb = new Builder(1);
    dmb.setTermFrequency(new BytesRef("foo"), 12L);
    dmb.setTermFrequency(new BytesRef("bar"), 4L);
    dmb.setTermFrequency(new BytesRef("baz"), 32L);
    ser.serialize(out, dmb.build());
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testSerialize_null()
      throws Exception {
    @SuppressWarnings("TypeMayBeWeakened")
    final Serializer ser = new Serializer();
    final ByteArrayOutputStream bouts = new ByteArrayOutputStream();
    final DataOutput out = new DataOutputStream(bouts);
    try {
      ser.serialize(out, null);
      Assert.fail("Expected IllegalArgumentException to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testDeserialize()
      throws Exception {
    // document id
    final int dom_id = 1;
    // number of unique terms
    final int dom_termCount = 3;
    // total term frequency
    final long dom_tf = 48L;
    // model
    final Builder dmb = new Builder(dom_id);
    dmb.setTermFrequency(new BytesRef("foo"), 12L);
    dmb.setTermFrequency(new BytesRef("bar"), 4L);
    dmb.setTermFrequency(new BytesRef("baz"), 32L);

    @SuppressWarnings("TypeMayBeWeakened")
    final Serializer ser = new Serializer();
    final ByteArrayOutputStream bouts = new ByteArrayOutputStream();
    final DataOutput out = new DataOutputStream(bouts);

    final DocumentModel ref = dmb.build();
    ser.serialize(out, ref);
    final byte[] bytes = bouts.toByteArray();

    final DocumentModel dom;
    final ByteArrayInputStream bins = new ByteArrayInputStream(bytes);
    @SuppressWarnings({"TypeMayBeWeakened", "resource"})
    final DataInputStream in = new DataInputStream(bins);
    dom = ser.deserialize(in, bytes.length);

    Assert.assertEquals("Document id differs.", dom_id, dom.id);
    Assert.assertEquals("Unique term count differs.",
        dom_termCount, dom.termCount());

    Assert.assertEquals("Term frequency value differs.",
        12L, dom.tf(new BytesRef("foo")));
    Assert.assertEquals("Term frequency value differs.",
        4L, dom.tf(new BytesRef("bar")));
    Assert.assertEquals("Term frequency value differs.",
        32L, dom.tf(new BytesRef("baz")));

    Assert.assertEquals("Total term frequency differs.", dom_tf, dom.tf());

    Assert.assertEquals("Relative term frequency value differs.",
        (double) 12L / (double) dom_tf, dom.relTf(new BytesRef("foo")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 4L / (double) dom_tf, dom.relTf(new BytesRef("bar")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 32L / (double) dom_tf, dom.relTf(new BytesRef("baz")), 0d);

    Assert.assertEquals("Equals returns false.", ref, dom);
  }
}