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

package de.unihildesheim.iw.lucene.document;

import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.lucene.document.DocumentModel.Builder;
import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for {@link DocumentModel}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class DocumentModelTest
    extends TestCase {
  public DocumentModelTest() {
    super(LoggerFactory.getLogger(DocumentModelTest.class));
  }

  @Test
  public void builder_testSetTermFrequency()
      throws Exception {
    final int dom_id = 1;
    final Builder builder = new Builder(dom_id);
    builder.setTermFrequency(new BytesRef("foo"), 12L);
    builder.setTermFrequency(new BytesRef("bar"), 10L);
    builder.setTermFrequency(new BytesRef("baz"), 3L);

    final DocumentModel dom = builder.getModel();

    Assert.assertEquals("Term frequency differs.",
        12L, dom.tf(new BytesRef("foo")));
    Assert.assertEquals("Term frequency differs.",
        10L, dom.tf(new BytesRef("bar")));
    Assert.assertEquals("Term frequency differs.",
        3L, dom.tf(new BytesRef("baz")));
  }

  @Test
  public void builder_testSetTermFrequency_nonUnique()
      throws Exception {
    final int dom_id = 1;
    final Builder builder = new Builder(dom_id);
    builder.setTermFrequency(new BytesRef("foo"), 12L);
    builder.setTermFrequency(new BytesRef("bar"), 10L);
    builder.setTermFrequency(new BytesRef("baz"), 3L);

    try {
      builder.setTermFrequency(new BytesRef("foo"), 22L);
      Assert.fail("Non unique term was successful added. " +
          "Expected an exception to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void builder_testSetTermFrequency_lt0()
      throws Exception {
    final int dom_id = 1;
    final Builder builder = new Builder(dom_id);
    builder.setTermFrequency(new BytesRef("foo"), 12L);
    builder.setTermFrequency(new BytesRef("bar"), 10L);

    try {
      builder.setTermFrequency(new BytesRef("baz"), -1L);
      Assert.fail("Term with negative term frequency value was successful " +
          "added. Expected an exception to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void builder_testSetTermFrequency_eq0()
      throws Exception {
    // document id
    final int dom_id = 1;
    // number of unique terms
    final int dom_termCount = 2; // one is zero
    // total term frequency
    final long dom_tf = 22L;

    final Builder builder = new Builder(dom_id);
    builder.setTermFrequency(new BytesRef("foo"), 12L);
    builder.setTermFrequency(new BytesRef("bar"), 10L);
    builder.setTermFrequency(new BytesRef("baz"), 0L);

    final DocumentModel dom = builder.getModel();

    Assert.assertEquals("Unique term count differs.",
        dom_termCount, dom.termCount());

    Assert.assertEquals("Term frequency differs.",
        12L, dom.tf(new BytesRef("foo")));
    Assert.assertEquals("Term frequency differs.",
        10L, dom.tf(new BytesRef("bar")));
    Assert.assertEquals("Term frequency differs.",
        0L, dom.tf(new BytesRef("baz")));

    Assert.assertEquals("Relative term frequency value differs.",
        (double) 12L / (double) dom_tf, dom.relTf(new BytesRef("foo")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 10L / (double) dom_tf, dom.relTf(new BytesRef("bar")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 0L / (double) dom_tf, dom.relTf(new BytesRef("baz")), 0d);

    Assert.assertEquals("Total term frequency differs.", dom_tf, dom.tf());
  }

  @Test
  public void builder_testSetTermFrequencyMap()
      throws Exception {
    // document id
    final int dom_id = 1;
    // number of unique terms
    final int dom_termCount = 2; // one is zero
    // total term frequency
    final long dom_tf = 25L;

    final Builder builder = new Builder(dom_id);

    final Map<BytesRef, Long> tfMap = new HashMap<>(6);
    tfMap.put(new BytesRef("foo"), 12L);
    tfMap.put(new BytesRef("bar"), 10L);
    tfMap.put(new BytesRef("baz"), 3L);

    builder.setTermFrequency(tfMap);
    final DocumentModel dom = builder.getModel();

    Assert.assertEquals("Term frequency differs.",
        12L, dom.tf(new BytesRef("foo")));
    Assert.assertEquals("Term frequency differs.",
        10L, dom.tf(new BytesRef("bar")));
    Assert.assertEquals("Term frequency differs.",
        3L, dom.tf(new BytesRef("baz")));

    Assert.assertEquals("Relative term frequency value differs.",
        (double) 12L / (double) dom_tf, dom.relTf(new BytesRef("foo")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 10L / (double) dom_tf, dom.relTf(new BytesRef("bar")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 3L / (double) dom_tf, dom.relTf(new BytesRef("baz")), 0d);

    Assert.assertEquals("Total term frequency differs.", dom_tf, dom.tf());
  }

  @Test
  public void builder_testSetTermFrequencyMap_eq0()
      throws Exception {
    // document id
    final int dom_id = 1;
    // number of unique terms
    final int dom_termCount = 2; // one is zero
    // total term frequency
    final long dom_tf = 22L;
    final Builder builder = new Builder(dom_id);

    final Map<BytesRef, Long> tfMap = new HashMap<>(6);
    tfMap.put(new BytesRef("foo"), 12L);
    tfMap.put(new BytesRef("bar"), 10L);
    tfMap.put(new BytesRef("baz"), 0L);

    builder.setTermFrequency(tfMap);
    final DocumentModel dom = builder.getModel();

    Assert.assertEquals("Term frequency differs.",
        12L, dom.tf(new BytesRef("foo")));
    Assert.assertEquals("Term frequency differs.",
        10L, dom.tf(new BytesRef("bar")));
    Assert.assertEquals("Term frequency differs.",
        0L, dom.tf(new BytesRef("baz")));

    Assert.assertEquals("Relative term frequency value differs.",
        (double) 12L / (double) dom_tf, dom.relTf(new BytesRef("foo")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 10L / (double) dom_tf, dom.relTf(new BytesRef("bar")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 0L / (double) dom_tf, dom.relTf(new BytesRef("baz")), 0d);

    Assert.assertEquals("Total term frequency differs.", dom_tf, dom.tf());
  }

  @Test
  public void builder_testSetTermFrequencyMap_lt0()
      throws Exception {
    final int dom_id = 1;
    final Builder builder = new Builder(dom_id);

    final Map<BytesRef, Long> tfMap = new HashMap<>(6);
    tfMap.put(new BytesRef("foo"), 12L);
    tfMap.put(new BytesRef("bar"), 10L);
    tfMap.put(new BytesRef("baz"), -3L);

    try {
      builder.setTermFrequency(tfMap);
      Assert.fail("Term with negative term frequency value was successful " +
          "added. Expected an exception to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @Test
  public void builder_testSetTermFrequencyMap_nullValue()
      throws Exception {
    final int dom_id = 1;
    final Builder builder = new Builder(dom_id);

    final Map<BytesRef, Long> tfMap = new HashMap<>(6);
    tfMap.put(new BytesRef("foo"), 12L);
    tfMap.put(null, 10L);
    tfMap.put(new BytesRef("baz"), -3L);

    try {
      builder.setTermFrequency(tfMap);
      Assert.fail("Null term was successful added. " +
          "Expected an exception to be thrown.");
    } catch (final IllegalArgumentException e) {
      // pass
    }
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void builder_testGetModel()
      throws Exception {
    // document id
    final int dom_id = 1;
    // number of unique terms
    final int dom_termCount = 3;
    // total term frequency
    final long dom_tf = 25L;

    // model data
    final Builder builder = new Builder(dom_id);
    builder.setTermFrequency(new BytesRef("foo"), 12L);
    builder.setTermFrequency(new BytesRef("bar"), 10L);
    builder.setTermFrequency(new BytesRef("baz"), 3L);

    final DocumentModel dom = builder.getModel();

    Assert.assertEquals("Document id differs.", dom_id, dom.id);
    Assert.assertEquals("Unique term count differs.",
        dom_termCount, dom.termCount());

    Assert.assertEquals("Term frequency differs.",
        12L, dom.tf(new BytesRef("foo")));
    Assert.assertEquals("Term frequency differs.",
        10L, dom.tf(new BytesRef("bar")));
    Assert.assertEquals("Term frequency differs.",
        3L, dom.tf(new BytesRef("baz")));

    Assert.assertEquals("Relative term frequency value differs.",
        (double) 12L / (double) dom_tf, dom.relTf(new BytesRef("foo")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 10L / (double) dom_tf, dom.relTf(new BytesRef("bar")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 3L / (double) dom_tf, dom.relTf(new BytesRef("baz")), 0d);

    Assert.assertEquals("Total term frequency differs.", dom_tf, dom.tf());
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void builder_testGetModelMap()
      throws Exception {
    // document id
    final int dom_id = 1;
    // number of unique terms
    final int dom_termCount = 3;
    // total term frequency
    final long dom_tf = 25L;

    final Builder builder = new Builder(dom_id);

    // model data
    final Map<BytesRef, Long> tfMap = new HashMap<>(6);
    tfMap.put(new BytesRef("foo"), 12L);
    tfMap.put(new BytesRef("bar"), 10L);
    tfMap.put(new BytesRef("baz"), 3L);

    builder.setTermFrequency(tfMap);
    final DocumentModel dom = builder.getModel();

    Assert.assertEquals("Document id differs.", dom_id, dom.id);
    Assert.assertEquals("Unique term count differs.",
        dom_termCount, dom.termCount());

    Assert.assertEquals("Term frequency differs.",
        12L, dom.tf(new BytesRef("foo")));
    Assert.assertEquals("Term frequency differs.",
        10L, dom.tf(new BytesRef("bar")));
    Assert.assertEquals("Term frequency differs.",
        3L, dom.tf(new BytesRef("baz")));

    Assert.assertEquals("Total term frequency differs.", dom_tf, dom.tf());

    Assert.assertEquals("Relative term frequency value differs.",
        (double) 12L / (double) dom_tf, dom.relTf(new BytesRef("foo")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 10L / (double) dom_tf, dom.relTf(new BytesRef("bar")), 0d);
    Assert.assertEquals("Relative term frequency value differs.",
        (double) 3L / (double) dom_tf, dom.relTf(new BytesRef("baz")), 0d);
  }

  @Test
  public void testWriteObject()
      throws Exception {
    final ByteArrayOutputStream bouts = new ByteArrayOutputStream();
    @SuppressWarnings("resource")
    final ObjectOutputStream out = new ObjectOutputStream(bouts);

    final Builder dmb = new Builder(1);
    dmb.setTermFrequency(new BytesRef("foo"), 12L);
    dmb.setTermFrequency(new BytesRef("bar"), 4L);
    dmb.setTermFrequency(new BytesRef("baz"), 32L);
    final DocumentModel dm = dmb.getModel();
    out.writeObject(dm);
    out.close();
  }

  @SuppressWarnings("ImplicitNumericConversion")
  @Test
  public void testReadObject()
      throws Exception {
    final ByteArrayOutputStream bouts = new ByteArrayOutputStream();
    @SuppressWarnings({"resource", "TypeMayBeWeakened"})
    final ObjectOutputStream out = new ObjectOutputStream(bouts);

    // document id
    final int dom_id = 1;
    // number of unique terms
    final int dom_termCount = 3;
    // total term frequency
    final long dom_tf = 48L;

    final Builder dmb = new Builder(dom_id);
    dmb.setTermFrequency(new BytesRef("foo"), 12L);
    dmb.setTermFrequency(new BytesRef("bar"), 4L);
    dmb.setTermFrequency(new BytesRef("baz"), 32L);
    final DocumentModel ref = dmb.getModel();
    out.writeObject(ref);
    final byte[] bytes = bouts.toByteArray();
    out.close();

    final ByteArrayInputStream bins = new ByteArrayInputStream(bytes);
    @SuppressWarnings({"TypeMayBeWeakened", "resource"})
    final ObjectInputStream in = new ObjectInputStream(bins);
    final DocumentModel dom = (DocumentModel) in.readObject();
    in.close();

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