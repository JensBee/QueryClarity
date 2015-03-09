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

import de.unihildesheim.iw.lucene.document.DocumentModel;
import de.unihildesheim.iw.lucene.document.DocumentModel.SerializationBuilder;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * MapDB serializers, comparators, etc. for {@link DocumentModel} objects.
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class DocumentModelSerializer {

  /**
   * MapDB {@link Serializer} for {@link DocumentModel}s.
   */
  public static final org.mapdb.Serializer SERIALIZER = new Serializer();

  /**
   * Custom MapDB {@link org.mapdb.Serializer Serializer} for {@link
   * DocumentModel} objects. Does not handle n{@code null} values.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Serializer
      implements org.mapdb.Serializer<DocumentModel>, Serializable {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = 5840690360278423158L;

    @Override
    public void serialize(final DataOutput out, final DocumentModel dm)
        throws IOException {
      // write document-id
      DataOutput2.packInt(out, dm.id);
      // write number of unique terms
      final int termCount = dm.termCount();
      DataOutput2.packInt(out, termCount);
      // write frequencies
      org.mapdb.Serializer.LONG_ARRAY.serialize(out,
          dm.getFreqsForSerialization());

      // write terms
      final BytesRef spare = new BytesRef();
      final BytesRefHash terms = dm.getTermsForSerialization();
      for (int i = 0; i < termCount; i++) {
        terms.get(i, spare);
        DataOutput2.packInt(out, spare.length);
        out.write(spare.bytes, spare.offset, spare.length);
      }
    }

    @SuppressWarnings("ObjectAllocationInLoop")
    @Override
    public DocumentModel deserialize(final DataInput in, final int available)
        throws IOException {
      // read document-id
      final int docId = DataInput2.unpackInt(in);
      // read number of unique terms
      final int termCount = DataInput2.unpackInt(in);
      // read term-frequencies
      final long[] freqs = org.mapdb.Serializer.LONG_ARRAY
          .deserialize(in, available);
      final SerializationBuilder builder =
          new SerializationBuilder(docId, termCount, freqs);

      // read terms
      final BytesRef spare = new BytesRef();
      for (int i = termCount -1; i >= 0; i--) {
        spare.bytes = new byte[DataInput2.unpackInt(in)];
        spare.offset = 0;
        spare.length = spare.bytes.length;
        in.readFully(spare.bytes);

        builder.addTerm(spare);
      }

      return builder.getModel();
    }

    @Override
    public int fixedSize() {
      return -1;
    }
  }
}
