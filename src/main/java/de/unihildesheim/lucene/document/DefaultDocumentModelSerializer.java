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
package de.unihildesheim.lucene.document;

import de.ruedigermoeller.serialization.FSTConfiguration;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import de.unihildesheim.io.FastByteArrayInputStream;
import de.unihildesheim.io.FastByteArrayOutputStream;
import de.unihildesheim.lucene.util.BytesWrap;
import de.unihildesheim.util.Tuple;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.mapdb.Serializer;

/**
 * Custom {@link Serializer} for {@link DefaultDocumentModel} objects.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class DefaultDocumentModelSerializer implements
        Serializer<DefaultDocumentModel>, Serializable {

  /**
   * Serialization class version id.
   */
  private static final long serialVersionUID = 0L;

  /**
   * Configuration for fast object serialization.
   */
  private static final FSTConfiguration FST_CONF = FSTConfiguration.
          createDefaultConfiguration();

  @Override
  public void serialize(final DataOutput out, final DefaultDocumentModel value)
          throws IOException {
    // pre-defined objects
    out.writeInt(value.getDocId());
    try (FastByteArrayOutputStream objBytes = new FastByteArrayOutputStream()) {
      final FSTObjectOutput objBytesStream = FST_CONF.getObjectOutput(objBytes);
      objBytesStream.writeObject(value.getTermData(), List.class);
      // final object as byte array
      final byte[] objByteArr = objBytes.getByteArray();
      // save object size
      out.writeInt(objByteArr.length);
      // save object
      out.write(objByteArr);
    }
  }

  @Override
  public DefaultDocumentModel deserialize(final DataInput in,
          final int available) throws IOException {
    if (available == 0) {
      return null;
    }

    // pre-defined objects
    final int docId = in.readInt();

    // custom objects
    List<Tuple.Tuple3<BytesWrap, String, Number>> termDataList;
    // size of the objects byte array
    final int objByteSize = in.readInt();
    // objects bytes
    final byte[] objByteArr = new byte[objByteSize];
    in.readFully(objByteArr);
    try (FastByteArrayInputStream objBytes
            = new FastByteArrayInputStream(objByteArr)) {
      FSTObjectInput objBytesStream = FST_CONF.getObjectInput(objBytes);
      try {
        termDataList
                = (List<Tuple.Tuple3<BytesWrap, String, Number>>) objBytesStream.
                readObject(List.class);
      } catch (Exception ex) {
        throw new IOException(ex);
      }
    }

    return new DefaultDocumentModel(docId, termDataList);
  }
}
