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

import de.unihildesheim.util.Tuple;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import org.apache.lucene.util.BytesRef;
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

  @Override
  public void serialize(final DataOutput out, final DefaultDocumentModel value)
          throws IOException {
    // pre-defined objects
    out.writeInt(value.getDocId());

    // custom objects
    // collect object bytes
    final ByteArrayOutputStream objBytes = new ByteArrayOutputStream();
    // convert object to bytes
    final ObjectOutput objBytesStream = new ObjectOutputStream(objBytes);
    objBytesStream.writeObject(value.getTermData());
    // final object as byte array
    final byte[] objByteArr = objBytes.toByteArray();
    // save object size
    out.writeInt(objByteArr.length);
    // save object
    out.write(objByteArr);
    objBytesStream.close();
    objBytes.close();
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
    List<Tuple.Tuple3<byte[], String, Number>> termDataList;
    // size of the objects byte array
    final int objByteSize = in.readInt();
    // objects bytes
    final byte[] objByteArr = new byte[objByteSize];
    in.readFully(objByteArr);
    final ByteArrayInputStream objBytes = new ByteArrayInputStream(objByteArr);
    ObjectInputStream objBytesStream = new ObjectInputStream(objBytes);
    try {
      termDataList
              = (List<Tuple.Tuple3<byte[], String, Number>>) objBytesStream.
              readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    objBytesStream.close();
    objBytes.close();

    return new DefaultDocumentModel(docId, termDataList);
  }
}
