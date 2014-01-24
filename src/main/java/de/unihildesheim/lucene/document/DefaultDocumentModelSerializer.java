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
import java.util.Map;
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
    ObjectOutput objBytesStream; // converts object to bytes
    ByteArrayOutputStream objBytes; // collects object bytes
    byte[] objByteArr; // final object as byte array

    // convert object to byte array (termFreqMap)
    objBytes = new ByteArrayOutputStream();
    objBytesStream = new ObjectOutputStream(objBytes);
    objBytesStream.writeObject(value.getTermFreqMap());
    objByteArr = objBytes.toByteArray();
    out.writeInt(objByteArr.length); // save object size
    out.write(objByteArr); // save object
    objBytesStream.close();
    objBytes.close();

    // convert object to byte array (termDataList)
    objBytes = new ByteArrayOutputStream();
    objBytesStream = new ObjectOutputStream(objBytes);
    objBytesStream.writeObject(value.getTermDataList());
    objByteArr = objBytes.toByteArray();
    out.writeInt(objByteArr.length); // save object size
    out.write(objByteArr); // save object
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
    Map<String, Long> termFreqMap;
    List<TermData<String, Number>> termDataList;

    int objByteSize; // size of the objects byte array
    byte[] objByteArr; // objects bytes
    ByteArrayInputStream objBytes;
    ObjectInputStream objBytesStream;

    // convert byte array to object (termFreqMap)
    objByteSize = in.readInt();
    objByteArr = new byte[objByteSize];
    in.readFully(objByteArr);
    objBytes = new ByteArrayInputStream(objByteArr);
    objBytesStream = new ObjectInputStream(objBytes);
    try {
      termFreqMap = (Map<String, Long>) objBytesStream.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    objBytesStream.close();
    objBytes.close();

    // convert byte array to object (termDataList)
    objByteSize = in.readInt();
    objByteArr = new byte[objByteSize];
    in.readFully(objByteArr);
    objBytes = new ByteArrayInputStream(objByteArr);
    objBytesStream = new ObjectInputStream(objBytes);
    try {
      termDataList = (List<TermData<String, Number>>) objBytesStream.
              readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    objBytesStream.close();
    objBytes.close();

    return new DefaultDocumentModel(docId, termFreqMap, termDataList);
  }
}
