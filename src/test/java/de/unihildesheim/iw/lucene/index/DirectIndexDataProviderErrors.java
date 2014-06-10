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

package de.unihildesheim.iw.lucene.index;

import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.util.RandomValue;
import de.unihildesheim.iw.util.concurrent.processing.CollectionSource;
import de.unihildesheim.iw.util.concurrent.processing.Processing;
import de.unihildesheim.iw.util.concurrent.processing.ProcessingException;
import de.unihildesheim.iw.util.concurrent.processing.TargetFuncCall;
import org.junit.Test;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * @author Jens Bertram
 */
public class DirectIndexDataProviderErrors {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      DirectIndexDataProviderErrors.class);

  final TestIndexDataProvider referenceIndex = new TestIndexDataProvider();

  @Test
  public void idxDocTermsMap_IndexOutOfBounds()
      throws IOException, Buildable.BuildableException, ProcessingException {

    // generate some random field names
    final int numFields = RandomValue.getInteger(3, 10);
    final Collection<String> rFields = new HashSet(numFields);
    for (int i = 0; i < numFields; i++) {
      rFields.add(RandomValue.getString(2, 15));
    }
    final List<String> randFields = new ArrayList(rFields);

    // instance objects
    DirectIndexDataProvider instance;
    Persistence p;

    instance = new DirectIndexDataProvider.Builder()
        .temporary()
        .dataPath(this.referenceIndex.reference().getDataDir())
        .documentFields(this.referenceIndex.reference().getDocumentFields())
        .indexReader(TestIndexDataProvider.getIndexReader())
        .loadOrCreateCache("idxDocTermsMap_IndexOutOfBounds" +
            RandomValue.getString(16))
        .warmUp()
        .noReadOnly()
        .build();

    // create a new instance and delete the 'doc terms map' after warm-up
    p = instance.getPersistStatic();

    final ConcurrentNavigableMap<Fun.Tuple3<ByteArray, SerializableByte,
        Integer>, Integer> dtMap = DirectIndexDataProvider.CacheDbMakers
        .idxDocTermsMapMkr(p.getDb()).makeOrGet();
    final ConcurrentNavigableMap<String, SerializableByte> cfMap =
        DirectIndexDataProvider.CacheDbMakers.cachedFieldsMapMaker(
            p.getDb()).makeOrGet();

    for (final String field : randFields) {
      final Collection<SerializableByte> keys = new HashSet<>(cfMap.values());
      for (byte i = Byte.MIN_VALUE; (int) i < (int) Byte.MAX_VALUE; i++) {
        @SuppressWarnings("ObjectAllocationInLoop")
        final SerializableByte sByte = new SerializableByte(i);
        if (keys.add(sByte)) {
          LOG.debug("cfMap-put");
          cfMap.put(field, sByte);
          break;
        }
      }
    }

    final Collection<Integer> ids = new ArrayList<>((int) 1E4);
    for (int i = 0; (double) i < 1E4; i++) {
      ids.add(i);
    }

    new Processing().setSourceAndTarget(
        new TargetFuncCall(
            new CollectionSource(ids),
            new PutTarget(randFields, dtMap, cfMap))).process(ids.size());
  }

  private final class PutTarget
      extends TargetFuncCall.TargetFunc<Integer> {

    private final ConcurrentNavigableMap<Fun.Tuple3<ByteArray, SerializableByte,
        Integer>, Integer> dtMap;
    private final ConcurrentNavigableMap<String, SerializableByte> cfMap;
    private final List<String> randFields;

    PutTarget(final List<String> newRandFields, final ConcurrentNavigableMap<Fun
        .Tuple3<ByteArray, SerializableByte,
        Integer>, Integer> newDtMap, final ConcurrentNavigableMap<String,
        SerializableByte> newCfMap) {
      this.dtMap = newDtMap;
      this.cfMap = newCfMap;
      this.randFields = newRandFields;
    }

    @Override
    public void call(final Integer data)
        throws Exception {
      if (data != null) {
        LOG.debug("dtMap-put");
        dtMap.putIfAbsent(Fun.t3(
            new ByteArray(RandomValue.getString(3, 15).getBytes("UTF-8")),
            cfMap.get(randFields.get(RandomValue.getInteger(0,
                randFields.size() - 1))),
            RandomValue.getInteger()
        ), RandomValue.getInteger());
      }
    }
  }
}
