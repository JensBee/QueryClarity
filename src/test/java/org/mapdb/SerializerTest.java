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

package org.mapdb;

import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.lucene.index.DirectIndexDataProvider;
import de.unihildesheim.iw.util.RandomValue;
import org.junit.Test;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Jens Bertram
 */
public final class SerializerTest {
  @Test
  @SuppressWarnings("ObjectAllocationInLoop")
  public void test()
      throws InterruptedException {
    final DB db = DBMaker.newTempFileDB()
        .transactionDisable()
        .mmapFileEnableIfSupported()
        .compressionEnable()
        .strictDBGet()
        .asyncWriteEnable()
        .checksumEnable()
        .asyncWriteFlushDelay(100)
        .closeOnJvmShutdown()
        .make();
    final ConcurrentNavigableMap<Fun.Tuple3<
        ByteArray, SerializableByte, Integer>,
        Integer> map = DirectIndexDataProvider.CacheDbMakers
        .idxDocTermsMapMkr(db).makeOrGet();

    final ExecutorService s = Executors.newCachedThreadPool();

    for (int i = 0; i < 6; i++) {
      s.execute(new Runnable() {
        @Override
        public void run() {
          System.out.println("new Thread");

          for (int i = 0; (double) i < 1E6; i++) {
            final ByteArray ba = new ByteArray(RandomValue
                .getString(1, 15).getBytes());
            final SerializableByte aByte = new SerializableByte
                (RandomValue.getByte());
            final Integer aInt = RandomValue.getInteger();
            Integer val = map.putIfAbsent(Fun.t3(ba, aByte, aInt),
                RandomValue.getInteger());
            if (val != null) {
              while (!map.replace(Fun.t3(ba, aByte, aInt), val,
                  RandomValue.getInteger())) {
                val = map.get(Fun.t3(ba, aByte, aInt));
              }
            }
          }
        }
      });
    }

    s.shutdown();

    s.awaitTermination(1L, TimeUnit.HOURS);
  }
}
