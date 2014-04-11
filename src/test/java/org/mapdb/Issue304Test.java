package org.mapdb;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class Issue304Test {

  @Test
  public void BTreeMap_replace() throws InterruptedException {

    final BTreeMap<Integer, Integer> m = DBMaker.newMemoryDirectDB().
            transactionDisable().make()
            .createTreeMap("test")
            .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_INT)
            .valueSerializer(Serializer.INTEGER)
            .makeOrGet();

    final int max = (int) 1e4;

    for (int i = 0; i < max; i += 2) {
      m.put(i, i);
    }

    ExecutorService s = Executors.newCachedThreadPool();

    for (int i = 0; i < 6; i++) {
      s.execute(new Runnable() {
        @Override
        public void run() {
          Random r = new Random();
          for (int i = 0; i < max * 100; i++) {
            Integer key = r.nextInt(max);
            Integer old = m.putIfAbsent(key, key);

            if (old != null) {
              do {
                old = m.get(key);
              } while (!m.replace(key, old, old + 10));
            }

          }

        }
      });
    }

    s.shutdown();

    s.awaitTermination(1, TimeUnit.HOURS);
  }

}
