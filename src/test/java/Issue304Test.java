
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

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

/**
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public class Issue304Test {
@Test public void BTreeMap_replace() throws InterruptedException {

        final BTreeMap<Integer,Integer> m = DBMaker.newMemoryDirectDB().transactionDisable().make()
                .createTreeMap("test")
                .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_INT)
                .valueSerializer(Serializer.INTEGER)
                .makeOrGet();


        final int max = (int) 1e4;

        for(int i=0;i<max;i+=2){
            m.put(i,i);
        }


        ExecutorService s = Executors.newCachedThreadPool();

        for(int i=0;i<6;i++){
            s.execute(new Runnable() {
                @Override
                public void run() {
                    Random r = new Random();
                    for(int i=0;i<max*100;i++){
                        Integer key = r.nextInt(max);
                        Integer old = m.putIfAbsent(key,key);

                        if(old!=null)
                            do {
                                old = m.get(key);
                            }while(!m.replace(key,old,old+10));

                    }

                }
            });
        }

        s.shutdown();

        s.awaitTermination(1, TimeUnit.HOURS);
    }

}
