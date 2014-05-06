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
package de.unihildesheim.iw.util.concurrent.processing;

import de.unihildesheim.iw.TestCase;
import de.unihildesheim.iw.util.RandomValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link Processing}.
 *
 * @author Jens Bertram
 */
public class ProcessingTest
    extends TestCase {

  /**
   * General rule to catch expected exceptions.
   */
  @Rule
  @java.lang.SuppressWarnings("PublicField")
  public ExpectedException exception = ExpectedException.none();

  /**
   * Test of setSource method, of class Processing.
   */
  @Test
  public void testSetSource() {
    final Collection<Object> coll = new ArrayList<>(1);
    final Source newSource = new CollectionSource<>(coll);
    final Processing instance = new Processing();
    instance.setSource(newSource);

    exception.expect(IllegalStateException.class);
    // should throw - no target set
    instance.process();
    exception.expect(IllegalArgumentException.class);
    // should throw - source is null
    instance.setSource(null);
  }

  /**
   * Test of setSourceAndTarget method, of class Processing.
   */
  @Test
  public void testSetSourceAndTarget() {
    final Collection<Object> coll = new ArrayList<>(1);
    final Source newSource = new CollectionSource<>(coll);
    @SuppressWarnings("unchecked")
    final Target newTarget = new Target.TargetTest<>(newSource);
    final Processing instance = new Processing();
    instance.setSourceAndTarget(newTarget);

    instance.process();

    exception.expect(IllegalArgumentException.class);
    instance.setSourceAndTarget(null);
  }

  /**
   * Test of setTarget method, of class Processing.
   */
  @Test
  public void testSetTarget() {
    final Collection<String> coll = new ArrayList<>(1);
    final Source newSource = new CollectionSource<>(coll);
    @SuppressWarnings("unchecked")
    final Target newTarget = new Target.TargetTest<>(newSource);
    final Processing instance = new Processing();

    instance.setTarget(newTarget);

    exception.expect(IllegalStateException.class);
    // should throw - no source set
    instance.process();

    exception.expect(IllegalArgumentException.class);
    instance.setTarget(null);
  }

  /**
   * Test of shutDown method, of class Processing.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testShutDown() {
    Collection<String> coll = new ArrayList<>(1);
    Source newSource = new CollectionSource<>(coll);
    Target newTarget = new Target.TargetTest<>(newSource);
    final Processing instance = new Processing();
    instance.setSourceAndTarget(newTarget);

    instance.process();
    Processing.shutDown();
    instance.process();

    int collSize = RandomValue.getInteger(100, 10000);
    coll = new ArrayList<>(collSize);
    for (int i = 0; i < collSize; i++) {
      coll.add(RandomValue.getString(1, 10));
    }
    newSource = new CollectionSource<>(coll);
    newTarget = new Target.TargetTest<>(newSource);
    Processing.shutDown();
    instance.setSourceAndTarget(newTarget);
    instance.process();
  }

  /**
   * Test of debugTestSource method, of class Processing.
   */
  @Test
  public void testDebugTestSource() {
    int collSize = RandomValue.getInteger(100, 10000);
    Collection<String> coll = new ArrayList<>(collSize);
    for (int i = 0; i < collSize; i++) {
      coll.add(RandomValue.getString(1, 10));
    }
    final Source newSource = new CollectionSource<>(coll);
    @SuppressWarnings("unchecked")
    final Target newTarget = new Target.TargetTest<>(newSource);
    final Processing instance = new Processing();
    instance.setSourceAndTarget(newTarget);

    long amount = instance.debugTestSource();
    assertEquals(collSize, amount);
  }

  /**
   * Test of process method, of class Processing.
   */
  @Test
  public void testProcess_0args() {
    Collection<String> coll = new ArrayList<>(1);
    Source newSource = new CollectionSource<>(coll);
    @SuppressWarnings("unchecked")
    Target newTarget = new Target.TargetTest<>(newSource);
    final Processing instance = new Processing();
    instance.setSourceAndTarget(newTarget);

    final int collSize = RandomValue.getInteger(100, 10000);
    coll = new ArrayList<>(collSize);
    for (int i = 0; i < collSize; i++) {
      coll.add(RandomValue.getString(1, 10));
    }
    instance.process();
  }

  /**
   * Test of process method, of class Processing.
   */
  @Test
  public void testProcess_int() {
    Collection<String> coll = new ArrayList<>(1);
    Source newSource = new CollectionSource<>(coll);
    @SuppressWarnings("unchecked")
    Target newTarget = new Target.TargetTest<>(newSource);
    final Processing instance = new Processing();
    instance.setSourceAndTarget(newTarget);

    final int collSize = RandomValue.getInteger(100, 10000);
    coll = new ArrayList<>(collSize);
    for (int i = 0; i < collSize; i++) {
      coll.add(RandomValue.getString(1, 10));
    }
    instance.process(RandomValue.getInteger(1, Runtime.getRuntime().
        availableProcessors()));
  }

}
