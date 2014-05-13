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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
  public void testSetSource()
      throws Exception {
    final Collection<String> coll = new ArrayList<>(1);
    coll.add(RandomValue.getString(1, 10));
    final Source<String> newSource = new CollectionSource<>(coll);
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
  public void testSetSourceAndTarget()
      throws Exception {
    final Collection<String> coll = new ArrayList<>(1);
    coll.add(RandomValue.getString(1, 10));
    final Source<String> newSource = new CollectionSource<>(coll);
    final Processing instance = new Processing();
    final TargetFuncCall target = new TargetFuncCall<>(newSource,
        new TestTargets.FuncCall<String>(new AtomicLong(0)));

    instance.setSourceAndTarget(target);

    instance.process();

    exception.expect(IllegalArgumentException.class);
    instance.setSourceAndTarget(null);
  }

  /**
   * Test of setTarget method, of class Processing.
   */
  @Test
  public void testSetTarget()
      throws Exception {
    final Collection<String> coll = new ArrayList<>(1);
    coll.add(RandomValue.getString(1, 10));
    final Source<String> newSource = new CollectionSource<>(coll);

    final Processing instance = new Processing();
    final TargetFuncCall target = new TargetFuncCall<>(newSource,
        new TestTargets.FuncCall<String>(new AtomicLong(0)));

    instance.setTarget(target);

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
  public void testShutDown()
      throws Exception {
    Collection<String> coll = new ArrayList<>(1);
    coll.add(RandomValue.getString(1, 10));
    Source<String> newSource = new CollectionSource<>(coll);

    final AtomicLong counter = new AtomicLong(0);
    final Processing instance = new Processing();
    TargetFuncCall target = new TargetFuncCall<>(newSource,
        new TestTargets.FuncCall<String>(counter));

    instance.setSourceAndTarget(target);
    instance.process(coll.size());
    Processing.shutDown();
    instance.process();

    final int collSize = RandomValue.getInteger(100, 10000);
    coll = new ArrayList<>(collSize);
    for (int i = 0; i < collSize; i++) {
      coll.add(RandomValue.getString(1, 10));
    }
    newSource = new CollectionSource<>(coll);
    target = new TargetFuncCall<>(newSource,
        new TestTargets.FuncCall<String>(counter));
    Processing.shutDown();
    instance.setSourceAndTarget(target);
    instance.process();
  }

  /**
   * Test of debugTestSource method, of class Processing.
   */
  @Test
  public void testDebugTestSource()
      throws Exception {
    final int collSize = RandomValue.getInteger(100, 10000);
    final Collection<String> coll = new ArrayList<>(collSize);
    for (int i = 0; i < collSize; i++) {
      coll.add(RandomValue.getString(1, 10));
    }
    final Source<String> newSource = new CollectionSource<>(coll);

    final AtomicLong counter = new AtomicLong(0);
    new Processing(
        new TargetFuncCall<>(
            newSource,
            new TestTargets.FuncCall<String>(counter)
        )
    ).process(coll.size());

    assertEquals("Not all items provided by source.", collSize,
        counter.intValue());
  }

  /**
   * Test of process method, of class Processing.
   */
  @Test
  public void testProcess_0args()
      throws Exception {
    final Collection<String> coll;

    final int collSize = RandomValue.getInteger(100, 10000);
    coll = new ArrayList<>(collSize);
    for (int i = 0; i < collSize; i++) {
      coll.add(RandomValue.getString(1, 10));
    }

    final Source<String> newSource = new CollectionSource<>(coll);

    final Processing instance = new Processing();
    final AtomicLong counter = new AtomicLong(0);
    final TargetFuncCall target = new TargetFuncCall<>(newSource,
        new TestTargets.FuncCall<String>(counter));

    instance.setSourceAndTarget(target).process();

    assertEquals("Number of processed items differs.", collSize,
        counter.intValue());
  }

  /**
   * Test of process method, of class Processing.
   */
  @Test
  public void testProcess_int()
      throws Exception {
    final Collection<String> coll;
    final int collSize = RandomValue.getInteger(100, 10000);
    coll = new ArrayList<>(collSize);
    for (int i = 0; i < collSize; i++) {
      coll.add(RandomValue.getString(1, 10));
    }

    final Source<String> newSource = new CollectionSource<>(coll);
    final Processing instance = new Processing();
    final AtomicLong counter = new AtomicLong(0);
    TargetFuncCall target = new TargetFuncCall<>(newSource,
        new TestTargets.FuncCall<String>(counter));

    instance.setSourceAndTarget(target).process(
        RandomValue.getInteger(1, Runtime.getRuntime().
            availableProcessors())
    );
    assertEquals("Number of processed items differs.", collSize,
        counter.intValue());
  }

  @Test
  public void testExceptionThrowing()
      throws Exception {
    final List<String> coll;
    final int collSize = 10000;
    coll = new ArrayList<>(collSize);
    for (int i = 0; i < collSize; i++) {
      coll.add(RandomValue.getString(1, 10));
    }

    try {
      new Processing().setSourceAndTarget(
          new TargetFuncCall<>(
              new CollectionSource<>(coll),
              new ExceptionThrowingTarget(coll.get(RandomValue.getInteger(0,
                  collSize - 1)))
          )
      ).process(coll.size());
      fail("Expected an Exception to be thrown");
    } catch (ProcessingException.TargetFailedException e) {
      // pass
    }
  }

  @Test
  public void testAssertThrowing()
      throws Exception {
    final List<String> coll;
    final int collSize = 10000;
    coll = new ArrayList<>(collSize);
    for (int i = 0; i < collSize; i++) {
      coll.add(RandomValue.getString(1, 10));
    }

    try {
      new Processing().setSourceAndTarget(
          new TargetFuncCall<>(
              new CollectionSource<>(coll),
              new AssertThrowingTarget(coll.get(RandomValue.getInteger(0,
                  collSize - 1)))
          )
      ).process(coll.size());
      fail("Expected an Exception to be thrown");
    } catch (ProcessingException.TargetFailedException e) {
      // pass
    }
  }

  /**
   * Simple {@link TargetFuncCall.TargetFunc} throwing an {@link Exception} if a
   * defined string is matched.
   */
  private final static class ExceptionThrowingTarget
      extends TargetFuncCall.TargetFunc<String> {

    private final String throwAt;

    ExceptionThrowingTarget(final String throwAtStr) {
      this.throwAt = throwAtStr;
    }

    @Override
    public void call(final String data) {
      if (this.throwAt.equals(data)) {
        throw new IllegalStateException("Fake exception!");
      }
    }
  }

  /**
   * Simple {@link TargetFuncCall.TargetFunc} throwing an {@link AssertionError}
   * if a defined string is matched.
   */
  private final static class AssertThrowingTarget
      extends TargetFuncCall.TargetFunc<String> {

    private final String throwAt;

    AssertThrowingTarget(final String throwAtStr) {
      this.throwAt = throwAtStr;
    }

    @Override
    public void call(final String data) {
      if (this.throwAt.equals(data)) {
        assert !this.throwAt.equals(data) : "Fake assertion error!";
      }
    }
  }

}
