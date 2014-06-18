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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test for {@link Processing}.
 *
 * @author Jens Bertram
 */
public final class ProcessingTest
    extends TestCase {

  /**
   * Test of setSource method, of class Processing.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetSource()
      throws Exception {
    final Collection<String> coll =
        Collections.singletonList(RandomValue.getString(1, 10));
    final Source<String> newSource = new CollectionSource<>(coll);
    final Processing instance = new Processing();
    instance.setSource(newSource);

    // should throw - no target set
    try {
      instance.process();
      Assert.fail("Expected to catch an exception.");
    } catch (final IllegalStateException e) {
      // pass
    }

    // should throw - source is null
    try {
      instance.setSource(null);
      Assert.fail("Expected to catch an exception.");
    } catch (final NullPointerException e) {
      // pass
    }
  }

  /**
   * Test of setSourceAndTarget method, of class Processing.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetSourceAndTarget()
      throws Exception {
    final Collection<String> coll =
        Collections.singletonList(RandomValue.getString(1, 10));
    final Source<String> newSource = new CollectionSource<>(coll);
    final Processing instance = new Processing();
    final TargetFuncCall target = new TargetFuncCall<>(newSource,
        new TestTargets.FuncCall<String>(new AtomicLong(0L)));

    instance.setSourceAndTarget(target);

    instance.process();

    try {
      instance.setSourceAndTarget(null);
      Assert.fail("Expected to catch an exception.");
    } catch (final NullPointerException e) {
      // pass
    }
  }

  /**
   * Test of setTarget method, of class Processing.
   *
   * @throws Exception Any exception thrown indicates an error
   */
  @Test
  public void testSetTarget()
      throws Exception {
    final Collection<String> coll =
        Collections.singletonList(RandomValue.getString(1, 10));
    final Source<String> newSource = new CollectionSource<>(coll);

    final Processing instance = new Processing();
    final TargetFuncCall target = new TargetFuncCall<>(newSource,
        new TestTargets.FuncCall<String>(new AtomicLong(0L)));

    instance.setTarget(target);

    // should throw - no source set
    try {
      instance.process();
      Assert.fail("Expected to catch an exception.");
    } catch (final IllegalStateException e) {
      // pass
    }

    try {
      instance.setTarget(null);
      Assert.fail("Expected to catch an exception.");
    } catch (final NullPointerException e) {
      // pass
    }
  }

  /**
   * Test of debugTestSource method, of class Processing.
   *
   * @throws Exception Any exception thrown indicates an error
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

    final AtomicLong counter = new AtomicLong(0L);
    new Processing(
        new TargetFuncCall<>(
            newSource,
            new TestTargets.FuncCall<String>(counter)
        )
    ).process(coll.size());

    Assert.assertEquals("Not all items provided by source.", (long) collSize,
        counter.longValue());
  }

  /**
   * Test of process method, of class Processing.
   *
   * @throws Exception Any exception thrown indicates an error
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
    final AtomicLong counter = new AtomicLong(0L);
    final TargetFuncCall target = new TargetFuncCall<>(newSource,
        new TestTargets.FuncCall<String>(counter));

    instance.setSourceAndTarget(target).process();

    Assert.assertEquals("Number of processed items differs.", (long) collSize,
        counter.longValue());
  }

  /**
   * Test of process method, of class Processing.
   *
   * @throws Exception Any exception thrown indicates an error
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
    final AtomicLong counter = new AtomicLong(0L);
    final TargetFuncCall target = new TargetFuncCall<>(newSource,
        new TestTargets.FuncCall<String>(counter));

    instance.setSourceAndTarget(target).process(
        RandomValue.getInteger(1, Runtime.getRuntime().
            availableProcessors())
    );
    Assert.assertEquals("Number of processed items differs.", (long) collSize,
        counter.longValue());
  }

  /**
   * @throws Exception Any exception thrown indicates an error
   */
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
      Assert.fail("Expected an Exception to be thrown");
    } catch (final TargetException.TargetFailedException e) {
      // pass
    }
  }

  /**
   * @throws Exception Any exception thrown indicates an error
   */
  @SuppressWarnings("UnusedAssignment")
  @Test
  public void testAssertThrowing()
      throws Exception {
    boolean assertOn = false;
    // *assigns* true if assertions are on.
    //noinspection AssertWithSideEffects,ConstantConditions
    assert assertOn = true;

    // only run this test, if assertions are enabled
    if (assertOn) {
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
        Assert.fail("Expected an Exception to be thrown");
      } catch (final TargetException.TargetFailedException e) {
        // pass
      }
    }
  }

  /**
   * Simple {@link TargetFuncCall.TargetFunc} throwing an {@link Exception} if a
   * defined string is matched.
   */
  private static final class ExceptionThrowingTarget
      extends TargetFuncCall.TargetFunc<String> {

    /**
     * Value to throw an exception at.
     */
    private final String throwAt;

    /**
     * New instance with exception throwing String set.
     *
     * @param throwAtStr String to throw an exception at, if matched
     */
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
  private static final class AssertThrowingTarget
      extends TargetFuncCall.TargetFunc<String> {

    /**
     * Value to throw an assertion error at.
     */
    private final String throwAt;

    /**
     * New instance with assertion error throwing String set.
     *
     * @param throwAtStr String to throw an assertion error at, if matched
     */
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
