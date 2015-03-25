/*
 * Copyright (C) 2014 Jens Bertram
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

package de.unihildesheim.iw.util.concurrent;

import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jens Bertram
 */
public final class AtomicBigDecimal
    extends Number {
  /**
   * Serialization id.
   */
  private static final long serialVersionUID = 4639655871375392142L;
  /**
   * Current value.
   */
  private final AtomicReference<BigDecimal> value;

  /**
   * Creates a new {@code AtomicBigDecimal} with initial value {@code 0.0}.
   */
  public AtomicBigDecimal() {
    this(BigDecimal.ZERO);
  }

  /**
   * Creates a new {@code AtomicDouble} with the given initial value.
   *
   * @param initialValue the initial value
   */
  public AtomicBigDecimal(final BigDecimal initialValue) {
    this.value = new AtomicReference<>(initialValue);
  }

  /**
   * Creates a new {@code AtomicDouble} with the given initial value.
   *
   * @param initialValue the initial value
   */
  public AtomicBigDecimal(final double initialValue) {
    this(new BigDecimal(initialValue));
  }

  /**
   * Sets to the given value.
   *
   * @param newValue the new value
   */
  public void set(final double newValue) {
    set(new BigDecimal(newValue));
  }

  /**
   * Sets to the given value.
   *
   * @param newValue the new value
   */
  public void set(final BigDecimal newValue) {
    this.value.set(newValue);
  }

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  public BigDecimal getAndSet(final BigDecimal newValue) {
    while (true) {
      final BigDecimal oldValue = get();

      if (compareAndSet(oldValue, newValue)) {
        return oldValue;
      }
    }
  }

  /**
   * Gets the current value.
   *
   * @return the current value
   */
  public BigDecimal get() {
    return this.value.get();
  }

  /**
   * Atomically sets the value to the given updated value if the current value
   * is equal to the expected value.
   *
   * @param expect the expected value
   * @param update the new value
   * @return {@code true} if successful. False return indicates that the actual
   * value was not equal to the expected value.
   */
  @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
  public boolean compareAndSet(final BigDecimal expect,
      final BigDecimal update) {
    while (true) {
      final BigDecimal origVal = get();

      if (origVal.compareTo(expect) == 0) {
        if (this.value.compareAndSet(origVal, update)) {
          return true;
        }
      } else {
        return false;
      }
    }
  }

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the previous value
   */
  public BigDecimal getAndAdd(final BigDecimal delta) {
    return getAndAdd(delta, null);
  }

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @param mc Math context to use
   * @return the previous value
   */
  public BigDecimal getAndAdd(
      final BigDecimal delta, @Nullable final MathContext mc) {
    while (true) {
      final BigDecimal origVal = get();
      final BigDecimal newVal;
      newVal = mc == null ? origVal.add(delta) : origVal.add(delta, mc);
      if (compareAndSet(origVal, newVal)) {
        return origVal;
      }
    }
  }

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the updated value
   */
  public BigDecimal addAndGet(final BigDecimal delta) {
    return addAndGet(delta, null);
  }

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @param mc Math context to use
   * @return the updated value
   */
  public BigDecimal addAndGet(
      final BigDecimal delta, @Nullable final MathContext mc) {
    while (true) {
      final BigDecimal origVal = get();
      final BigDecimal newVal;
      newVal = mc == null ? origVal.add(delta) : origVal.add(delta, mc);
      if (compareAndSet(origVal, newVal)) {
        return newVal;
      }
    }
  }

  /**
   * Returns the value of this {@code AtomicBigDecimal} as an {@code int} after
   * a narrowing primitive conversion.
   *
   * @return Integer value
   */
  @Override
  public int intValue() {
    return get().intValue();
  }

  /**
   * Returns the value of this {@code AtomicBigDecimal} as an {@code int} after
   * a narrowing primitive conversion.
   *
   * @return Integer value
   */
  @Override
  public long longValue() {
    return get().longValue();
  }

  /**
   * Returns the value of this {@code AtomicBigDecimal} as an {@code int} after
   * a narrowing primitive conversion.
   *
   * @return Integer value
   */
  @Override
  public float floatValue() {
    return get().floatValue();
  }

  /**
   * Returns the value of this {@code AtomicBigDecimal} as an {@code int} after
   * a narrowing primitive conversion.
   *
   * @return Integer value
   */
  @Override
  public double doubleValue() {
    return get().doubleValue();
  }

  /**
   * Returns the String representation of the current value.
   *
   * @return the String representation of the current value
   */
  @Override
  public String toString() {
    return get().toString();
  }
}
