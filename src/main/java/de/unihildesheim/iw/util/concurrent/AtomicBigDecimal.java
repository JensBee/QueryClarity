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

package de.unihildesheim.iw.util.concurrent;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@code BigDecimal} value that may be updated atomically. See the {@link
 * java.util.concurrent.atomic} package specification for description of the
 * properties of atomic variables.
 *
 * @author Jens Bertram
 */
public class AtomicBigDecimal
    extends Number {
  private final AtomicReference<BigDecimal> valueHolder = new
      AtomicReference<>();

  /**
   * Creates a new AtomicBigDecimal with the given initial value.
   *
   * @param initialValue the initial value
   */
  public AtomicBigDecimal(BigDecimal initialValue) {
    valueHolder.set(initialValue);
  }

  /**
   * Creates a new AtomicBigDecimal with initial value {@code 0}.
   */
  public AtomicBigDecimal() {
    valueHolder.set(BigDecimal.ZERO);
  }

  /**
   * Gets the current value.
   *
   * @return the current value
   */
  public final BigDecimal get() {
    return valueHolder.get();
  }

  /**
   * Sets to the given value.
   *
   * @param newValue the new value
   */
  public final void set(final BigDecimal newValue) {
    valueHolder.set(newValue);
  }

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  public BigDecimal getAndSet(final BigDecimal newValue) {
    for (; ; ) {
      final BigDecimal current = valueHolder.get();
      if (valueHolder.compareAndSet(current, newValue)) {
        return current;
      }
    }
  }

  /**
   * Atomically increments by one the current value.
   *
   * @return the previous value
   */
  public BigDecimal getAndIncrement() {
    for (; ; ) {
      final BigDecimal current = valueHolder.get();
      final BigDecimal next = current.add(BigDecimal.ONE);
      if (valueHolder.compareAndSet(current, next)) {
        return current;
      }
    }
  }

  /**
   * Atomically decrements by one the current value.
   *
   * @return the previous value
   */
  public BigDecimal getAndDecrement() {
    for (; ; ) {
      final BigDecimal current = valueHolder.get();
      final BigDecimal next = current.subtract(BigDecimal.ONE);
      if (valueHolder.compareAndSet(current, next)) {
        return current;
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
    for (; ; ) {
      final BigDecimal current = valueHolder.get();
      final BigDecimal next = current.add(delta);
      if (valueHolder.compareAndSet(current, next)) {
        return current;
      }
    }
  }

  /**
   * Atomically increments by one the current value.
   *
   * @return the updated value
   */
  public BigDecimal incrementAndGet() {
    for (; ; ) {
      final BigDecimal current = valueHolder.get();
      final BigDecimal next = current.add(BigDecimal.ONE);
      if (valueHolder.compareAndSet(current, next)) {
        return next;
      }
    }
  }

  /**
   * Atomically decrements by one the current value.
   *
   * @return the updated value
   */
  public BigDecimal decrementAndGet() {
    for (; ; ) {
      final BigDecimal current = valueHolder.get();
      final BigDecimal next = current.subtract(BigDecimal.ONE);
      if (valueHolder.compareAndSet(current, next)) {
        return next;
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
    for (; ; ) {
      final BigDecimal current = valueHolder.get();
      final BigDecimal next = current.add(delta);
      if (valueHolder.compareAndSet(current, next)) {
        return next;
      }
    }
  }

  @Override
  public String toString() {
    final BigDecimal current = valueHolder.get();
    return current.toString();
  }

  @Override
  public int intValue() {
    final BigDecimal current = valueHolder.get();
    return current.intValue();
  }

  @Override
  public long longValue() {
    final BigDecimal current = valueHolder.get();
    return current.longValue();
  }

  @Override
  public float floatValue() {
    final BigDecimal current = valueHolder.get();
    return current.floatValue();
  }

  @Override
  public double doubleValue() {
    final BigDecimal current = valueHolder.get();
    return current.doubleValue();
  }
}
