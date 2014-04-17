/*
 * Written by Doug Lea and Martin Buchholz with assistance from
 * members of JCP JSR-166 Expert Group and released to the public
 * domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package de.unihildesheim.util.concurrent;

/*
 * Source:
 * http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/jsr166e/extra/AtomicDouble.java?revision=1.13
 * (Modified to adapt to guava coding conventions and
 * to use AtomicLongFieldUpdater instead of sun.misc.Unsafe)
 */
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Double.longBitsToDouble;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A {@code double} value that may be updated atomically. See the
 * {@link java.util.concurrent.atomic} package specification for description
 * of the properties of atomic variables. An {@code
 * AtomicDouble} is used in applications such as atomic accumulation, and
 * cannot be used as a replacement for a {@link Double}. However, this class
 * does extend {@code Number} to allow uniform access by tools and utilities
 * that deal with numerically-based classes.
 *
 * <p>
 * This class compares primitive {@code double} values in methods such as
 * {@link #compareAndSet} by comparing their bitwise representation using
 * {@link Double#doubleToRawLongBits}, which differs from both the primitive
 * double {@code ==} operator and from {@link Double#equals}, as if
 * implemented by:
 * <pre> {@code
 * static boolean bitEquals(double x, double y) {
 *   long xBits = Double.doubleToRawLongBits(x);
 *   long yBits = Double.doubleToRawLongBits(y);
 *   return xBits == yBits;
 * }}</pre>
 *
 * <p>
 * It is possible to write a more scalable updater, at the cost of giving up
 * strict atomicity. See for example
 * <a
 * href="http://gee.cs.oswego.edu/dl/jsr166/dist/jsr166edocs/jsr166e/DoubleAdder.html">
 * DoubleAdder</a>
 * and
 * <a
 * href="http://gee.cs.oswego.edu/dl/jsr166/dist/jsr166edocs/jsr166e/DoubleMaxUpdater.html">
 * DoubleMaxUpdater</a>.
 *
 * @author Doug Lea Martin Buchholz
 */
public final class AtomicDouble extends Number {

  /**
   * Serialization id.
   */
  private static final long serialVersionUID = 1041898478390238059L;

  /**
   * Current value.
   */
  private transient volatile long value;

  /**
   * Updater to atomatically set a new value.
   */
  private static final AtomicLongFieldUpdater<AtomicDouble> UPDATER
          = AtomicLongFieldUpdater.newUpdater(AtomicDouble.class, "value");

  /**
   * Creates a new {@code AtomicDouble} with the given initial value.
   *
   * @param initialValue the initial value
   */
  public AtomicDouble(final double initialValue) {
    this.value = doubleToRawLongBits(initialValue);
  }

  /**
   * Creates a new {@code AtomicDouble} with initial value {@code 0.0}.
   */
  public AtomicDouble() {
    assert doubleToRawLongBits(0.0) == 0L;
  }

  /**
   * Gets the current value.
   *
   * @return the current value
   */
  public double get() {
    return longBitsToDouble(this.value);
  }

  /**
   * Sets to the given value.
   *
   * @param newValue the new value
   */
  public void set(final double newValue) {
    long next = doubleToRawLongBits(newValue);
    this.value = next;
  }

  /**
   * Eventually sets to the given value.
   *
   * @param newValue the new value
   */
  public void lazySet(final double newValue) {
    long next = doubleToRawLongBits(newValue);
    AtomicDouble.UPDATER.lazySet(this, next);
  }

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  public double getAndSet(final double newValue) {
    long next = doubleToRawLongBits(newValue);
    return longBitsToDouble(AtomicDouble.UPDATER.getAndSet(this, next));
  }

  /**
   * Atomically sets the value to the given updated value if the current value
   * is <a href="#bitEquals">bitwise equal</a>
   * to the expected value.
   *
   * @param expect the expected value
   * @param update the new value
   * @return {@code true} if successful. False return indicates that the
   * actual value was not bitwise equal to the expected value.
   */
  public boolean compareAndSet(final double expect,
          final double update) {
    return AtomicDouble.UPDATER.compareAndSet(this,
            doubleToRawLongBits(expect),
            doubleToRawLongBits(update));
  }

  /**
   * Atomically sets the value to the given updated value if the current value
   * is <a href="#bitEquals">bitwise equal</a>
   * to the expected value.
   *
   * <p>
   * May <a
   * href="http://download.oracle.com/javase/7/docs/api/java/util/concurrent/atomic/package-summary.html#Spurious">
   * fail spuriously</a>
   * and does not provide ordering guarantees, so is only rarely an
   * appropriate alternative to {@code compareAndSet}.
   *
   * @param expect the expected value
   * @param update the new value
   * @return {@code true} if successful
   */
  public boolean weakCompareAndSet(final double expect,
          final double update) {
    return AtomicDouble.UPDATER.weakCompareAndSet(this, doubleToRawLongBits(
            expect),
            doubleToRawLongBits(update));
  }

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the previous value
   */
  public double getAndAdd(final double delta) {
    while (true) {
      long current = this.value;
      double currentVal = longBitsToDouble(current);
      double nextVal = currentVal + delta;
      long next = doubleToRawLongBits(nextVal);
      if (AtomicDouble.UPDATER.compareAndSet(this, current, next)) {
        return currentVal;
      }
    }
  }

  /**
   * Atomically adds the given value to the current value.
   *
   * @param delta the value to add
   * @return the updated value
   */
  public double addAndGet(final double delta) {
    while (true) {
      long current = this.value;
      double currentVal = longBitsToDouble(current);
      double nextVal = currentVal + delta;
      long next = doubleToRawLongBits(nextVal);
      if (AtomicDouble.UPDATER.compareAndSet(this, current, next)) {
        return nextVal;
      }
    }
  }

  /**
   * Returns the String representation of the current value.
   *
   * @return the String representation of the current value
   */
  @Override
  public String toString() {
    return Double.toString(get());
  }

  /**
   * Returns the value of this {@code AtomicDouble} as an {@code int} after a
   * narrowing primitive conversion.
   *
   * @return Integer value
   */
  @Override
  public int intValue() {
    return (int) get();
  }

  /**
   * Returns the value of this {@code AtomicDouble} as a {@code long} after a
   * narrowing primitive conversion.
   *
   * @return Long value
   */
  @Override
  public long longValue() {
    return (long) get();
  }

  /**
   * Returns the value of this {@code AtomicDouble} as a {@code float} after a
   * narrowing primitive conversion.
   *
   * @return Float value
   */
  @Override
  public float floatValue() {
    return (float) get();
  }

  /**
   * Returns the value of this {@code AtomicDouble} as a {@code double}.
   *
   * @return Double value
   */
  @Override
  public double doubleValue() {
    return get();
  }

  /**
   * Saves the state to a stream (that is, serializes it).
   *
   * @param s Stream to write to
   * @serialData The current value is emitted (a {@code double}).
   * @throws IOException Thrown on low-level I/O errors
   */
  private void writeObject(final ObjectOutputStream s) throws IOException {
    s.defaultWriteObject();
    s.writeDouble(get());
  }

  /**
   * Reconstitutes the instance from a stream (that is, deserializes it).
   *
   * @param s Stream to read from
   * @throws IOException Thrown on low-level I/O errors
   * @throws ClassNotFoundException Thrown, if class could not be found
   */
  private void readObject(final ObjectInputStream s) throws IOException,
          ClassNotFoundException {
    s.defaultReadObject();
    set(s.readDouble());
  }
}
