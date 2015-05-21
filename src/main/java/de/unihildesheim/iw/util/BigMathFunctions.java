/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache
 * .org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.unihildesheim.iw.util;

import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Several useful BigDecimal mathematical functions. Retrieved from
 * https://github.com/tareknaj/BigFunctions 05/2015.
 */
public final class BigMathFunctions {
  /**
   * Compute x^exponent to a given scale.  Uses the same algorithm as class
   * numbercruncher.mathutils.IntPower.
   *
   * @param x the value x
   * @param exponent the exponent value
   * @param scale the desired scale of the result
   * @return the result value
   */
  public static BigDecimal intPower(
      @NotNull BigDecimal x, long exponent, final int scale) {
    // If the exponent is negative, compute 1/(x^-exponent).
    if (exponent < 0L) {
      return BigDecimal.valueOf(1L)
          .divide(intPower(x, -exponent, scale), scale,
              BigDecimal.ROUND_HALF_EVEN);
    }

    BigDecimal power = BigDecimal.valueOf(1);

    // Loop to compute value^exponent.
    while (exponent > 0L) {
      // Is the rightmost bit a 1?
      if ((exponent & 1L) == 1L) {
        power = power.multiply(x)
            .setScale(scale, BigDecimal.ROUND_HALF_EVEN);
      }

      // Square x and shift exponent 1 bit to the right.
      x = x.multiply(x)
          .setScale(scale, BigDecimal.ROUND_HALF_EVEN);
      exponent >>= 1;

      Thread.yield();
    }

    return power;
  }

  /**
   * Compute the integral root of x to a given scale, x >= 0. Use Newton's
   * algorithm.
   *
   * @param x the value of x
   * @param index the integral root value
   * @param scale the desired scale of the result
   * @return the result value
   */
  public static BigDecimal intRoot(
      @NotNull BigDecimal x, final long index, final int scale) {
    // Check that x >= 0.
    if (x.signum() < 0) {
      throw new IllegalArgumentException("x < 0");
    }

    final int sp1 = scale + 1;
    final BigDecimal n = x;
    final BigDecimal i = BigDecimal.valueOf(index);
    final BigDecimal im1 = BigDecimal.valueOf(index - 1L);
    final BigDecimal tolerance = BigDecimal.valueOf(5L).movePointLeft(sp1);
    BigDecimal xPrev;

    // The initial approximation is x/index.
    x = x.divide(i, scale, BigDecimal.ROUND_HALF_EVEN);

    // Loop until the approximations converge
    // (two successive approximations are equal after rounding).
    do {
      // x^(index-1)
      final BigDecimal xToIm1 = intPower(x, index - 1, sp1);

      // x^index
      final BigDecimal xToI = x.multiply(xToIm1)
          .setScale(sp1, BigDecimal.ROUND_HALF_EVEN);

      // n + (index-1)*(x^index)
      final BigDecimal numerator = n.add(im1.multiply(xToI))
          .setScale(sp1, BigDecimal.ROUND_HALF_EVEN);

      // (index*(x^(index-1))
      final BigDecimal denominator = i.multiply(xToIm1)
          .setScale(sp1, BigDecimal.ROUND_HALF_EVEN);

      // x = (n + (index-1)*(x^index)) / (index*(x^(index-1)))
      xPrev = x;
      x = numerator
          .divide(denominator, sp1, BigDecimal.ROUND_DOWN);

      Thread.yield();
    } while (x.subtract(xPrev).abs().compareTo(tolerance) > 0);

    return x;
  }

  /**
   * Compute e^x to a given scale. Break x into its whole and fraction parts and
   * compute (e^(1 + fraction/whole))^whole using Taylor's formula.
   *
   * @param x the value of x
   * @param scale the desired scale of the result
   * @return the result value
   */
  public static BigDecimal exp(@NotNull BigDecimal x, int scale) {
    // e^0 = 1
    if (x.signum() == 0) {
      return BigDecimal.valueOf(1);
    }

    // If x is negative, return 1/(e^-x).
    else if (x.signum() == -1) {
      return BigDecimal.valueOf(1)
          .divide(exp(x.negate(), scale), scale,
              BigDecimal.ROUND_HALF_EVEN);
    }

    // Compute the whole part of x.
    BigDecimal xWhole = x.setScale(0, BigDecimal.ROUND_DOWN);

    // If there isn't a whole part, compute and return e^x.
    if (xWhole.signum() == 0) {
      return expTaylor(x, scale);
    }

    // Compute the fraction part of x.
    final BigDecimal xFraction = x.subtract(xWhole);

    // z = 1 + fraction/whole
    final BigDecimal z = BigDecimal.valueOf(1)
        .add(xFraction.divide(xWhole, scale, BigDecimal.ROUND_HALF_EVEN));

    // t = e^z
    final BigDecimal t = expTaylor(z, scale);

    final BigDecimal maxLong = BigDecimal.valueOf(Long.MAX_VALUE);
    BigDecimal result = BigDecimal.valueOf(1);

    // Compute and return t^whole using intPower().
    // If whole > Long.MAX_VALUE, then first compute products
    // of e^Long.MAX_VALUE.
    while (xWhole.compareTo(maxLong) >= 0) {
      result = result.multiply(
          intPower(t, Long.MAX_VALUE, scale))
          .setScale(scale, BigDecimal.ROUND_HALF_EVEN);
      xWhole = xWhole.subtract(maxLong);

      Thread.yield();
    }
    return result.multiply(intPower(t, xWhole.longValue(), scale))
        .setScale(scale, BigDecimal.ROUND_HALF_EVEN);
  }

  /**
   * Compute e^x to a given scale by the Taylor series.
   *
   * @param x the value of x
   * @param scale the desired scale of the result
   * @return the result value
   */
  private static BigDecimal expTaylor(@NotNull BigDecimal x, final int scale) {
    BigDecimal factorial = BigDecimal.valueOf(1);
    BigDecimal xPower = x;
    BigDecimal sumPrev;

    // 1 + x
    BigDecimal sum = x.add(BigDecimal.valueOf(1));

    // Loop until the sums converge
    // (two successive sums are equal after rounding).
    int i = 2;
    do {
      // x^i
      xPower = xPower.multiply(x)
          .setScale(scale, BigDecimal.ROUND_HALF_EVEN);

      // i!
      factorial = factorial.multiply(BigDecimal.valueOf(i));

      // x^i/i!
      final BigDecimal term = xPower
          .divide(factorial, scale,
              BigDecimal.ROUND_HALF_EVEN);

      // sum = sum + x^i/i!
      sumPrev = sum;
      sum = sum.add(term);

      ++i;
      Thread.yield();
    } while (sum.compareTo(sumPrev) != 0);

    return sum;
  }

  /**
   * Compute the natural logarithm of x to a given scale, x > 0.
   */
  public static BigDecimal ln(@NotNull BigDecimal x, final int scale) {
    // Check that x > 0.
    if (x.signum() <= 0) {
      throw new IllegalArgumentException("x <= 0");
    }

    // The number of digits to the left of the decimal point.
    final int magnitude = x.toString().length() - x.scale() - 1;

    if (magnitude < 3) {
      return lnNewton(x, scale);
    }

    // Compute magnitude*ln(x^(1/magnitude)).
    else {

      // x^(1/magnitude)
      final BigDecimal root = intRoot(x, (long) magnitude, scale);

      // ln(x^(1/magnitude))
      final BigDecimal lnRoot = lnNewton(root, scale);

      // magnitude*ln(x^(1/magnitude))
      return BigDecimal.valueOf((long) magnitude).multiply(lnRoot)
          .setScale(scale, BigDecimal.ROUND_HALF_EVEN);
    }
  }

  /**
   * Compute the natural logarithm of x to a given scale, x > 0. Use Newton's
   * algorithm.
   */
  private static BigDecimal lnNewton(@NotNull BigDecimal x, final int scale) {
    final int sp1 = scale + 1;
    final BigDecimal n = x;
    BigDecimal term;

    // Convergence tolerance = 5*(10^-(scale+1))
    final BigDecimal tolerance = BigDecimal.valueOf(5)
        .movePointLeft(sp1);

    // Loop until the approximations converge
    // (two successive approximations are within the tolerance).
    do {

      // e^x
      final BigDecimal eToX = exp(x, sp1);

      // (e^x - n)/e^x
      term = eToX.subtract(n)
          .divide(eToX, sp1, BigDecimal.ROUND_DOWN);

      // x - (e^x - n)/e^x
      x = x.subtract(term);

      Thread.yield();
    } while (term.compareTo(tolerance) > 0);

    return x.setScale(scale, BigDecimal.ROUND_HALF_EVEN);
  }

  /**
   * Compute the arctangent of x to a given scale, |x| < 1
   *
   * @param x the value of x
   * @param scale the desired scale of the result
   * @return the result value
   */
  public static BigDecimal arctan(@NotNull BigDecimal x, final int scale) {
    // Check that |x| < 1.
    if (x.abs().compareTo(BigDecimal.valueOf(1)) >= 0) {
      throw new IllegalArgumentException("|x| >= 1");
    }

    // If x is negative, return -arctan(-x).
    if (x.signum() == -1) {
      return arctan(x.negate(), scale).negate();
    } else {
      return arctanTaylor(x, scale);
    }
  }

  /**
   * Compute the arctangent of x to a given scale by the Taylor series, |x| < 1
   *
   * @param x the value of x
   * @param scale the desired scale of the result
   * @return the result value
   */
  private static BigDecimal arctanTaylor(
      @NotNull BigDecimal x, final int scale) {
    final int sp1 = scale + 1;
    int i = 3;
    boolean addFlag = false;

    BigDecimal power = x;
    BigDecimal sum = x;
    BigDecimal term;

    // Convergence tolerance = 5*(10^-(scale+1))
    final BigDecimal tolerance = BigDecimal.valueOf(5)
        .movePointLeft(sp1);

    // Loop until the approximations converge
    // (two successive approximations are within the tolerance).
    do {
      // x^i
      power = power.multiply(x).multiply(x)
          .setScale(sp1, BigDecimal.ROUND_HALF_EVEN);

      // (x^i)/i
      term = power.divide(BigDecimal.valueOf(i), sp1,
          BigDecimal.ROUND_HALF_EVEN);

      // sum = sum +- (x^i)/i
      sum = addFlag ? sum.add(term)
          : sum.subtract(term);

      i += 2;
      addFlag = !addFlag;

      Thread.yield();
    } while (term.compareTo(tolerance) > 0);

    return sum;
  }

  /**
   * Compute the square root of x to a given scale, x >= 0. Use Newton's
   * algorithm.
   *
   * @param x the value of x
   * @param scale the desired scale of the result
   * @return the result value
   */
  public static BigDecimal sqrt(@NotNull BigDecimal x, final int scale) {
    // Check that x >= 0.
    if (x.signum() < 0) {
      throw new IllegalArgumentException("x < 0");
    }

    // n = x*(10^(2*scale))
    final BigInteger n = x.movePointRight(scale << 1).toBigInteger();

    // The first approximation is the upper half of n.
    final int bits = (n.bitLength() + 1) >> 1;
    BigInteger ix = n.shiftRight(bits);
    BigInteger ixPrev;

    // Loop until the approximations converge
    // (two successive approximations are equal after rounding).
    do {
      ixPrev = ix;

      // x = (x + n/x)/2
      ix = ix.add(n.divide(ix)).shiftRight(1);

      Thread.yield();
    } while (ix.compareTo(ixPrev) != 0);

    return new BigDecimal(ix, scale);
  }
}