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

package de.unihildesheim.iw.util;

import java.io.IOException;
import java.math.MathContext;

/**
 * Global file based configuration.
 *
 * @author Jens Bertram
 */
public final class GlobalConfiguration
    extends ConfigurationFile {

  /**
   * Default math context to use for high precision calculations.
   */
  public static final String DEFAULT_MATH_CONTEXT =
      MathContext.DECIMAL32.toString();
  /**
   * File name of the global configuration.
   */
  private static final String FILE_NAME = "configuration.properties";
  /**
   * Singleton instance reference.
   */
  private static final GlobalConfiguration INSTANCE;

  static {
    try {
      INSTANCE = new GlobalConfiguration();
    } catch (final IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Private constructor. Statically called.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  private GlobalConfiguration()
      throws IOException {
    super(FILE_NAME);
    setDefaults();
    // activate storing to disk upon exit
    saveOnExit();
  }

  /**
   * Set default configuration values.
   */
  private void setDefaults() {
    this.getAndAddString(DefaultKeys.MATH_CONTEXT.toString(),
        DEFAULT_MATH_CONTEXT);
    this.getAndAddBoolean(DefaultKeys.MATH_LOW_PRECISION.toString(), false);
  }

  /**
   * Get the singleton instance.
   *
   * @return Instance
   */
  public static GlobalConfiguration conf() {
    return INSTANCE;
  }

  /**
   * Default settings keys. Used by {@link #setDefaults()}
   */
  @SuppressWarnings("PublicInnerClass")
  public enum DefaultKeys {
    /**
     * Default math context for high-precision calculations.
     */
    MATH_CONTEXT,
    /**
     * Boolean flag. If true, low precision math is used.
     */
    MATH_LOW_PRECISION,
    /**
     * Maximum number of parallel threads to use.
     */
    MAX_THREADS
  }
}
