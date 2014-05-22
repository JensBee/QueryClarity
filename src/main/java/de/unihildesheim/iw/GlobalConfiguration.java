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

package de.unihildesheim.iw;

import de.unihildesheim.iw.util.ConfigurationFile;

import java.io.IOException;

/**
 * Global file based configuration.
 *
 * @author Jens Bertram
 */
public class GlobalConfiguration
    extends ConfigurationFile {

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
      INSTANCE = new GlobalConfiguration(FILE_NAME);
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Private constructor. Statically called.
   *
   * @param newFileName File name of the configuration
   * @throws IOException Thrown on low-level I/O errors
   */
  private GlobalConfiguration(final String newFileName)
      throws IOException {
    super(newFileName);
    // activate storing to disk upon exit
    saveOnExit();
  }

  public static final String mkPrefix(final String identifier) {
    return identifier + "_";
  }

  /**
   * Get the singleton instance.
   *
   * @return Instance
   */
  public static final GlobalConfiguration conf() {
    return INSTANCE;
  }
}
