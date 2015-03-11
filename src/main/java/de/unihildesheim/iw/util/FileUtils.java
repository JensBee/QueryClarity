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

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;

/**
 * @author Jens Bertram
 */
public final class FileUtils {

  /**
   * Private empty constructor for utility class.
   */
  private FileUtils() {
  }

  /**
   * Get the canonical path of the given File with a trailing separator char
   * added.
   *
   * @param file File whose path to get
   * @return Path as string
   * @throws IOException Thrown on low-level I/O errors
   */
  public static String getPath(@NotNull final File file)
      throws IOException {
    return makePath(file.getCanonicalPath());
  }

  /**
   * Add a trailing separator char to the given path String, if needed.
   *
   * @param path File path string
   * @return Given path with a trailing separator char
   */
  public static String makePath(@NotNull final String path) {
    if (StringUtils.isStrippedEmpty(path)) {
      throw new IllegalArgumentException("Path was empty.");
    }
    if (path.charAt(path.length() - 1) != File.separatorChar) {
      return path + File.separator;
    }
    return path;
  }

  /**
   * Tries to create a file directory.
   *
   * @param filePath Path to use/create
   * @return File instance of the path
   * @throws IOException Thrown, if the path is not a directory, if the path
   * does not exist an could not be created or if reading/writing to this
   * directory is not allowed.
   */
  public static File tryCreatePath(@NotNull final String filePath)
      throws IOException {
    if (StringUtils.isStrippedEmpty(filePath)) {
      throw new IllegalArgumentException("Data path was empty.");
    }

    final File dataDir = new File(filePath);
    if (dataDir.exists()) {
      // check, if path is a directory
      if (!dataDir.isDirectory()) {
        throw new IOException("File '" + dataDir.getCanonicalPath()
            + "' exists, but is not a directory.");
      }
    } else if (!dataDir.mkdirs()) {
      throw new IOException("Error while creating directories '"
          + dataDir.getCanonicalPath() + "'.");
    }
    if (!dataDir.canWrite() || !dataDir.canRead()) {
      throw new IOException("Insufficient rights for directory '"
          + dataDir.getCanonicalPath() + "'.");
    }
    return dataDir;
  }
}
