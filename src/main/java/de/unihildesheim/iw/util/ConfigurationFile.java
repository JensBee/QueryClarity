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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * File based configuration store.
 *
 * @author Jens Bertram
 */
public class ConfigurationFile
    extends Configuration {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      ConfigurationFile.class);

  /**
   * Properties store.
   */
  private final Properties prop;

  /**
   * Name of the properties file.
   */
  private final String fileName;

  /**
   * Flag indicating, if a ShutdownHook is already set.
   */
  private boolean hasShutdownHook = false;

  /**
   * Creates a new file-backed configuration storage with the given name.
   *
   * @param newFileName Name of the properties file
   * @throws IOException Thrown on low-level I/O errors
   */
  public ConfigurationFile(final String newFileName)
      throws IOException {
    super();
    if (Objects.requireNonNull(newFileName, "Filename was null.").trim()
        .isEmpty()) {
      throw new IllegalArgumentException("Empty filename.");
    }
    this.fileName = newFileName;
    this.prop = new Properties();
    try (FileReader reader = new FileReader(this.fileName)) {
      this.prop.load(reader);
    }
    LOG.info("Configuration loaded from '{}'", this.fileName);
    setProperties(prop);
  }

  /**
   * Saves the current configuration to disk.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  public void save()
      throws IOException {
    try (OutputStream output = new FileOutputStream(this.fileName)) {
      prop.store(output, null);
      output.close();
    }
  }

  /**
   * Activates the storing of the configuration to disk when the runtime is
   * terminated.
   */
  public void saveOnExit() {
    if (hasShutdownHook) {
      return;
    }
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          save();
        } catch (IOException e) {
          LOG.error("Error saving configuration.", e);
        }
        LOG.info("Configuration saved.");
      }
    });
    this.hasShutdownHook = true;
  }
}
