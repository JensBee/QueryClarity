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

import java.io.File;
import java.io.FileNotFoundException;
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
  static final Logger LOG = LoggerFactory.getLogger(
      ConfigurationFile.class);

  /**
   * Properties store.
   */
  private final Properties prop;

  /**
   * File holding our properties.
   */
  private final File confFile;

  /**
   * Flag indicating, if a ShutdownHook is already set.
   */
  private boolean hasShutdownHook;

  /**
   * Creates a new file-backed configuration storage with the given name.
   *
   * @param newFileName Name of the properties file
   * @throws IOException Thrown on low-level I/O errors
   */
  public ConfigurationFile(final String newFileName)
      throws IOException {
    this(newFileName, true);
  }

  /**
   * New configuration file manager with a given file. File is created on
   * request.
   *
   * @param newFileName File name for storing configuration
   * @param create If true, file will be created, if it does not exist
   * @throws IOException Thrown, if file was not found or could not be created
   */
  @SuppressWarnings("BooleanParameter")
  public ConfigurationFile(final String newFileName, final boolean create)
      throws IOException {
    if (Objects.requireNonNull(newFileName, "Filename was null.").trim()
        .isEmpty()) {
      throw new IllegalArgumentException("Empty filename.");
    }

    this.prop = new Properties();
    this.confFile = new File(newFileName);

    if (this.confFile.exists()) {
      try (FileReader reader = new FileReader(this.confFile)) {
        this.prop.load(reader);
        LOG.info("Configuration loaded from '{}'", newFileName);
      }
    } else if (create) {
      if (this.confFile.createNewFile()) {
        LOG.info("New configuration created '{}'", newFileName);
      } else {
        throw new IOException(
            "Error creating configuration file '" + this.confFile
                .getPath() + "'.");
      }
    } else {
      throw new FileNotFoundException("Configuration file '" + this.confFile
          .getPath() + "' not found.");
    }

    setProperties(this.prop);
  }

  /**
   * Activates the storing of the configuration to disk when the runtime is
   * terminated.
   */
  public final void saveOnExit() {
    if (this.hasShutdownHook) {
      return;
    }
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          save();
        } catch (final IOException e) {
          LOG.error("Error saving configuration.", e);
        }
        LOG.info("Configuration saved.");
      }
    });
    this.hasShutdownHook = true;
  }

  /**
   * Saves the current configuration to disk.
   *
   * @throws IOException Thrown on low-level I/O errors
   */
  public final void save()
      throws IOException {
    try (OutputStream output = new FileOutputStream(this.confFile)) {
      this.prop.store(output, null);
      output.close();
    }
  }
}
