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
package de.unihildesheim.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;
import org.slf4j.LoggerFactory;

/**
 * Configuration manager.
 *
 * @author Jens Bertram <code@jens-bertram.net>
 */
public final class Configuration {

  /**
   * Logger instance for this class.
   */
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(
          Configuration.class);

  private static Configuration instance;

  /**
   * Configuration properties.
   */
  private static Properties conf;

  /**
   * Name of the configuration file to load.
   */
  private static String confFile;

  /**
   * Flag indicating, if this instance is initialized.
   */
  private static boolean initialized = false;

  /**
   * Initialize this singleton with a specific configuration file name.
   *
   * @param fileName Configuration file name to use
   */
  public static void initInstance(final String fileName) {
    if (initialized) {
      throw new IllegalStateException("Instance already initialized.");
    }
    if (fileName.endsWith(".properties")) {
      confFile = fileName;
    } else {
      confFile = fileName + ".properties";
    }
    instance = new Configuration();
    initialized = true;
  }

  /**
   * Check if an instance is initialized.
   */
  private static void checkInstance() {
    if (!initialized) {
      throw new IllegalStateException("Instance not initialized.");
    }
  }

  /**
   * Private singleton constructor.
   */
  private Configuration() {
    conf = new Properties();
    try (InputStream resIn = Thread.currentThread().
            getContextClassLoader().getResourceAsStream(
                    confFile)) {

              if (resIn != null) {
                //load a properties file from class path, inside static method
                conf.load(resIn);
              }
            } catch (IOException ex) {
              LOG.info("No configration file found. "
                      + "Creating a new one upon exit.");
            }
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
              @Override
              public void run() {
                saveConfig();
              }
            }, "Configuration_shurdownHandler"));
  }

  /**
   * Save the current configuration back to disk.
   */
  private void saveConfig() {
    try (OutputStream propFile = new FileOutputStream(confFile)) {
      conf.store(propFile, null);
    } catch (IOException ex) {
      LOG.error("Error while storing configuration.", ex);
    }
  }

  /**
   * Get an configuration item by key.
   *
   * @param key Configuration item key
   * @return Value assigned to the key, or <tt>null</tt> if there was none
   */
  public static String get(final String key) {
    checkInstance();
    return conf.getProperty(key);
  }

  /**
   * Get an configuration item by key, specifying a default value.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key
   * was found
   * @return Value assigned to the key, or <tt>defaultValue</tt> if there was
   * none
   */
  public static String get(final String key, final String defaultValue) {
    if (!initialized) {
      return defaultValue;
    }
    if (!conf.containsKey(key)) {
      // push missing value to store
      conf.setProperty(key, defaultValue);
    }
    return conf.getProperty(key, defaultValue);
  }

  /**
   * Tries to get an integer value associated with the given key.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key
   * was found
   * @return Integer value assigned to the key, or <tt>defaultValue</tt> if
   * there was none
   */
  public static Integer getInt(final String key, final Integer defaultValue) {
    if (!initialized) {
      return defaultValue;
    }
    String value = get(key);
    if (value == null) {
      // push missing value to store
      get(key, defaultValue.toString());
      return defaultValue;
    } else {
      return Integer.parseInt(value);
    }
  }

  /**
   * Tries to get an integer value associated with the given key.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key
   * was found
   * @return Double value assigned to the key, or <tt>defaultValue</tt> if
   * there was none
   */
  public static Double getDouble(final String key, final Double defaultValue) {
    if (!initialized) {
      return defaultValue;
    }
    String value = get(key);
    if (value == null) {
      // push missing value to store
      get(key, defaultValue.toString());
      return defaultValue;
    } else {
      return Double.parseDouble(value);
    }
  }
}
