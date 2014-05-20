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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;

/**
 * General basic configuration management class.
 */
public class Configuration {

  /**
   * Logger instance for this class.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      Configuration.class);

  /**
   * Configuration properties.
   */
  private Properties data;

  /**
   * Creates a new empty configuration object.
   */
  public Configuration() {
    this.data = new Properties();
  }

  /**
   * Constructor for overriding classes, to pass in an already created {@link
   * Properties} object.
   * @param prop Properties provided by overriding class
   */
  protected Configuration(final Properties prop) {
    this.data = prop;
  }

  /**
   * Creates a new configuration object initialized with the configuration set
   * from the given map.
   *
   * @param initial Initial set of configuration options
   */
  public Configuration(final Map<String, String> initial) {
    this();
    addAll(Objects.requireNonNull(initial));
  }

  protected void setProperties(final Properties prop) {
    this.data = prop;
  }

  /**
   * Dump the configuration to the logger.
   */
  public final void debugDump() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Dumping configuration - start");
      for (final Entry<Object, Object> conf : this.data.entrySet()) {
        LOG.debug(" [{}]={}", conf.getKey(), conf.getValue());
      }
      LOG.debug("Dumping configuration - done");
    }
  }

  /**
   * Add all entries from the given map to the configuration.
   *
   * @param config Map with configuration settings
   */
  public final void addAll(final Map<String, String> config) {
    Objects.requireNonNull(config);
    for (final Entry<String, String> confEntry : config.entrySet()) {
      this.data.setProperty(confEntry.getKey(), confEntry.getValue());
    }
  }

  /**
   * Store a string value under the given key.
   *
   * @param key Key to use for storing
   * @param value Value to store
   */
  public final void add(final String key, final String value) {
    checkKeyValue(key, value);
    this.data.setProperty(key, value);
  }

  /**
   * Store a integer value under the given key.
   *
   * @param key Key to use for storing
   * @param value Value to store
   */
  public final void add(final String key, final Integer value) {
    checkKeyValue(key, value);
    this.data.setProperty(key, value.toString());
  }

  protected void checkKeyValue(final String key, final Object value) {
    if (Objects.requireNonNull(key).trim().isEmpty()) {
      throw new IllegalArgumentException("Key was empty.");
    }
    Objects.requireNonNull(value);
  }

  /**
   * Store a double value under the given key.
   *
   * @param key Key to use for storing
   * @param value Value to store
   */
  public final void add(final String key, final Double value) {
    checkKeyValue(key, value);
    this.data.setProperty(key, value.toString());
  }

  /**
   * Tries to get a string value associated with the given key.
   *
   * @param key Configuration item key
   * @return String value assigned to the key, or <tt>null</tt> if there was
   * none or there was an error interpreting the value as integer
   */
  public final String getString(final String key) {
    return getString(key, null);
  }

  /**
   * Tries to get a string value associated with the given key.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return String value assigned to the key, or <tt>defaultValue</tt> if there
   * was none
   */
  public final String getString(final String key,
      final String defaultValue) {
    if (Objects.requireNonNull(key).trim().isEmpty()) {
      throw new IllegalArgumentException("Key was empty.");
    }
    return this.data.getProperty(key, defaultValue);
  }

  /**
   * Tries to get a String value associated with the given key. Adds the
   * default value as new entry to the configuration, if no value is present.
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return String value assigned to the key, or <tt>defaultValue</tt> if there
   * was none
   * @see #getString(String, String)
   */
  public final String getAndAddString(final String key,
      final String defaultValue) {
    final String value = getString(key, defaultValue);
    if (defaultValue.equals(value)) {
      add(key, defaultValue);
      return defaultValue;
    }
    return value;
  }

  /**
   * Tries to get an integer value associated with the given key.
   *
   * @param key Configuration item key
   * @return Integer value assigned to the key, or <tt>null</tt> if there was
   * none or there was an error interpreting the value as integer
   */
  public final Integer getInteger(final String key) {
    return getInteger(key, null);
  }

  /**
   * Tries to get an integer value associated with the given key.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return Integer value assigned to the key, or <tt>defaultValue</tt> if
   * there was none or there was an error interpreting the value as integer
   */
  public final Integer getInteger(final String key,
      final Integer defaultValue) {
    final String value = getString(key);
    if (value == null) {
      return defaultValue;
    } else {
      try {
        return Integer.parseInt(value);
      } catch (NumberFormatException ex) {
        LOG.warn("Failed to restore integer value. key={} val={}", key, value);
        return defaultValue;
      }
    }
  }

  /**
   * Tries to get a Integer value associated with the given key. Adds the
   * default value as new entry to the configuration, if no value is present.
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return Integer value assigned to the key, or <tt>defaultValue</tt> if
   * there was none or there was an error interpreting the value as integer
   * @see #getInteger(String, Integer)
   */
  public final Integer getAndAddInteger(final String key,
      final Integer defaultValue) {
    if (getString(key) == null) {
      add(key, defaultValue);
      return defaultValue;
    }
    return getInteger(key, defaultValue);
  }

  /**
   * Tries to get a double value associated with the given key.
   *
   * @param key Configuration item key
   * @return Double value assigned to the key, or <tt>null</tt> if there was
   * none or there was an error interpreting the value as double
   */
  public final Double getDouble(final String key) {
    return getDouble(key, null);
  }

  /**
   * Tries to get a double value associated with the given key.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return Double value assigned to the key, or <tt>defaultValue</tt> if there
   * was none or there was an error interpreting the value as double
   */
  public final Double getDouble(final String key,
      final Double defaultValue) {
    final String value = getString(key);
    if (value == null) {
      return defaultValue;
    } else {
      try {
        return Double.parseDouble(value);
      } catch (NumberFormatException ex) {
        LOG.warn("Failed to restore double value. key={} val={}", key, value);
        return defaultValue;
      }
    }
  }

  /**
   * Tries to get a double value associated with the given key. Adds the default
   * value as new entry to the configuration, if no value is present.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return Double value assigned to the key, or <tt>defaultValue</tt> if there
   * was none or there was an error interpreting the value as double
   * @see #getDouble(String, Double)
   */
  public final Double getAndAddDouble(final String key,
      final Double defaultValue) {
    if (getString(key) == null) {
      add(key, defaultValue);
      return defaultValue;
    }
    return getDouble(key, defaultValue);
  }
}
