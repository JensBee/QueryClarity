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

import de.unihildesheim.iw.Tuple;
import de.unihildesheim.iw.Tuple.Tuple2;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
   * Constructor for overriding classes, to pass in an already created {@link
   * Properties} object.
   *
   * @param prop Properties provided by overriding class
   */
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  protected Configuration(final Properties prop) {
    this.data = prop;
  }

  /**
   * Creates a new configuration object initialized with the configuration set
   * from the given map.
   *
   * @param initial Initial set of configuration options
   */
  protected Configuration(@NotNull final Map<String, String> initial) {
    this();
    addAll(initial);
  }

  /**
   * Creates a new empty configuration object.
   */
  public Configuration() {
    this.data = new Properties();
  }

  /**
   * Add all entries from the given map to the configuration.
   *
   * @param config Map with configuration settings
   */
  public final void addAll(@NotNull final Map<String, String> config) {
    for (final Entry<String, String> confEntry : config.entrySet()) {
      this.data.setProperty(confEntry.getKey(), confEntry.getValue());
    }
  }

  /**
   * Set the properties object directly.
   *
   * @param prop Properties to set
   */
  @SuppressWarnings("AssignmentToCollectionOrArrayFieldFromParameter")
  protected final void setProperties(final Properties prop) {
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
   * Tries to get a String value associated with the given key. Adds the default
   * value as new entry to the configuration, if no value is present.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return String value assigned to the key, or {@code defaultValue} if there
   * was none
   * @see #getString(String, String)
   */
  public final String getAndAddString(
      @NotNull final String key,
      @NotNull final String defaultValue) {
    final String value = getString(key, defaultValue);
    if (defaultValue.equals(value)) {
      add(key, defaultValue);
      return defaultValue;
    }
    assert value != null;
    return value;
  }

  /**
   * Tries to get a string value associated with the given key.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return String value assigned to the key, or {@code defaultValue} if there
   * was none
   */
  @Nullable
  public final String getString(
      @NotNull final String key,
      @Nullable final String defaultValue) {
    if (StringUtils.isStrippedEmpty(key)) {
      throw new IllegalArgumentException("Key was empty.");
    }
    return this.data.getProperty(key, defaultValue);
  }

  /**
   * Store a string value under the given key.
   *
   * @param key Key to use for storing
   * @param value Value to store
   */
  public final void add(
      @NotNull final String key,
      @NotNull final String value) {
    this.data.setProperty(checkKey(key), value);
  }

  /**
   * Checks if key is valid (i.e. not empty).
   *
   * @param key Key
   * @return Passed in key
   */
  protected static String checkKey(@NotNull final String key) {
    if (StringUtils.isStrippedEmpty(key)) {
      throw new IllegalArgumentException("Key was empty.");
    }
    return key;
  }

  /**
   * Tries to get a Boolean value associated with the given key. Adds the
   * default value as new entry to the configuration, if no value is present.
   * '1', 'yes', 'true' are recognized as boolean {@code TRUE} values.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return Boolean value assigned to the key, or {@code defaultValue} if there
   * was none
   * @see #getBoolean(String, Boolean)
   */
  @SuppressWarnings("BooleanMethodNameMustStartWithQuestion")
  public final Boolean getAndAddBoolean(
      @NotNull final String key,
      @NotNull final Boolean defaultValue) {
    final String value = getString(key, defaultValue.toString());
    if (defaultValue.toString().equalsIgnoreCase(value)) {
      add(key, defaultValue.toString());
      return defaultValue;
    }
    final Boolean result = getBoolean(key, defaultValue);
    assert result != null;
    return result;
  }

  /**
   * Tries to get a boolean value associated with the given key.
   *
   * @param key Configuration item key
   * @param defaultValue Default value, if none already set
   * @return Boolean value assigned to the key, or {@code null} if there was
   * none
   */
  @SuppressWarnings({"BooleanParameter",
      "BooleanMethodNameMustStartWithQuestion"})
  @Nullable
  public final Boolean getBoolean(
      @NotNull final String key,
      @Nullable final Boolean defaultValue) {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(key, "Key was null."))) {
      throw new IllegalArgumentException("Key was empty.");
    }
    final String value = getString(key);
    if (value == null) {
      return defaultValue;
    } else {
      if ("1".equals(value) || "true".equalsIgnoreCase(value) || "yes"
          .equalsIgnoreCase(value)) {
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    }
  }

  /**
   * Tries to get a string value associated with the given key.
   *
   * @param key Configuration item key
   * @return String value assigned to the key, or {@code null} if there was none
   * or there was an error interpreting the value as integer
   */
  @Nullable
  public final String getString(final String key) {
    return getString(key, null);
  }

  /**
   * Tries to get an integer value associated with the given key.
   *
   * @param key Configuration item key
   * @return Integer value assigned to the key, or {@code null} if there was
   * none or there was an error interpreting the value as integer
   */
  @Nullable
  public final Integer getInteger(final String key) {
    return getInteger(key, null);
  }

  /**
   * Tries to get an integer value associated with the given key.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return Integer value assigned to the key, or {@code defaultValue} if there
   * was none or there was an error interpreting the value as integer
   */
  @Nullable
  public final Integer getInteger(
      @NotNull final String key,
      @Nullable final Integer defaultValue) {
    @Nullable
    final String value = getString(key);
    if (value == null) {
      return defaultValue;
    } else {
      try {
        return Integer.parseInt(value);
      } catch (final NumberFormatException ex) {
        LOG.warn("Failed to restore integer value. key={} val={}", key, value);
        return defaultValue;
      }
    }
  }

  /**
   * Tries to get a Integer value associated with the given key. Adds the
   * default value as new entry to the configuration, if no value is present.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return Integer value assigned to the key, or {@code defaultValue} if there
   * was none or there was an error interpreting the value as integer
   * @see #getInteger(String, Integer)
   */
  public final Integer getAndAddInteger(
      @NotNull final String key,
      @NotNull final Integer defaultValue) {
    if (getString(key) == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No config-data for {}.", key);
      }
      add(key, defaultValue);
      return defaultValue;
    }
    final Integer result = getInteger(key, defaultValue);
    assert result != null;
    return result;
  }

  /**
   * Store a integer value under the given key.
   *
   * @param key Key to use for storing
   * @param value Value to store
   */
  public final void add(
      @NotNull final String key,
      @NotNull final Integer value) {
    this.data.setProperty(checkKey(key), value.toString());
  }

  /**
   * Tries to get a double value associated with the given key.
   *
   * @param key Configuration item key
   * @return Double value assigned to the key, or {@code null} if there was none
   * or there was an error interpreting the value as double
   */
  @Nullable
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
  @Nullable
  public final Double getDouble(
      @NotNull final String key,
      @Nullable final Double defaultValue) {
    final String value = getString(key);
    if (value == null) {
      return defaultValue;
    } else {
      try {
        return Double.parseDouble(value);
      } catch (final NumberFormatException ex) {
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
  public final Double getAndAddDouble(
      @NotNull final String key,
      @NotNull final Double defaultValue) {
    if (getString(key) == null) {
      add(key, defaultValue);
      return defaultValue;
    }
    final Double result = getDouble(key, defaultValue);
    assert result != null;
    return result;
  }

  /**
   * Store a double value under the given key.
   *
   * @param key Key to use for storing
   * @param value Value to store
   */
  public final void add(final String key, final Double value) {
    this.data.setProperty(checkKey(key), value.toString());
  }

  /**
   * Get an iterator for all configuration entries.
   *
   * @return Iterator over all configuration entries
   */
  public Iterator<Entry<Object, Object>> iterator() {
    return Collections.unmodifiableSet(this.data.entrySet()).iterator();
  }

  /**
   * Creates a mapping of the current configuration.
   *
   * @return Configuration values mapped as key, value pairs
   */
  public final Map<String, String> entryMap() {
    final Map<String, String> entries = new HashMap<>(this.data.size());
    for (final Entry<Object, Object> e : this.data.entrySet()) {
      entries.put(e.getKey().toString(), e.getValue().toString());
    }
    return entries;
  }

  /**
   * Creates a list of the current configuration.
   *
   * @return Configuration values list as key, value {@link Tuple2 tuple}
   * pairs
   */
  public final List<Tuple2<String, String>> entryList() {
    final List<Tuple2<String, String>> entries = new ArrayList<>(
        this.data.size());
    for (final Entry<Object, Object> e : this.data.entrySet()) {
      entries.add(Tuple.tuple2(e.getKey().toString(), e.getValue().toString()));
    }
    return entries;
  }
}
