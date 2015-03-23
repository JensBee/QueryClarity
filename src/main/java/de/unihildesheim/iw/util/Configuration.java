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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
   * Default constnt empty string value.
   */
  private static final String EMPTY_STRING = "";

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
    return value;
  }

  /**
   * Tries to get a string value associated with the given key.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return String value assigned to the key, or {@code #EMPTY_STRING an empty
   * string} if there was none
   */
  @NotNull
  public final String getString(
      @NotNull final String key,
      @Nullable final String defaultValue) {
    String value = defaultValue == null ? EMPTY_STRING : defaultValue;
    if (!StringUtils.isStrippedEmpty(key)) {
      value = this.data.getProperty(key, defaultValue);
      if (value == null) {
        value = EMPTY_STRING;
      }
    }
    return value;
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
   * @see #getBoolean(String, boolean)
   */
  @SuppressWarnings({"BooleanMethodNameMustStartWithQuestion",
      "BooleanParameter"})
  public final boolean getAndAddBoolean(
      @NotNull final String key, final boolean defaultValue) {
    final String valueStr = defaultValue ? "1" : "0";
    final String value = getString(key, valueStr);
    final boolean result;
    if (value.isEmpty()) {
      add(key, valueStr);
      result = defaultValue;
    } else {
      result = getBoolean(key, defaultValue);
    }
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
  public final boolean getBoolean(
      @NotNull final String key,
      final boolean defaultValue) {
    if (StringUtils.isStrippedEmpty(
        Objects.requireNonNull(key, "Key was null."))) {
      throw new IllegalArgumentException("Key was empty.");
    }
    final String value = getString(key);
    if (value.isEmpty()) {
      return defaultValue;
    } else {
      return "1".equals(value) || "true".equalsIgnoreCase(value) || "yes"
          .equalsIgnoreCase(value);
    }
  }

  /**
   * Tries to get a string value associated with the given key.
   *
   * @param key Configuration item key
   * @return String value assigned to the key, or an {@link #EMPTY_STRING empty
   * String}, if there was none or there was an error interpreting the value as
   * integer
   */
  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
  @NotNull
  private String getString(@NotNull final String key) {
    return getString(key, EMPTY_STRING);
  }

  /**
   * Tries to get an integer value associated with the given key.
   *
   * @param key Configuration item key
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return Integer value assigned to the key, or {@code defaultValue} if there
   * was none
   */
  public final int getInteger(
      @NotNull final String key,
      final int defaultValue) {
    @Nullable
    final String value = getString(key);
    final int result;
    if (value.isEmpty()) {
      result = defaultValue;
    } else {
      result = Integer.parseInt(value);
    }
    return result;
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
   * @see #getInteger(String, int)
   */
  public final Integer getAndAddInteger(
      @NotNull final String key,
      final int defaultValue) {
    final int result;
    if (getString(key).isEmpty()) {
      add(key, defaultValue);
      result = defaultValue;
    } else {
      result = getInteger(key, defaultValue);
    }
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
   * @param defaultValue Default value to use, if no data for the given key was
   * found
   * @return Double value assigned to the key, or <tt>defaultValue</tt> if there
   * was none
   */
  public final double getDouble(
      @NotNull final String key,
      final double defaultValue) {
    final String value = getString(key);
    final double result;
    if (value.isEmpty()) {
      result = defaultValue;
    } else {
      result = Double.parseDouble(value);
    }
    return result;
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
   * @see #getDouble(String, double)
   */
  public final double getAndAddDouble(
      @NotNull final String key,
      final double defaultValue) {
    final double result;
    if (getString(key).isEmpty()) {
      add(key, defaultValue);
      result = defaultValue;
    } else {
      result = getDouble(key, defaultValue);
    }
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
   * @return Configuration values list as key, value {@link Tuple2 tuple} pairs
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
