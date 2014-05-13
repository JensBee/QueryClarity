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

/**
 * Interface for builder classes.
 *
 * @param <T> Type to build.
 * @author Jens Bertram
 */
public interface Buildable<T> {
  /**
   * Builds the desired object. Should call {@link #validate()} before
   * building.
   *
   * @return New Object instance
   */
  T build()
      throws BuildableException;

  /**
   * Validates the builder settings.
   *
   * @throws ConfigurationException Thrown, if the current builder configuration
   * is not valid.
   * @throws Exception Any exception may be thrown by implementing classes
   */
  void validate()
      throws ConfigurationException;

  public static abstract class BuildableException
      extends Exception {
    BuildableException(final String msg, final Exception e) {
      super(msg, e);
    }

    BuildableException(final String msg) {
      super(msg);
    }
  }

  /**
   * {@link Exception} {@link Buildable}s to indicate that a mandatory setting
   * is missing or invalid.
   *
   * @author Jens Bertram
   */
  public static final class ConfigurationException
      extends BuildableException {
    public ConfigurationException(final String msg) {
      super(msg);
    }

    public ConfigurationException(final String msg, Exception ex) {
      super(msg, ex);
    }
  }

  /**
   * {@link Exception} {@link Buildable}s to indicate that building the object
   * has failed.
   *
   * @author Jens Bertram
   */
  public static final class BuildException
      extends BuildableException {
    public BuildException(final Exception ex) {
      super("Failed to build instance.", ex);
    }

    public BuildException(final String msg) {
      super(msg);
    }

    public BuildException(final String msg, Exception ex) {
      super(msg, ex);
    }
  }
}
