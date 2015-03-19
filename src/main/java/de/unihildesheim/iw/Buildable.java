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

import org.jetbrains.annotations.NotNull;

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
   * @throws BuildableException Thrown, if building the object has failed
   */
  @NotNull
  T build()
      throws BuildableException;

  /**
   * Validates the builder settings.
   *
   * @throws ConfigurationException Thrown, if the current builder configuration
   * is not valid.
   */
  default void validate()
      throws ConfigurationException {
    // NOP
  }

  @SuppressWarnings({"PublicInnerClass", "AbstractClassExtendsConcreteClass"})
  /**
   * Exception for buildable objects.
   */
  public abstract class BuildableException
      extends Exception {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = -7502921394560275176L;

    /**
     * Create a new exception with a specific message.
     *
     * @param msg Message
     * @param t Exception to forward
     */
    BuildableException(final String msg, final Throwable t) {
      super(msg, t);
    }

    /**
     * Create a new exception with a specific message.
     *
     * @param msg Message
     */
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
  @SuppressWarnings("PublicInnerClass")
  public final class ConfigurationException
      extends BuildableException {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = -5780455839142527885L;

    /**
     * Create a new exception with a specific message.
     *
     * @param msg Message
     */
    public ConfigurationException(final String msg) {
      super(msg);
    }

    /**
     * Create a new exception with a specific message.
     *
     * @param msg Message
     * @param t Exception to forward
     */
    public ConfigurationException(final String msg, final Throwable t) {
      super(msg, t);
    }
  }

  /**
   * {@link Exception} {@link Buildable}s to indicate that building the object
   * has failed.
   *
   * @author Jens Bertram
   */
  @SuppressWarnings("PublicInnerClass")
  public final class BuildException
      extends BuildableException {
    /**
     * Serialization id.
     */
    private static final long serialVersionUID = -7413912503761773186L;

    /**
     * Create a new exception with a general message.
     *
     * @param t Exception to forward
     */
    public BuildException(final Throwable t) {
      super("Failed to build instance.", t);
    }

    /**
     * Create a new exception with a specific message.
     *
     * @param msg Message
     */
    public BuildException(final String msg) {
      super(msg);
    }

    /**
     * Create a new exception with a specific message.
     *
     * @param msg Message
     * @param t Exception to forward
     */
    public BuildException(final String msg, final Throwable t) {
      super(msg, t);
    }
  }
}
