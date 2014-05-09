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

package de.unihildesheim.iw.lucene.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context providing Lucene run-time related information.
 * @author Jens Bertram
 */
public class RuntimeContext {

  /**
   * Private constructor.
   */
  private RuntimeContext() {}

  private static RuntimeContext build(final Builder builder) {
    final RuntimeContext instance = new RuntimeContext();
    return instance;
  }

  /**
   * Builder to create a {@link RuntimeContext}.
   */
  @SuppressWarnings("PublicInnerClass")
  public static final class Builder {
    /**
     * Logger instance for this class.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Builder.class);



    /**
     * Validates the current builder configuration.
     *
     * @throws ContextCreationException Thrown, if not all required parameters
     * are configured or a lower-level exception got caught while initializing
     * the context.
     */
    private void validate()
        throws ContextCreationException {
    }

    /**
     * Build the {@link RuntimeContext} using the current builder configuration.
     *
     * @return New {@link RuntimeContext} instance
     * @throws ContextCreationException Thrown, if not all required parameters
     * are configured or a lower-level exception got caught while initializing
     * the context.
     * @throws java.io.IOException Thrown on low-level I/O errors
     */
    public RuntimeContext build()
        throws ContextCreationException {
      validate();
      return RuntimeContext.build(this);
    }
  }
}
