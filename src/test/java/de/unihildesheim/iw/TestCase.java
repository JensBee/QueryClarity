/*
 * Copyright (C) 2014 Jens Bertram
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

import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;

/**
 * Generic test case template.
 *
 * @author Jens Bertram
 */
public abstract class TestCase {
  /**
   * Logger instance.
   */
  private final Logger log;

  /**
   * Init test-case and set the Logger instance.
   * @param l Logger instance
   */
  public TestCase(final Logger l) {
    this.log = l;
  }

  @SuppressWarnings(
      {"PackageVisibleField", "AnonymousInnerClassMayBeStatic", "PublicField"})
  @Rule
  /**
   * Print current test method.
   */
  public TestRule watcher = new TestWatcher() {
    @Override
    protected void starting(final Description description) {
      log.info("{} being run...", description.getMethodName());
    }
  };
}
