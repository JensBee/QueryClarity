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

import de.unihildesheim.iw.TestCase;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link ConfigurationFile}.
 *
 * @author Jens Bertram
 */
@SuppressWarnings("JavaDoc")
public class ConfigurationFileTest
    extends TestCase {
  public ConfigurationFileTest() {
    super(LoggerFactory.getLogger(ConfigurationFileTest.class));
  }

  @Test
  public void testSave()
      throws Exception {
    final ConfigurationFile cf = new ConfigurationFile("testconf.properties");
    cf.getAndAddString("foo", "bar");
    cf.save();
  }
}