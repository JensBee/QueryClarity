/*
 * Copyright (C) 2015 Jens Bertram (code@jens-bertram.net)
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

package de.unihildesheim.iw.fiz.cli;

import de.unihildesheim.iw.fiz.Defaults;
import de.unihildesheim.iw.fiz.Defaults.ES_CONF;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.slf4j.Logger;

import java.net.SocketTimeoutException;
import java.util.Random;

/**
 * @author Jens Bertram (code@jens-bertram.net)
 */
public final class ESUtils {
  /**
   * Logger instance for this class.
   */
  private static final Logger LOG =
      org.slf4j.LoggerFactory.getLogger(ESUtils.class);
  /** Used to generate randomized delays for request throttling. */
  private static final Random rand = new Random();

  /**
   * Runs a REST request against the ES instance. Optionally retrying the
   * request {@link Defaults.ES_CONF#MAX_RETRY} times, if a request has timed out.
   * @param action Request action
   * @return Request result
   * @throws Exception Thrown on any error while performing the request
   */
  public static JestResult runRequest(final JestClient client, final Action
      action)
      throws Exception {
    int tries = 0;
    while (tries < ES_CONF.MAX_RETRY) {
      try {
        return client.execute(action);
      } catch (final SocketTimeoutException ex) {
        // connection timed out - retry after a short delay
        final int delay = (1 + rand.nextInt(10)) * 100;
        LOG.warn("Timeout - retry ~{}..", delay);
        Thread.sleep((long) delay);
        tries++;
      }
    }
    // retries maxed out
    throw new RuntimeException("Giving up trying to connect after "+tries+" " +
        "retries.");
  }

}
