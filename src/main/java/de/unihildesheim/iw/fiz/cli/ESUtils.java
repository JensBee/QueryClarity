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

import de.unihildesheim.iw.fiz.Defaults.ES_CONF;
import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.jetbrains.annotations.NotNull;
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
  /**
   * Used to generate randomized delays for request throttling.
   */
  private static final Random RAND = new Random();

  /**
   * Runs a REST request against the ES instance. Optionally retrying the
   * request {@link ES_CONF#MAX_RETRY} times, if a request has timed out.
   *
   * @param client Jest client
   * @param action Request action
   * @return Request result
   * @throws Exception Thrown on any error while performing the request
   */
  @SuppressWarnings("BusyWait")
  public static JestResult runRequest(
      @NotNull final JestClient client,
      @NotNull final Action action)
      throws Exception {
    int tries = 0;
    while (tries <= ES_CONF.MAX_RETRY) {
      try {
        return client.execute(action);
      } catch (final SocketTimeoutException ex) {
        tries++;
        // connection timed out - retry after a short delay
        int delay = (1 + RAND.nextInt(10)) * 100 * tries;
        LOG.warn("Timeout - retry {}/{} ~{}..",
            tries, ES_CONF.MAX_RETRY, delay);
        Thread.sleep((long) delay);
      }
    }
    // retries maxed out
    throw new RuntimeException(
        "Giving up trying to connect after " + tries + ' ' +
            "retries.");
  }
}
