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

package de.unihildesheim.iw.cli;

import au.com.bytecode.opencsv.CSVWriter;
import de.unihildesheim.iw.Buildable;
import de.unihildesheim.iw.ByteArray;
import de.unihildesheim.iw.Persistence;
import de.unihildesheim.iw.SerializableByte;
import de.unihildesheim.iw.lucene.index.AbstractIndexDataProviderBuilder;
import de.unihildesheim.iw.lucene.index.DirectIndexDataProvider;
import de.unihildesheim.iw.util.ByteArrayUtils;
import de.unihildesheim.iw.util.FileUtils;
import de.unihildesheim.iw.util.StringUtils;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * @author Jens Bertram
 */
public class DidpDump
    extends CliBase {
  /**
   * Logger instance for this class.
   */
  static final Logger LOG =
      LoggerFactory.getLogger(DidpDump.class);
  /**
   * Object wrapping commandline options.
   */
  private final Params cliParams = new Params();

  /**
   * Initialize the cli base class.
   */
  private DidpDump() {
    super("DirectIndexDataProvider data dumper.", "Dump varoius data elements" +
        " from a DirectIndexDataProvider database.");
  }

  /**
   * Main method.
   *
   * @param args Commandline arguments.
   */
  public static void main(final String[] args)
      throws IOException, Buildable.ConfigurationException,
             Buildable.BuildException {
    new DidpDump().runMain(args);
    System.exit(0); // required to trigger shutdown-hooks
  }

  /**
   * Class setup.
   *
   * @param args Commandline arguments.
   */
  private void runMain(final String[] args)
      throws IOException, Buildable.BuildException,
             Buildable.ConfigurationException {
    parseWithHelp(this.cliParams, args);

    // check, if files and directories are sane
    this.cliParams.check();

    final Persistence.Builder persistenceBuilder = new Persistence.Builder();
    persistenceBuilder.name(AbstractIndexDataProviderBuilder.createCacheName
        (DirectIndexDataProvider.getIdentifier(), this.cliParams.prefix + "_"
            + this.cliParams.lang));
    persistenceBuilder.dataPath(FileUtils.getPath(this.cliParams.dataPath));
    final Persistence persist = persistenceBuilder.get().build();
    final DB db = persist.getDb();

    final CSVWriter writer = new CSVWriter(new FileWriter(new File
        ("idx_terms_map.csv")));
    if (db.exists(DirectIndexDataProvider.CacheDbMakers.Stores.IDX_TERMS_MAP
        .name())) {
      Map<Fun.Tuple2<SerializableByte, ByteArray>, Long> termsMap =
          DirectIndexDataProvider.CacheDbMakers.idxTermsMapMkr(db)
              .makeOrGet();
      final String[] data = new String[3];
      for (Map.Entry<Fun.Tuple2<SerializableByte, ByteArray>,
          Long> e : termsMap.entrySet()) {
        data[0] = ByteArrayUtils.utf8ToString(e.getKey().b);
        data[1] = e.getValue().toString();
        writer.writeNext(data);
      }
      writer.close();
    }
  }

  /**
   * Wrapper for commandline options.
   */
  private static final class Params {
    /**
     * Source file containing extracted claims.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-i", metaVar = "FILE", required = true,
        usage = "Path where the data db resides in.")
    File dataPath;
    /**
     * Single languages.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-lang", metaVar = "language", required = true,
        usage = "Process the defined language.")
    String lang;
    /**
     * Prefix for cache data.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-prefix", metaVar = "name", required = true,
        usage = "Naming prefix for cached data files to load or create.")
    String prefix;
    /**
     * Data elements to dump.
     */
    @SuppressWarnings("PackageVisibleField")
    @Option(name = "-data", metaVar = "dataToDump", required = true,
        handler = StringArrayOptionHandler.class,
        usage = "List of data elements to dump.")
    String[] data;

    /**
     * Check, if the defined files and directories are available.
     */
    void check() {
      this.lang = StringUtils.lowerCase(this.lang);
    }
  }
}
