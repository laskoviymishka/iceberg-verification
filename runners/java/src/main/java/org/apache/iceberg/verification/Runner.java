/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.verification;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;

/**
 * iceberg-java (reference) conformance runner for the iceberg-verification corpus.
 *
 * <p>Parses a logical op-log (the YAML authoring profile), materializes a fresh SQLite +
 * {@code file://} table, drives Iceberg's Java API, scans, and emits canonical output matching
 * {@code expected-output.schema.json}. Emit-only — it never compares.
 *
 * <pre>runner --spec &lt;l-log.yaml&gt; --warehouse &lt;dir&gt; --out &lt;canonical.json&gt;</pre>
 *
 * Exit codes (spec/runner-contract.md): 0 ok · 2 spec invalid · 3 rejected · 4 unsupported · 1 crash.
 */
public final class Runner {
  private static final int EXIT_OK = 0;
  private static final int EXIT_CRASH = 1;
  private static final int EXIT_SPEC_INVALID = 2;
  private static final int EXIT_UNSUPPORTED = 4;

  public static void main(String[] args) {
    System.exit(run(args));
  }

  private static int run(String[] args) {
    Map<String, String> opts = parseArgs(args);
    String specPath = opts.get("spec");
    String warehouse = opts.get("warehouse");
    String outPath = opts.get("out");
    if (specPath == null || warehouse == null || outPath == null) {
      System.err.println("usage: runner --spec <l-log> --warehouse <dir> --out <canonical.json>");
      return EXIT_CRASH;
    }

    LLog log;
    try {
      log = LLog.parse(Paths.get(specPath));
      validate(log);
    } catch (Exception e) {
      System.err.println("spec parse error: " + e.getMessage());
      return EXIT_SPEC_INVALID;
    }

    JdbcCatalog catalog;
    try {
      catalog = openCatalog(warehouse);
    } catch (Exception e) {
      System.err.println("catalog setup error: " + e.getMessage());
      return EXIT_CRASH;
    }

    try {
      Interpret interp = new Interpret(catalog, TableIdentifier.of("default", "t"));
      Emit out = interp.run(log);
      writeOutput(Paths.get(outPath), out);
      return EXIT_OK;
    } catch (Interpret.UnsupportedFeature u) {
      System.err.println("unsupported: " + u.getMessage());
      return EXIT_UNSUPPORTED;
    } catch (Exception e) {
      System.err.println("execution error: " + e);
      return EXIT_CRASH;
    }
  }

  private static void validate(LLog log) {
    if (log.entries.isEmpty()) {
      throw new IllegalArgumentException("spec has no entries");
    }
    Object fv = log.header.get("format-version");
    int v = fv == null ? 0 : ((Number) fv).intValue();
    if (v < 1 || v > 3) {
      throw new IllegalArgumentException("invalid format-version " + v);
    }
  }

  private static JdbcCatalog openCatalog(String warehouse) throws Exception {
    Path abs = Paths.get(warehouse).toAbsolutePath();
    Files.createDirectories(abs);
    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.URI, "jdbc:sqlite:" + abs.resolve("catalog.db"));
    props.put(CatalogProperties.WAREHOUSE_LOCATION, abs.toString());
    JdbcCatalog catalog = new JdbcCatalog();
    catalog.setConf(new Configuration());
    catalog.initialize("conformance", props);
    return catalog;
  }

  private static void writeOutput(Path path, Emit out) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    // The emit POJOs expose package-private fields, not getters — tell Jackson to
    // serialize fields directly.
    mapper.setVisibility(
        com.fasterxml.jackson.annotation.PropertyAccessor.FIELD,
        com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY);
    String json = mapper.writeValueAsString(out) + "\n";
    Files.writeString(path, json);
  }

  private static Map<String, String> parseArgs(String[] args) {
    Map<String, String> opts = new HashMap<>();
    for (int i = 0; i + 1 < args.length; i += 2) {
      if (args[i].startsWith("--")) {
        opts.put(args[i].substring(2), args[i + 1]);
      }
    }
    return opts;
  }

  private Runner() {}
}
