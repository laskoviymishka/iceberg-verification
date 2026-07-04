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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Restores a checked-in read fixture's bytes to their pinned absolute root (so the
 * metadata's embedded {@code file://} paths resolve) via the shared
 * {@code tools/materialize.py}. The rewrite logic lives in the one Python script, not
 * per-runner; this just invokes it and returns the metadata.json path to load.
 */
final class Materialize {
  private Materialize() {}

  /** Materialize {@code fixtureDir} (which contains bytes/) and return the metadata.json path. */
  static String materialize(Path fixtureDir) throws IOException {
    Path script = findMaterializer(fixtureDir);
    ProcessBuilder pb =
        new ProcessBuilder("python3", script.toString(), fixtureDir.toString());
    pb.redirectErrorStream(false);
    Process proc = pb.start();
    String stdout = new String(proc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    String stderr = new String(proc.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
    int code;
    try {
      code = proc.waitFor();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("materialize.py interrupted", e);
    }
    if (code != 0) {
      throw new IOException("materialize.py failed (" + code + "): " + stderr);
    }
    String meta = stdout.strip();
    if (meta.isEmpty()) {
      throw new IOException("materialize.py produced no metadata path");
    }
    return meta;
  }

  /** Walk up from the fixture dir to locate tools/materialize.py at the corpus root. */
  private static Path findMaterializer(Path fixtureDir) throws IOException {
    Path dir = fixtureDir.toAbsolutePath().normalize();
    while (dir != null) {
      Path candidate = dir.resolve("tools").resolve("materialize.py");
      if (Files.isRegularFile(candidate)) {
        return candidate;
      }
      dir = dir.getParent();
    }
    throw new IOException("tools/materialize.py not found above " + fixtureDir);
  }
}
