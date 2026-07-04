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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;

/**
 * Loads the logical op-log (the YAML authoring profile of spec/l-log.schema.json)
 * while preserving the physical-type tags (!int, !long, !struct, ...). SnakeYAML's
 * standard constructors reject unknown tags, so we walk the node tree directly and
 * lower it into plain Java containers plus {@link TypedValue} leaves.
 */
final class LLog {
  final Map<String, Object> header;
  final List<Map<String, Object>> entries;

  private LLog(Map<String, Object> header, List<Map<String, Object>> entries) {
    this.header = header;
    this.entries = entries;
  }

  /** Parse an op-log file. Values carrying a physical-type tag become TypedValue. */
  @SuppressWarnings("unchecked")
  static LLog parse(Path path) throws IOException {
    LoaderOptions opts = new LoaderOptions();
    opts.setTagInspector(tag -> true); // allow custom tags; we interpret them ourselves
    Yaml yaml = new Yaml(new SafeConstructor(opts));
    Node root = yaml.compose(Files.newBufferedReader(path));
    if (root == null) {
      throw new IllegalArgumentException("empty spec");
    }
    Object tree = lower(root);
    if (!(tree instanceof Map)) {
      throw new IllegalArgumentException("spec root must be a mapping");
    }
    Map<String, Object> map = (Map<String, Object>) tree;
    Object header = map.get("header");
    Object entries = map.get("entries");
    if (!(header instanceof Map)) {
      throw new IllegalArgumentException("spec missing 'header' mapping");
    }
    if (!(entries instanceof List)) {
      throw new IllegalArgumentException("spec missing 'entries' list");
    }
    return new LLog((Map<String, Object>) header, (List<Map<String, Object>>) entries);
  }

  /**
   * Lower a SnakeYAML node into plain Java: mappings to LinkedHashMap, sequences to
   * ArrayList, untagged scalars to their standard Java type, and any scalar/collection
   * carrying a non-standard '!tag' to a {@link TypedValue}.
   */
  private static Object lower(Node node) {
    String tag = node.getTag().getValue();
    boolean custom = tag.startsWith("!");

    if (node instanceof ScalarNode scalar) {
      if (custom) {
        return TypedValue.primitive(stripBang(tag), scalar.getValue());
      }
      return standardScalar(scalar);
    }

    if (node instanceof SequenceNode seq) {
      List<Object> items = new ArrayList<>();
      for (Node child : seq.getValue()) {
        items.add(lower(child));
      }
      if (custom) {
        return TypedValue.list(stripBang(tag), items);
      }
      return items;
    }

    if (node instanceof MappingNode mapping) {
      Map<String, Object> map = new LinkedHashMap<>();
      for (NodeTuple tuple : mapping.getValue()) {
        String key = ((ScalarNode) tuple.getKeyNode()).getValue();
        map.put(key, lower(tuple.getValueNode()));
      }
      if (custom) {
        return TypedValue.mapping(stripBang(tag), map);
      }
      return map;
    }

    throw new IllegalArgumentException("unsupported yaml node: " + node);
  }

  /** Decode a standard (untagged) YAML scalar to Java per its resolved core tag. */
  private static Object standardScalar(ScalarNode scalar) {
    String tag = scalar.getTag().getValue();
    String v = scalar.getValue();
    return switch (tag) {
      case "tag:yaml.org,2002:int" -> Long.parseLong(v);
      case "tag:yaml.org,2002:float" -> Double.parseDouble(v);
      case "tag:yaml.org,2002:bool" -> Boolean.parseBoolean(v);
      case "tag:yaml.org,2002:null" -> null;
      default -> v; // strings
    };
  }

  private static String stripBang(String tag) {
    return tag.startsWith("!") ? tag.substring(1) : tag;
  }
}
