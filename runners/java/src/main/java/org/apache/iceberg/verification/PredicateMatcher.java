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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;

/**
 * Evaluates an op-log predicate against a decoded {@link Record}, so the runner can find the
 * positions a merge-on-read delete targets. Covers the l-log predicate grammar the corpus uses
 * (comparisons, is-null/not-null, in/not-in, and/or/not, true/false).
 */
@FunctionalInterface
interface PredicateMatcher {
  boolean matches(Record record);

  @SuppressWarnings("unchecked")
  static PredicateMatcher of(Map<String, Object> pred, Schema schema) {
    String type = (String) pred.get("type");
    switch (type) {
      case "true":
        return r -> true;
      case "false":
        return r -> false;
      case "is-null": {
        String term = (String) pred.get("term");
        return r -> r.getField(term) == null;
      }
      case "not-null": {
        String term = (String) pred.get("term");
        return r -> r.getField(term) != null;
      }
      case "eq":
      case "ne":
      case "lt":
      case "lt-eq":
      case "gt":
      case "gt-eq": {
        String term = (String) pred.get("term");
        Type fieldType = schema.findType(term);
        Comparable<Object> want = comparable(term(pred.get("value"), fieldType));
        return r -> {
          Object have = r.getField(term);
          if (have == null) {
            return false;
          }
          int cmp = want.compareTo(have);
          // want.compareTo(have): negative if want < have. Translate per op on (have OP want).
          return switch (type) {
            case "eq" -> cmp == 0;
            case "ne" -> cmp != 0;
            case "lt" -> cmp > 0; // have < want
            case "lt-eq" -> cmp >= 0;
            case "gt" -> cmp < 0; // have > want
            case "gt-eq" -> cmp <= 0;
            default -> false;
          };
        };
      }
      case "in":
      case "not-in": {
        String term = (String) pred.get("term");
        Type fieldType = schema.findType(term);
        List<Object> raw = (List<Object>) pred.get("values");
        List<Object> wants = raw.stream().map(v -> term(v, fieldType)).toList();
        boolean negate = type.equals("not-in");
        return r -> {
          Object have = r.getField(term);
          boolean contained = wants.stream().anyMatch(w -> w.equals(have));
          return negate != contained;
        };
      }
      case "and": {
        List<Object> args = (List<Object>) pred.get("args");
        List<PredicateMatcher> subs = args.stream().map(a -> of((Map<String, Object>) a, schema)).toList();
        return r -> subs.stream().allMatch(m -> m.matches(r));
      }
      case "or": {
        List<Object> args = (List<Object>) pred.get("args");
        List<PredicateMatcher> subs = args.stream().map(a -> of((Map<String, Object>) a, schema)).toList();
        return r -> subs.stream().anyMatch(m -> m.matches(r));
      }
      case "not": {
        PredicateMatcher sub = of((Map<String, Object>) pred.get("arg"), schema);
        return r -> !sub.matches(r);
      }
      default:
        throw new IllegalArgumentException("unsupported predicate type: " + type);
    }
  }

  /** Resolve a predicate value (a TypedValue) into the Java value the Record holds. */
  private static Object term(Object value, Type fieldType) {
    if (value instanceof TypedValue tv) {
      return tv.toJavaValue(fieldType);
    }
    throw new IllegalArgumentException("predicate value must carry a physical-type tag");
  }

  @SuppressWarnings("unchecked")
  private static Comparable<Object> comparable(Object v) {
    if (v instanceof Comparable) {
      return (Comparable<Object>) v;
    }
    throw new IllegalArgumentException("predicate value is not comparable: " + v);
  }
}
