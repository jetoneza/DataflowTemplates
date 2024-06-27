/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.mongodb.templates;

import com.google.api.services.bigquery.model.TableRow;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

public class BigQueryUtils implements Serializable {
  static final DateTimeFormatter TIMEFORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  public static TableRow getTableSchema(Document document) {
    TableRow row = new TableRow();

    document.forEach(
        (key, value) -> {
          if (value == null) {
            return;
          }

          String valueClass = value.getClass().getName();

          switch (valueClass) {
            case "java.lang.Double":
            case "java.lang.Integer":
            case "java.lang.Long":
            case "java.lang.Boolean":
            case "java.lang.Float":
              row.set(key, value);
              break;
            case "org.bson.Document":
              Document subDoc = Document.parse(value.toString());
              TableRow data = getTableSchema(subDoc);
              row.set(key, data);
              break;
            case "java.util.ArrayList":
            case "java.util.List":
              List<?> valueList = (List<?>) value;
              List<Object> array = new ArrayList<>();
              for (Object item : valueList) {
                if (item instanceof Document) {
                  Document subDocItem = Document.parse(item.toString());
                  TableRow dataItem = getTableSchema(subDocItem);
                  array.add(dataItem);
                } else {
                  array.add(item);
                }
              }
              row.set(key, array);
              break;
            default:
              row.set(key, value.toString());
          }
        });

    LocalDateTime localdate = LocalDateTime.now(ZoneId.of("UTC"));
    row.set("timestamp", localdate.format(TIMEFORMAT));

    return row;
  }
}
