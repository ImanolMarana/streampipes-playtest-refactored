/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.streampipes.storage.couchdb.serializer;

import org.apache.streampipes.model.DataProcessorType;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

public class EpaTypeAdapter extends PeTypeAdapter<DataProcessorType> {

  @Override
  public void write(JsonWriter out, DataProcessorType value) throws IOException {
    write(out, value.getLabel(), value.getDescription(), value.name());
  }

  @Override
  public DataProcessorType read(JsonReader in) throws IOException {
    DataProcessorType dataProcessorType = null;
    in.beginObject();
    while (in.hasNext()) {
      String name = in.nextName();
      if (name.equals("type")) {
        dataProcessorType = DataProcessorType.valueOf(in.nextString());
      }
    }
    in.endObject();
    if (dataProcessorType != null) {
      return dataProcessorType;
    } else {
      throw new IOException();
    }
  }

}
