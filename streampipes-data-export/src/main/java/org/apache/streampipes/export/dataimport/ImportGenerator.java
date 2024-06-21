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

package org.apache.streampipes.export.dataimport;

import org.apache.streampipes.commons.zip.ZipFileExtractor;
import org.apache.streampipes.export.constants.ExportConstants;
import org.apache.streampipes.export.utils.SerializationUtils;
import org.apache.streampipes.model.export.StreamPipesApplicationPackage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.lightcouch.DocumentConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class ImportGenerator<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ImportGenerator.class);

  protected ObjectMapper spMapper;
  protected ObjectMapper defaultMapper;

  public ImportGenerator() {
    this.spMapper = SerializationUtils.getSpObjectMapper();
    this.defaultMapper = SerializationUtils.getDefaultObjectMapper();
  }

  public T generate(InputStream inputStream) throws IOException {
    Map<String, byte[]> previewFiles = new ZipFileExtractor(inputStream).extractZipToMap();
    var manifest = getManifest(previewFiles);

    handleAssets(manifest, previewFiles);
    handleAdapters(manifest, previewFiles);
    handleDashboards(manifest, previewFiles);
    handleDataViews(manifest, previewFiles);
    handleDataSources(manifest, previewFiles);
    handlePipelines(manifest, previewFiles);
    handleDataLakeMeasures(manifest, previewFiles);
    handleDashboardWidgets(manifest, previewFiles);
    handleDataViewWidgets(manifest, previewFiles);
    handleFiles(manifest, previewFiles);

    afterResourcesCreated();
    return getReturnObject();
  }

  private void handleAssets(StreamPipesApplicationPackage manifest, Map<String, byte[]> previewFiles) {
    for (String assetId : manifest.getAssets()) {
      handleResource(previewFiles, assetId, "asset", this::handleAsset);
    }
  }

  private void handleAdapters(StreamPipesApplicationPackage manifest, Map<String, byte[]> previewFiles) {
    for (String adapterId : manifest.getAdapters()) {
      handleResource(previewFiles, adapterId, "adapter", this::handleAdapter);
    }
  }

  private void handleDashboards(StreamPipesApplicationPackage manifest, Map<String, byte[]> previewFiles) {
    for (String dashboardId : manifest.getDashboards()) {
      handleResource(previewFiles, dashboardId, "dashboard", this::handleDashboard);
    }
  }

  private void handleDataViews(StreamPipesApplicationPackage manifest, Map<String, byte[]> previewFiles) {
    for (String dataViewId : manifest.getDataViews()) {
      handleResource(previewFiles, dataViewId, "data view", this::handleDataView);
    }
  }

  private void handleDataSources(StreamPipesApplicationPackage manifest, Map<String, byte[]> previewFiles) {
    for (String dataSourceId : manifest.getDataSources()) {
      handleResource(previewFiles, dataSourceId, "data source", this::handleDataSource);
    }
  }

  private void handlePipelines(StreamPipesApplicationPackage manifest, Map<String, byte[]> previewFiles) {
    for (String pipelineId : manifest.getPipelines()) {
      handleResource(previewFiles, pipelineId, "pipeline", this::handlePipeline);
    }
  }

  private void handleDataLakeMeasures(StreamPipesApplicationPackage manifest, Map<String, byte[]> previewFiles) {
    for (String measurementId : manifest.getDataLakeMeasures()) {
      handleResource(previewFiles, measurementId, "data lake measure", this::handleDataLakeMeasure);
    }
  }

  private void handleDashboardWidgets(StreamPipesApplicationPackage manifest, Map<String, byte[]> previewFiles) {
    for (String dashboardWidgetId : manifest.getDashboardWidgets()) {
      handleResource(previewFiles, dashboardWidgetId, "dashboard widget", this::handleDashboardWidget);
    }
  }

  private void handleDataViewWidgets(StreamPipesApplicationPackage manifest, Map<String, byte[]> previewFiles) {
    for (String dataViewWidgetId : manifest.getDataViewWidgets()) {
      handleResource(previewFiles, dataViewWidgetId, "data view widget", this::handleDataViewWidget);
    }
  }

  private void handleFiles(StreamPipesApplicationPackage manifest, Map<String, byte[]> previewFiles) {
    for (String fileMetadataId : manifest.getFiles()) {
      try {
        handleFile(asString(previewFiles.get(fileMetadataId)), fileMetadataId, previewFiles);
      } catch (DocumentConflictException e) {
        LOG.warn("Skipping import of file {} (already present with the same id)", fileMetadataId);
      }
    }
  }

  private void handleResource(Map<String, byte[]> previewFiles, String resourceId, String resourceType,
                              ResourceHandler handler) {
    try {
      handler.handle(asString(previewFiles.get(resourceId)), resourceId);
    } catch (DocumentConflictException e) {
      LOG.warn("Skipping import of {} {} (already present with the same id)", resourceType, resourceId);
    } catch (IOException e) {
      LOG.error("Error importing {} {}", resourceType, resourceId, e);
    }
  }

  @FunctionalInterface
  private interface ResourceHandler {
    void handle(String document, String resourceId) throws IOException;
  }

//Refactoring end