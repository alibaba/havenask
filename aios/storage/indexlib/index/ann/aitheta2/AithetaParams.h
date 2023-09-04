/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <limits>
#include <numeric>
#include <string>
namespace indexlibv2::index::ann {

static const std::string DIMENSION = "dimension";

// distance type
static const std::string DISTANCE_TYPE = "distance_type";
static const std::string INNER_PRODUCT = "InnerProduct";
static const std::string SQUARED_EUCLIDEAN = "SquaredEuclidean";
static const std::string MIPS_SQUARED_EUCLIDEAN = "MipsSquaredEuclidean";

// build config
static const std::string INDEX_BUILDER_NAME = "builder_name";
static const std::string LINEAR_BUILDER = "LinearBuilder";
static const std::string QC_BUILDER = "QcBuilder";
static const std::string HNSW_BUILDER = "HnswBuilder";
static const std::string QGRAPH_BUILDER = "QGraphBuilder";

static const std::string INDEX_BUILD_IN_FULL_BUILD_PHASE = "is_alter_index";
static const std::string INDEX_BUILD_THRESHOLD = "linear_build_threshold";
static const std::string IGNORE_FIELD_COUNT_MISMATCH = "ignore_field_count_error";
static const std::string IGNORE_BUILD_ERROR = "ignore_build_error";
static const std::string INDEX_BUILD_PARAMETERS = "build_index_params";
static const std::string STORE_PRIMARY_KEY = "is_pk_saved";
static const std::string STORE_ORIGINAL_EMBEDDING = "is_embedding_saved";
static const std::string INDEX_ORDER_TYPE = "major_order";
static const std::string ORDER_TYPE_ROW = "row";
static const std::string ORDER_TYPE_COL = "col";

static const std::string PRIMARY_KEY_FILE = "aitheta.pk";
static const std::string EMBEDDING_DATA_FILE = "aitheta.embedding_data";

// search config
static const std::string INDEX_SEARCHER_NAME = "searcher_name";
static const std::string LINEAR_SEARCHER = "LinearSearcher";
static const std::string QC_SEARCHER = "QcSearcher";
static const std::string HNSW_SEARCHER = "HnswSearcher";
static const std::string QGRAPH_SEARCHER = "QGraphSearcher";

static const std::string GPU_PREFIX = "Gpu";
static const std::string GPU_QC_SEARCHER = GPU_PREFIX + QC_SEARCHER;

static const std::string INDEX_SCAN_COUNT = "min_scan_doc_cnt";
static const std::string INDEX_SEARCH_PARAMETERS = "search_index_params";

// realtime config
static const std::string INDEX_STREAMER_NAME = "streamer_name";
static const std::string OSWG_STREAMER = "OswgStreamer";
static const std::string HNSW_STREAMER = "HnswStreamer";
static const std::string QGRAPH_STREAMER = "QGraphStreamer";
static const std::string QC_STREAMER = "QcStreamer";

static const std::string REALTIME_ENABLE = "enable_rt_build";
static const std::string REALTIME_PARAMETERS = "rt_index_params";

// recall config
static const std::string RECALL_ENABLE = "enable_recall_report";
static const std::string RECALL_SAMPLE_RATIO = "recall_report_sample_ratio";

// legacy param name, include build & search params & rt params
static const std::string INDEX_PARAMETERS = "index_params";

// storage type
static const std::string TMP_MEM_STORAGE = "TemporaryMemoryStorage";

// feature type
static const std::string FEATURE_TYPE = "feature_type";
static const std::string FEATURE_TYPE_INT8 = "int8";
static const std::string FEATURE_TYPE_FP16 = "fp16";
static const std::string FEATURE_TYPE_FP32 = "fp32";

// query config
static const std::string kTopkKey = "&n=";
static const std::string kScoreThresholdKey = "&sf=";
static const std::string kEmbeddingDelimiter = ",";
static const std::string kIndexIdEmbeddingDelimiter = "#";
static const std::string kIndexIdDelimiter = ",";
static const std::string kQueryDelimiter = ";";
static const std::string EMBEDDING_DELIMITER = "embedding_delimiter";

const int64_t kDefaultIndexId = std::numeric_limits<int64_t>::max();
const int64_t kInvalidIndexId = -1ul;
const uint32_t kMaxIndexCount = 20000;

} // namespace indexlibv2::index::ann
