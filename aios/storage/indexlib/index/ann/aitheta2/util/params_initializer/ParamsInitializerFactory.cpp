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
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializerFactory.h"

#include "indexlib/index/ann/aitheta2/util/params_initializer/HnswParamsInitializer.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/QcParamsInitializer.h"

namespace indexlibv2::index::ann {

ParamsInitializerPtr ParamsInitializerFactory::Create(const std::string& name, size_t docCount)
{
    ParamsInitializerPtr initializer;
    if (name == LINEAR_SEARCHER || name == LINEAR_BUILDER) {
        initializer.reset(new LrParamsInitializer());
    } else if (name == QC_SEARCHER || name == QC_BUILDER || name == QC_STREAMER) {
        initializer.reset(new QcParamsInitializer(docCount));
    } else if (name == GPU_QC_SEARCHER) {
        initializer.reset(new GpuQcParamsInitializer(docCount));
    } else if (name == HNSW_SEARCHER || name == HNSW_BUILDER || name == HNSW_STREAMER || name == OSWG_STREAMER) {
        initializer.reset(new HnswParamsInitializer());
    } else if (name == QGRAPH_SEARCHER || name == QGRAPH_BUILDER || name == QGRAPH_STREAMER) {
        initializer.reset(new QGraphParamsInitializer());
    } else {
        AUTIL_LOG(ERROR, "unknown searcher/builder/streamer name[%s]", name.c_str());
    }

    return initializer;
}

AUTIL_LOG_SETUP(indexlib.index, ParamsInitializerFactory);
} // namespace indexlibv2::index::ann
