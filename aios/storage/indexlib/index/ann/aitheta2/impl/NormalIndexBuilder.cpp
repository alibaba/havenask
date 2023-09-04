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
#include "indexlib/index/ann/aitheta2/impl/NormalIndexBuilder.h"

#include "indexlib/index/ann/aitheta2/util/AithetaFactoryWrapper.h"
#include "indexlib/index/ann/aitheta2/util/CustomizedAithetaDumper.h"

using namespace std;
using namespace autil;
using namespace aitheta2;

namespace indexlibv2::index::ann {

bool NormalIndexBuilder::BuildAndDump(const EmbeddingDataPtr& embData, IndexDataWriterPtr& indexDataWriter)
{
    InitBuildMetrics();
    index_id_t indexId = embData->get_index_id();
    size_t docCount = embData->count();
    if (docCount <= _indexConfig.buildConfig.buildThreshold) {
        _indexConfig.searchConfig.searcherName = LINEAR_SEARCHER;
        _indexConfig.buildConfig.builderName = LINEAR_BUILDER;
    }

    AUTIL_LOG(INFO, "building index[%lu], doc count[%lu]", indexId, docCount);

    ANN_CHECK(AiThetaFactoryWrapper::CreateBuilder(_indexConfig, docCount, _builder), "create failed");
    {
        ScopedLatencyReporter reporter(_trainLatencyMetric);
        ANN_CHECK_OK(_builder->train(embData), "train failed");
    }
    {
        ScopedLatencyReporter reporter(_buildLatencyMetric);
        ANN_CHECK_OK(_builder->build(embData), "build failed");
    }
    ANN_CHECK_OK(CustomizedAiThetaDumper::dump(_builder, indexDataWriter), "dump failed");

    _indexMeta.docCount = docCount;
    _indexMeta.builderName = _indexConfig.buildConfig.builderName;
    _indexMeta.searcherName = _indexConfig.searchConfig.searcherName;

    AUTIL_LOG(INFO, "build index success");
    return true;
}

void NormalIndexBuilder::InitBuildMetrics()
{
    METRIC_SETUP(_trainLatencyMetric, "indexlib.vector.aitheta2_train_latency", kmonitor::GAUGE);
    METRIC_SETUP(_buildLatencyMetric, "indexlib.vector.aitheta2_build_latency", kmonitor::GAUGE);
}

AUTIL_LOG_SETUP(indexlib.index, NormalIndexBuilder);
} // namespace indexlibv2::index::ann