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

bool NormalIndexBuilder::Init(size_t docCount)
{
    InitBuildMetrics();
    if (!_indexConfig.buildConfig.distributedBuild && docCount <= _indexConfig.buildConfig.buildThreshold) {
        _indexConfig.searchConfig.searcherName = LINEAR_SEARCHER;
        _indexConfig.buildConfig.builderName = LINEAR_BUILDER;
    }
    ANN_CHECK(AiThetaFactoryWrapper::CreateBuilder(_indexConfig, docCount, _builder), "create failed");
    return true;
}

bool NormalIndexBuilder::Train(std::shared_ptr<EmbeddingBufferBase>& embBuffer,
                               std::shared_ptr<aitheta2::CustomizedCkptManager>& indexCkptManager)
{
    assert(_builder);
    size_t docCount = embBuffer->count();
    AUTIL_LOG(INFO, "train doc count[%lu]", docCount);
    ScopedLatencyReporter reporter(_trainLatencyMetric);
    ANN_CHECK_OK(_builder->train(nullptr, embBuffer, indexCkptManager), "train failed");
    return true;
}

bool NormalIndexBuilder::BuildAndDump(std::shared_ptr<EmbeddingBufferBase>& embBuffer,
                                      std::shared_ptr<aitheta2::CustomizedCkptManager>& indexCkptManager,
                                      IndexDataWriterPtr& indexDataWriter)
{
    assert(_builder);
    size_t docCount = embBuffer->count();
    AUTIL_LOG(INFO, "building doc count[%lu]", docCount);
    {
        ScopedLatencyReporter reporter(_buildLatencyMetric);
        embBuffer->SetMultiPass(false);
        ANN_CHECK_OK(_builder->build(nullptr, embBuffer, indexCkptManager), "build failed");
    }
    ANN_CHECK_OK(CustomizedAiThetaDumper::dump(_builder, indexDataWriter), "dump failed");

    _indexMeta.trainStats.stats = _builder->train_stats().to_json();
    _indexMeta.buildStats.stats = _builder->build_stats().to_json();
    _indexMeta.docCount = docCount;
    _indexMeta.builderName = _indexConfig.buildConfig.builderName;
    _indexMeta.searcherName = _indexConfig.searchConfig.searcherName;

    AUTIL_LOG(INFO, "build index done");
    return true;
}

void NormalIndexBuilder::InitBuildMetrics()
{
    METRIC_SETUP(_trainLatencyMetric, "indexlib.vector.offline.train_latency", kmonitor::GAUGE);
    METRIC_SETUP(_buildLatencyMetric, "indexlib.vector.offline.build_latency", kmonitor::GAUGE);
}

AUTIL_LOG_SETUP(indexlib.index, NormalIndexBuilder);
} // namespace indexlibv2::index::ann
