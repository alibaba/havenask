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
#include "indexlib/index/ann/aitheta2/util/AithetaFactoryWrapper.h"

#include "indexlib/index/ann/aitheta2/util/CustomizedAithetaContainer.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializerFactory.h"

using namespace std;
using namespace autil;
using namespace aitheta2;
namespace indexlibv2::index::ann {

bool AiThetaFactoryWrapper::CreateBuilder(const AithetaIndexConfig& config, size_t docCount, AiThetaBuilderPtr& builder)
{
    string builderName = config.buildConfig.builderName;
    auto intializer = ParamsInitializerFactory::Create(builderName, docCount);
    ANN_CHECK(intializer, "create parameter initializer failed");

    AiThetaMeta meta;
    ANN_CHECK(intializer->InitAiThetaMeta(config, meta), "init failed");
    // 对于图算法，proxima没有支持mips转换, 因此使用球面距离
    if ((builderName == HNSW_BUILDER || builderName == QGRAPH_BUILDER) && meta.measure_name() == INNER_PRODUCT) {
        meta.set_measure(MIPS_SQUARED_EUCLIDEAN, 0, AiThetaParams());
        AUTIL_LOG(INFO, "update distance type from %s to %s", INNER_PRODUCT.c_str(), MIPS_SQUARED_EUCLIDEAN.c_str());
    }
    AiThetaParams params;
    ANN_CHECK(intializer->InitBuildParams(config, params), "init failed");
    builder = AiThetaFactory::CreateBuilder(builderName);
    ANN_CHECK(builder != nullptr, "create failed");
    ANN_CHECK_OK(builder->init(meta, params), "init failed");
    AUTIL_LOG(INFO, "create index builder[%s] success", builderName.c_str());
    return true;
}

bool AiThetaFactoryWrapper::CreateSearcher(const AithetaIndexConfig& config, const IndexMeta& indexMeta,
                                           const IndexDataReaderPtr& reader, AiThetaSearcherPtr& searcher)
{
    auto& searcherName = indexMeta.searcherName;
    size_t docCount = indexMeta.docCount;
    auto initializer = ParamsInitializerFactory::Create(searcherName, docCount);
    ANN_CHECK(initializer != nullptr, "create parameter initializer failed");

    AiThetaParams params;
    AithetaIndexConfig newConfig = config;
    if (indexMeta.builderName == OSWG_STREAMER || indexMeta.builderName == QC_STREAMER) {
        newConfig.searchConfig.indexParams = config.realtimeConfig.indexParams;
    }
    ANN_CHECK(initializer->InitSearchParams(newConfig, params), "init failed");

    auto container = std::make_shared<CustomizedAiThetaContainer>(reader);
    ANN_CHECK_OK(container->init(params), "container init failed");
    ANN_CHECK_OK(container->load(), "container load failed");

    searcher = AiThetaFactory::CreateSearcher(searcherName);
    ANN_CHECK(searcher != nullptr, "create searcher[%s] failed", searcherName.c_str());
    ANN_CHECK_OK(searcher->init(params), "searcher init failed");
    ANN_CHECK_OK(searcher->load(container, aitheta2::IndexMeasure::Pointer()), "searcher load failed");

    return true;
}

bool AiThetaFactoryWrapper::CreateStreamer(const AithetaIndexConfig& config,
                                           const std::shared_ptr<RealtimeIndexBuildResource>& resource,
                                           AiThetaStreamerPtr& streamer)
{
    string streamerName = config.realtimeConfig.streamerName;
    bool isColdStart = true;
    if (resource != nullptr) {
        std::string builderName = resource->normalIndexMeta.builderName;
        if (streamerName == QGRAPH_STREAMER && builderName == QGRAPH_BUILDER) {
            isColdStart = false;
        } else if (streamerName == QC_STREAMER && builderName == QC_BUILDER) {
            isColdStart = false;
        }
    }
    // HNSW_STREAMER也支持冷启动
    if (isColdStart && (streamerName != HNSW_STREAMER)) {
        AUTIL_LOG(INFO, "update streamer from %s to %s", streamerName.c_str(), OSWG_STREAMER.c_str());
        streamerName = OSWG_STREAMER;
    }

    auto initializer = ParamsInitializerFactory::Create(streamerName, 0);
    ANN_CHECK(initializer, "create parameter initializer failed");
    AiThetaParams params;
    ANN_CHECK(initializer->InitRealtimeParams(config, params), "init failed");

    streamer = AiThetaFactory::CreateStreamer(streamerName);
    ANN_CHECK(streamer != nullptr, "create streamer %s failed", streamerName.c_str());

    if (isColdStart) {
        AiThetaMeta meta;
        ANN_CHECK(initializer->InitAiThetaMeta(config, meta), "init failed");
        // 对于冷启动，无法使用mips转换(没有全局norm), 因此使用球面距离
        if (meta.measure_name() == INNER_PRODUCT) {
            meta.set_measure(MIPS_SQUARED_EUCLIDEAN, 0, AiThetaParams());
            AUTIL_LOG(INFO, "update distance type from %s to %s", INNER_PRODUCT.c_str(),
                      MIPS_SQUARED_EUCLIDEAN.c_str());
        }
        ANN_CHECK_OK(streamer->init(meta, params), "init failed");
    } else {
        auto reader = resource->normalIndexDataReader;
        auto container = std::make_shared<CustomizedAiThetaContainer>(reader);
        ANN_CHECK_OK(container->init(params), "container init failed");
        ANN_CHECK_OK(container->load(), "container load failed");
        ANN_CHECK_OK(streamer->init(container, params), "streamer init failed");
    }

    AiThetaStoragePtr storage;
    ANN_CHECK(CreateStorage(TMP_MEM_STORAGE, params, storage), "create storage failed");
    ANN_CHECK_OK(streamer->open(storage), "streamer open storage failed");
    return true;
}

bool AiThetaFactoryWrapper::CreateStorage(const std::string& name, const AiThetaParams& params,
                                          AiThetaStoragePtr& storage)
{
    storage = AiThetaFactory::CreateStorage(name);
    ANN_CHECK(storage != nullptr, "create[%s] storage failed", name.c_str());
    ANN_CHECK_OK(storage->init(params), "init failed");
    do {
        string path = StringUtil::toString(TimeUtility::currentTime());
        auto instance = aitheta2::IndexMemory::Instance();
        ANN_CHECK(instance != nullptr, "get index memory instance failed");
        if (instance->has(path)) {
            continue;
        }
        ANN_CHECK_OK(storage->open(path, true), "open storage failed");
        break;
    } while (true);

    return true;
}

AUTIL_LOG_SETUP(indexlib.index, AiThetaFactoryWrapper);
} // namespace indexlibv2::index::ann
