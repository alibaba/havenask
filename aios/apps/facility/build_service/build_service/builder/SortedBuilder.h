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

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/Lock.h"
#include "autil/Thread.h"
#include "build_service/builder/AsyncBuilder.h"
#include "build_service/builder/Builder.h"
#include "build_service/builder/SortDocumentContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "fslib/fs/FileLock.h"
#include "indexlib/base/Constant.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace util {
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

namespace build_service { namespace builder {

class SortedBuilder : public Builder
{
public:
    SortedBuilder(const indexlib::partition::IndexBuilderPtr& indexBuilder, size_t buildTotalMemMB,
                  fslib::fs::FileLock* fileLock = NULL, const proto::BuildId& buildId = proto::BuildId());
    ~SortedBuilder();

private:
    SortedBuilder(const SortedBuilder&);
    SortedBuilder& operator=(const SortedBuilder&);

public:
    bool init(const config::BuilderConfig& builderConfig,
              indexlib::util::MetricProviderPtr metricProvider = indexlib::util::MetricProviderPtr()) override;
    bool build(const indexlib::document::DocumentPtr& doc) override;
    void stop(int64_t stopTimestamp = INVALID_TIMESTAMP) override;
    bool hasFatalError() const override;
    bool tryDump() override;

    static bool reserveMemoryQuota(const config::BuilderConfig& builderConfig, size_t buildTotalMemMB,
                                   const indexlib::util::QuotaControlPtr& quotaControl);

    static int64_t calculateSortQueueMemory(const config::BuilderConfig& builderConfig, size_t buildTotalMemMB);

private:
    void reportMetrics();

private:
    void initConfig(const config::BuilderConfig& builderConfig);
    bool initSortContainers(const config::BuilderConfig& builderConfig,
                            const indexlib::config::IndexPartitionSchemaPtr& schema);
    bool initOneContainer(const config::BuilderConfig& builderConfig,
                          const indexlib::config::IndexPartitionSchemaPtr& schema, SortDocumentContainer& container);

    void flush();
    void flushUnsafe();
    void buildAll();
    void dumpAll();
    void waitAllDocBuilt();
    void buildThread();
    bool initBuilder(const config::BuilderConfig& builderConfig,
                     indexlib::util::MetricProviderPtr metricProvider = indexlib::util::MetricProviderPtr());
    bool buildOneDoc(const indexlib::document::DocumentPtr& document);
    bool needFlush(size_t sortQueueMemUse, size_t docCount);

private:
    enum BuildThreadStatus {
        WaitingSortDone = 0,
        Building = 1,
        Dumping = 2,
    };
    enum SortThreadStatus {
        WaitingBuildDone = 0,
        Pushing = 1,
        Sorting = 2,
    };

private:
    volatile bool _running;
    size_t _buildTotalMem;
    int64_t _sortQueueMem;
    uint32_t _sortQueueSize;
    mutable autil::ThreadMutex _collectContainerLock;
    mutable autil::ProducerConsumerCond _swapCond;
    SortDocumentContainer _collectContainer;
    SortDocumentContainer _buildContainer;
    AsyncBuilderPtr _asyncBuilder;
    autil::ThreadPtr _batchBuildThreadPtr;
    indexlib::util::MetricPtr _sortQueueSizeMetric;
    indexlib::util::MetricPtr _buildQueueSizeMetric;
    indexlib::util::MetricPtr _buildThreadStatusMetric;
    indexlib::util::MetricPtr _sortThreadStatusMetric;
    indexlib::util::MetricPtr _sortQueueMemUseMetric;

private:
    friend class SortedBuilderTest;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SortedBuilder);

inline bool SortedBuilder::buildOneDoc(const indexlib::document::DocumentPtr& document)
{
    if (_asyncBuilder) {
        if (!_asyncBuilder->build(document)) {
            setFatalError();
            return false;
        }
        return true;
    }
    // TODO: legacy for async build shut down
    if (!Builder::build(document)) {
        std::string errorMsg = "build failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        setFatalError();
        return false;
    }
    return true;
}

}} // namespace build_service::builder
