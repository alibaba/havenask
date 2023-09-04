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

#include "autil/SynchronizedQueue.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/SingleSwiftProcessedDocProducerV2.h"
#include "build_service/workflow/SwiftProcessedDocProducer.h"

namespace indexlib { namespace util {
class MetricProvider;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
}} // namespace indexlib::util

namespace build_service { namespace common {
struct SwiftParam;
}} // namespace build_service::common

namespace build_service { namespace workflow {

// MultiSwiftProcessedDocProducerV2包含了多个SingleSwiftProcessedDocProducerV2，和一个线程池。
// MultiSwiftProcessedDocProducerV2负责创建多个读线程，每个读线程由SingleSwiftProcessedDocProducerV2对swift的一个range进行读取，将读取的doc写入到DocumentQueue的producer端。
// 另外MultiSwiftProcessedDocProducerV2的主线程负责从DocumentQueue的consumer端，按顺序读取doc，作为输出produce()。

class MultiSwiftProcessedDocProducerV2 : public SwiftProcessedDocProducer
{
public:
    MultiSwiftProcessedDocProducerV2(
        std::vector<common::SwiftParam> params, const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
        const proto::PartitionId& partitionId,
        const indexlib::util::TaskSchedulerPtr& taskScheduler = indexlib::util::TaskSchedulerPtr());
    ~MultiSwiftProcessedDocProducerV2();

    MultiSwiftProcessedDocProducerV2(const MultiSwiftProcessedDocProducerV2&) = delete;
    MultiSwiftProcessedDocProducerV2& operator=(const MultiSwiftProcessedDocProducerV2&) = delete;
    MultiSwiftProcessedDocProducerV2(MultiSwiftProcessedDocProducerV2&&) = delete;
    MultiSwiftProcessedDocProducerV2& operator=(MultiSwiftProcessedDocProducerV2&&) = delete;

public:
    bool init(indexlib::util::MetricProviderPtr metricProvider, const std::string& pluginPath,
              const indexlib::util::CounterMapPtr& counterMap, int64_t startTimestamp, int64_t stopTimestamp,
              int64_t noMoreMsgPeriod, int64_t maxCommitInterval, uint64_t sourceSignature,
              bool allowSeekCrossSrc = false) override;

    FlowError produce(document::ProcessedDocumentVecPtr& processedDocVec) override;
    bool seek(const common::Locator& locator) override;
    bool stop(StopOption stopOption) override;
    int64_t suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action) override;

public: // 功能相关接口
    bool needUpdateCommittedCheckpoint() const override;
    bool updateCommittedCheckpoint(const indexlibv2::base::Progress::Offset& checkpoint) override;

    int64_t getStartTimestamp() const override;
    bool getMaxTimestamp(int64_t& timestamp) override;
    bool getLastReadTimestamp(int64_t& timestamp) override;
    uint64_t getLocatorSrc() const override { return getStartTimestamp(); }

public: // metrics 和 测试 相关接口
    void setLinkReporter(const common::SwiftLinkFreshnessReporterPtr& reporter) override;
    void reportFreshnessMetrics(int64_t locatorTimestamp, bool noMoreMsg, const std::string& docSource,
                                bool isReportFastQueueSwiftReadDelay) override;
    void setRecovered(bool isServiceRecovered) override;
    void resumeRead() override;
    autil::ThreadPool* TEST_GetThreadPool() { return _threadPool.get(); }

private:
    void StartWork();

private:
    uint16_t _parallelNum = 0;
    std::atomic<uint16_t> _stopedSingleProducerCount = 0;
    std::vector<SingleSwiftProcessedDocProducerV2*> _singleProducers;
    std::unique_ptr<autil::ThreadPool> _threadPool;
    // SynchronizedQueue have lock,may need to optimize lockless
    std::unique_ptr<autil::SynchronizedQueue<std::pair<size_t, document::ProcessedDocumentPtr>>> _documentQueue;

    std::string _buildIdStr;
    std::vector<std::vector<indexlibv2::base::Progress>> _progress;
    volatile bool _isRunning = false;
    volatile bool _hasFatalError = false;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MultiSwiftProcessedDocProducerV2);

}} // namespace build_service::workflow
