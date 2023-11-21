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

#include <deque>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "build_service/common/Locator.h"
#include "build_service/common/PeriodDocCounter.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Consumer.h"
#include "build_service/workflow/FlowError.h"
#include "build_service/workflow/StopOption.h"
#include "indexlib/misc/common.h"
#include "indexlib/util/counter/CounterBase.h"
#include "swift/client/SwiftWriter.h"

namespace indexlib { namespace util {
class Metric;

typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

BS_DECLARE_REFERENCE_CLASS(common, CounterSynchronizer);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, StateCounter);
DECLARE_REFERENCE_CLASS(util, StringCounter);

namespace build_service { namespace workflow {

struct SwiftWriterWithMetric {
    SwiftWriterWithMetric();
    void updateMetrics();
    bool isSendingDoc() const;
    std::shared_ptr<swift::client::SwiftWriter> swiftWriter;
    std::string zkRootPath;
    std::string topicName;
    indexlib::util::MetricPtr unsendMsgSizeMetric;
    indexlib::util::MetricPtr uncommittedMsgSizeMetric;
    indexlib::util::MetricPtr sendBufferFullQpsMetric;
};

class SwiftProcessedDocConsumer : public ProcessedDocConsumer
{
public:
    SwiftProcessedDocConsumer();
    virtual ~SwiftProcessedDocConsumer();

private:
    SwiftProcessedDocConsumer(const SwiftProcessedDocConsumer&);
    SwiftProcessedDocConsumer& operator=(const SwiftProcessedDocConsumer&);

public:
    bool init(const std::map<std::string, SwiftWriterWithMetric>& writers,
              const common::CounterSynchronizerPtr& counterSynchronizer, int64_t checkpointInterval,
              const common::Locator& locator, int32_t batchMask, bool disableCounter);

public:
    FlowError consume(const document::ProcessedDocumentVecPtr& item) override;
    bool getLocator(common::Locator& locator) const override;
    bool stop(FlowError lastError, StopOption stopOption) override;

private:
    bool isFinished() const;
    void syncCountersWrapper();
    bool syncCounters(int64_t checkpointId, uint64_t src, std::string userData);
    void updateMetric();

    void getMinCheckpoint(int64_t* checkpointId, uint64_t* src, std::string* userData) const;
    bool initCounters(const indexlib::util::CounterMapPtr& counterMap, int64_t startTimestamp);
    indexlib::util::CounterBasePtr getOrCreateCounter(const indexlib::util::CounterMapPtr& counterMap,
                                                      const std::string& counterName,
                                                      indexlib::util::CounterBase::CounterType type,
                                                      int64_t defaultStateCounterValue);
    int64_t updateCheckpointIdToUserData(int64_t checkpointId);
    static std::string getCheckpointUserData(const std::deque<std::pair<int64_t, std::string>>& checkpointIdToUserData,
                                             int64_t checkpointId);

private:
    static const int64_t MAX_SYNC_RETRY = 10;

private:
    std::map<std::string, SwiftWriterWithMetric> _writers;
    common::CounterSynchronizerPtr _counterSynchronizer;

    // Counters used for checkpoint.
    indexlib::util::StateCounterPtr _srcCounter;
    indexlib::util::StateCounterPtr _checkpointCounter;
    indexlib::util::StringCounterPtr _userDataCounter;
    // other counters
    indexlib::util::AccumulativeCounterPtr _consumedDocCountCounter;
    autil::LoopThreadPtr _syncCounterThread;
    autil::LoopThreadPtr _updateMetricThread;
    uint64_t _src;
    std::string _checkpointUserData;
    std::deque<std::pair<int64_t, std::string>> _checkpointIdToUserData;
    mutable autil::ThreadMutex _lock;
    mutable autil::ThreadMutex _consumeLock;
    mutable common::Locator _cachedLocator;
    int64_t _latestInvalidDocCheckpointId;
    bool _hasCounter;
    std::string _traceField;
    common::PeriodDocCounterBase* _docCounter;
    int32_t _batchMask;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftProcessedDocConsumer);

}} // namespace build_service::workflow
