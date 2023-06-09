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
#ifndef ISEARCH_BS_BATCHPROCESSOR_H
#define ISEARCH_BS_BATCHPROCESSOR_H

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "build_service/common_define.h"
#include "build_service/processor/Processor.h"
#include "build_service/processor/BatchRawDocumentDeduper.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class BatchProcessor : public Processor
{
public:
    BatchProcessor(const std::string& strategyParam);
    ~BatchProcessor();

private:
    BatchProcessor(const BatchProcessor&);
    BatchProcessor& operator=(const BatchProcessor&);

public:
    bool start(const config::ProcessorConfigReaderPtr& resourceReaderPtr, const proto::PartitionId& partitionId,
               indexlib::util::MetricProviderPtr metricProvider, const indexlib::util::CounterMapPtr& counterMap,
               const KeyValueMap& kvMap = KeyValueMap(), bool forceSingleThreaded = false,
               bool isTablet = false) override;

    void processDoc(const document::RawDocumentPtr& rawDoc) override;

    void stop(bool instant = false, bool seal = false) override;

private:
    bool parseParam(const std::string& paramStr, uint32_t& batchSize,
                    int64_t& maxEnQueueTime, bool& splitDocBatch,
                    std::string& dedupField, std::string& syncConfigPath) const;

    void checkConsumeTimeout();
    void resetDocQueue();
    void FlushDocQueue();
    void syncConfig();
    
private:
    document::RawDocumentVecPtr _docQueue;
    mutable autil::RecursiveThreadMutex _queueMutex;
    autil::LoopThreadPtr _loopThreadPtr;
    autil::LoopThreadPtr _syncConfigLoop;
    volatile uint32_t _batchSize;
    volatile int64_t _maxEnQueueTime;
    volatile int64_t _inQueueTime;
    volatile int64_t _lastFlushTime;
    volatile int64_t _lastSyncConfigTime;    
    volatile bool _splitDocBatch;
    int64_t _syncConfigInterval;

    std::shared_ptr<BatchRawDocumentDeduper> _docDeduper;
    std::string _dedupField;
    std::string _syncConfigPath;
    indexlib::util::MetricPtr _flushQueueSizeMetric;
    indexlib::util::MetricPtr _flushIntervalMetric;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BatchProcessor);

}} // namespace build_service::processor

#endif // ISEARCH_BS_BATCHPROCESSOR_H
