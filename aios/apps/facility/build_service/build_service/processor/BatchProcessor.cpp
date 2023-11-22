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
#include "build_service/processor/BatchProcessor.h"

#include <assert.h>
#include <cstddef>
#include <functional>
#include <limits>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/common/Locator.h"
#include "build_service/config/ProcessorInfo.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/processor/ProcessorWorkItem.h"
#include "build_service/processor/ProcessorWorkItemExecutor.h"
#include "build_service/util/Monitor.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/misc/log.h"
#include "kmonitor/client/MetricType.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::document;
using namespace build_service::common;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, BatchProcessor);

// batch_size=64;max_enqueue_seconds=1
BatchProcessor::BatchProcessor(const string& strategyParam)
    : Processor(strategyParam)
    , _batchSize(100)
    , _maxEnQueueTime(1 * 1000 * 1000) // 1s
    , _inQueueTime(std::numeric_limits<int64_t>::max())
    , _lastFlushTime(0)
    , _lastSyncConfigTime(0)
    , _splitDocBatch(false)
    , _syncConfigInterval(autil::EnvUtil::getEnv("batch_process_sync_config_interval", 20 * 1000 * 1000))
{
}

BatchProcessor::~BatchProcessor() { stop(/*instant*/ false, /*seal*/ false); }

bool BatchProcessor::start(const config::ProcessorConfigReaderPtr& resourceReaderPtr,
                           const proto::PartitionId& partitionId, indexlib::util::MetricProviderPtr metricProvider,
                           const indexlib::util::CounterMapPtr& counterMap, const KeyValueMap& kvMap,
                           bool forceSingleThreaded, bool isTablet)
{
    if (!Processor::start(resourceReaderPtr, partitionId, metricProvider, counterMap, kvMap, forceSingleThreaded,
                          isTablet)) {
        return false;
    }

    _flushQueueSizeMetric = DECLARE_METRIC(_metricProvider, "perf/flushQueueSize", kmonitor::GAUGE, "count");
    _flushIntervalMetric = DECLARE_METRIC(_metricProvider, "perf/flushInterval", kmonitor::GAUGE, "count");

    uint32_t batchSize = 1;
    int64_t maxEnQueueTime = 1 * 1000 * 1000;
    bool splitDocBatch = false;
    if (!parseParam(_strategyParam, batchSize, maxEnQueueTime, splitDocBatch, _dedupField, _syncConfigPath)) {
        return false;
    }

    if (!_dedupField.empty()) {
        _docDeduper.reset(new BatchRawDocumentDeduper(_dedupField, metricProvider));
    }
    _batchSize = batchSize;
    _maxEnQueueTime = maxEnQueueTime;
    _splitDocBatch = splitDocBatch;
    _loopThreadPtr =
        LoopThread::createLoopThread(bind(&BatchProcessor::checkConsumeTimeout, this), 100 * 1000); // 100 ms
    if (!_loopThreadPtr) {
        BS_LOG(ERROR, "create check batch queue consume timeout thread failed.");
        return false;
    }
    if (!_syncConfigPath.empty()) {
        _syncConfigLoop = LoopThread::createLoopThread(bind(&BatchProcessor::syncConfig, this), 1000 * 1000,
                                                       "batchProcessSync"); // 1 s
        if (!_syncConfigLoop) {
            BS_LOG(ERROR, "create sync config info thread failed.");
            return false;
        }
    }
    _lastFlushTime = TimeUtility::currentTime();
    return true;
}

bool BatchProcessor::parseParam(const string& paramStr, uint32_t& batchSize, int64_t& maxEnQueueTime,
                                bool& splitDocBatch, string& dedupField, string& syncConfigPath) const
{
    std::vector<std::string> kvStrVec;
    autil::StringUtil::fromString(paramStr, kvStrVec, ";");
    for (auto kvStr : kvStrVec) {
        std::string key;
        std::string value;
        autil::StringUtil::getKVValue(kvStr, key, value, "=", true);
        if (key.empty() || value.empty()) {
            IE_LOG(ERROR, "invalid kv parameter [%s]", kvStr.c_str());
            return false;
        }
        if (key == "batch_size") {
            if (!StringUtil::fromString(value, batchSize)) {
                IE_LOG(ERROR, "bad batch_size: value [%s]", value.c_str());
                return false;
            }
            if (batchSize < 1) {
                IE_LOG(INFO, "batch_size should not be [%u], will set to 100 by default", batchSize);
                batchSize = 100;
            }
            continue;
        }
        if (key == "max_enqueue_seconds") {
            int64_t maxEnQueueSeconds = 0;
            if (!StringUtil::fromString(value, maxEnQueueSeconds) || maxEnQueueSeconds <= 0) {
                IE_LOG(ERROR, "bad max_enqueue_seconds: value [%s]", value.c_str());
                return false;
            }
            maxEnQueueTime = maxEnQueueSeconds * 1000 * 1000;
            continue;
        }
        if (key == "split_doc_batch") {
            splitDocBatch = (value == "true");
            continue;
        }
        if (key == "deduplicate_doc_by_field") {
            dedupField = value;
            IE_LOG(INFO, "enable deduplicate doc by field [%s]", dedupField.c_str());
            continue;
        }
        if (key == "sync_config_path") {
            syncConfigPath = value;
            IE_LOG(INFO, "sync config path is [%s]", syncConfigPath.c_str());
            continue;
        }
        IE_LOG(ERROR, "bad strategy param key [%s]", key.c_str());
        return false;
    }
    return true;
}

void BatchProcessor::processDoc(const document::RawDocumentPtr& rawDoc)
{
    if (unlikely(isSealed())) {
        return;
    }
    if (!rawDoc) {
        return;
    }
    ScopedLock lock(_queueMutex);
    if (!_docQueue) {
        resetDocQueue();
    }
    _docQueue->push_back(rawDoc);
    if (_docQueue->size() >= (size_t)_batchSize) {
        FlushDocQueue();
    }
}

void BatchProcessor::checkConsumeTimeout()
{
    if (_inQueueTime == std::numeric_limits<int64_t>::max()) {
        // empty queue
        return;
    }

    int64_t deadLine = TimeUtility::currentTime() - _maxEnQueueTime;
    if (_inQueueTime > deadLine) {
        return;
    }

    ScopedLock lock(_queueMutex);
    BS_LOG(DEBUG, "flush doc queue : %ld, %ld", _inQueueTime, deadLine);
    if (_inQueueTime <= deadLine) {
        FlushDocQueue();
    }
}

void BatchProcessor::stop(bool instant, bool seal)
{
    _loopThreadPtr.reset();
    _syncConfigLoop.reset();
    {
        ScopedLock lock(_queueMutex);
        if (_docQueue) {
            FlushDocQueue();
        }
    }
    Processor::stop(instant, seal);
}

void BatchProcessor::resetDocQueue()
{
    _docQueue.reset(new RawDocumentVec);
    _docQueue->reserve(_batchSize);
    _inQueueTime = TimeUtility::currentTime();
}

void BatchProcessor::FlushDocQueue()
{
    if (_docDeduper) {
        _docDeduper->process(_docQueue);
    }
    if (_docQueue) {
        assert(!_docQueue->empty());
        REPORT_METRIC(_flushQueueSizeMetric, _docQueue->size());
        if (_splitDocBatch || _docQueue->size() == 1) {
            for (size_t i = 0; i < _docQueue->size(); i++) {
                ProcessorWorkItem* workItem =
                    new ProcessorWorkItem(getProcessorChains(), _chainSelector, false, &_reporter);
                workItem->setProcessErrorCounter(_processErrorCounter);
                workItem->setProcessDocCountCounter(_processDocCountCounter);
                workItem->initProcessData((*_docQueue)[i]);
                if (!_executor->push(workItem)) {
                    BS_LOG(WARN, "push work item failed, drop it");
                    delete workItem;
                }
            }
        } else {
            ProcessorWorkItem* workItem =
                new ProcessorWorkItem(getProcessorChains(), _chainSelector, false, &_reporter);
            workItem->setProcessErrorCounter(_processErrorCounter);
            workItem->setProcessDocCountCounter(_processDocCountCounter);
            workItem->initBatchProcessData(_docQueue);
            if (!_executor->push(workItem)) {
                BS_LOG(WARN, "push work item failed, drop it");
                delete workItem;
            }
        }
    }
    int64_t newFlushTime = TimeUtility::currentTime();
    REPORT_METRIC(_flushIntervalMetric, newFlushTime - _lastFlushTime);
    _lastFlushTime = newFlushTime;

    _docQueue.reset();
    _inQueueTime = std::numeric_limits<int64_t>::max();
}

void BatchProcessor::syncConfig()
{
    if (_syncConfigPath.empty()) {
        return;
    }
    auto currentTs = TimeUtility::currentTime();
    if (currentTs - _lastSyncConfigTime < _syncConfigInterval) {
        return;
    }
    _lastSyncConfigTime = currentTs;
    string content;
    auto ec = fslib::fs::FileSystem::readFile(_syncConfigPath, content);
    if (ec == fslib::EC_OK) {
        uint32_t batchSize = _batchSize;
        int64_t maxEnQueueTime = _maxEnQueueTime;
        bool splitDocBatch = _splitDocBatch;
        std::string dedupField = _dedupField;
        std::string syncConfigPath = _syncConfigPath;
        if (!parseParam(content, batchSize, maxEnQueueTime, splitDocBatch, dedupField, syncConfigPath)) {
            BS_LOG(ERROR, "invalid content [%s] in config path [%s]", content.c_str(), _syncConfigPath.c_str());
            return;
        }
        if (dedupField != _dedupField || syncConfigPath != _syncConfigPath) {
            BS_LOG(ERROR,
                   "invalid content [%s], should not update"
                   " [deduplicate_doc_by_field] or [sync_config_path] ",
                   content.c_str());
            return;
        }
        if (batchSize != _batchSize) {
            BS_LOG(INFO, "update batch_size to [%u]", batchSize);
            _batchSize = batchSize;
        }
        if (maxEnQueueTime != _maxEnQueueTime) {
            BS_LOG(INFO, "update max_enqueue_seconds to [%ld]", maxEnQueueTime);
            _maxEnQueueTime = maxEnQueueTime;
        }
        if (splitDocBatch != _splitDocBatch) {
            BS_LOG(INFO, "update split_doc_batch to [%s]", splitDocBatch ? "true" : "false");
            _splitDocBatch = splitDocBatch;
        }
    } else if (ec == fslib::EC_NOENT) {
        BS_LOG(INFO, "config [%s] not exist, batch processor use default setting", _syncConfigPath.c_str());
        return;
    } else {
        BS_LOG(ERROR, "read config [%s] failed, error [%d], batch processor use default setting",
               _syncConfigPath.c_str(), ec);
        return;
    }
}

}} // namespace build_service::processor
