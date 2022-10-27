#include "build_service/processor/BatchProcessor.h"
#include "build_service/util/Monitor.h"
#include <autil/TimeUtility.h>
#include <indexlib/util/counter/accumulative_counter.h>

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::document;
using namespace build_service::common;

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, BatchProcessor);

// batch_size=64;max_enqueue_seconds=1
BatchProcessor::BatchProcessor(const string& strategyParam)
    : Processor(strategyParam)
    , _batchSize(100)
    , _maxEnQueueTime(1 * 1000 * 1000) // 1s
    , _inQueueTime(std::numeric_limits<int64_t>::max())
    , _lastFlushTime(0)
{
}

BatchProcessor::~BatchProcessor() {
    stop();
}

bool BatchProcessor::start(const config::ResourceReaderPtr &resourceReaderPtr,
                           const proto::PartitionId &partitionId,
                           IE_NAMESPACE(misc)::MetricProviderPtr metricProvider,
                           const IE_NAMESPACE(util)::CounterMapPtr& counterMap)
{
    if (!Processor::start(resourceReaderPtr, partitionId, metricProvider, counterMap)) {
        return false;
    }

    _flushQueueSizeMetric = DECLARE_METRIC(_metricProvider, perf/flushQueueSize, kmonitor::GAUGE, "count");
    _flushIntervalMetric = DECLARE_METRIC(_metricProvider, perf/flushInterval, kmonitor::GAUGE, "count");
    
    if (!parseParam(_strategyParam, _batchSize, _maxEnQueueTime)) {
        return false;
    }
    _loopThreadPtr = LoopThread::createLoopThread(
            tr1::bind(&BatchProcessor::checkConsumeTimeout, this), 100 * 1000); // 100 ms
    if (!_loopThreadPtr) {
        BS_LOG(ERROR, "create check batch queue consume timeout thread failed.");
        return false;
    }

    _lastFlushTime = TimeUtility::currentTime();
    return true;
}

bool BatchProcessor::parseParam(const string& paramStr,
                                uint32_t &batchSize,
                                int64_t &maxEnQueueTime) const
{
    vector<vector<string>> paramVec;
    StringUtil::fromString(paramStr, paramVec, "=", ";");
    for (auto& iPair : paramVec) {
        if (iPair.size() != 2) {
            IE_LOG(ERROR, "bad strategy param [%s]", paramStr.c_str());
            return false;
        }
        if (iPair[0] == "batch_size") {
            if (!StringUtil::fromString(iPair[1], batchSize)) {
                IE_LOG(ERROR, "bad batch_size: value [%s]", iPair[1].c_str());
                return false;
            }
            if (batchSize <= 1) {
                IE_LOG(INFO, "batch_size should not be [%u], will set to 100 by default", batchSize);
                batchSize = 100;
            }
            if (batchSize > 10000) {
                IE_LOG(INFO, "batch_size too large [%u], will set to 10000 by default", batchSize);
                batchSize = 10000;
            }
            continue;
        }

        if (iPair[0] == "max_enqueue_seconds") {
            int64_t maxEnQueueSeconds = 0;
            if (!StringUtil::fromString(iPair[1], maxEnQueueSeconds) || maxEnQueueSeconds <= 0) {
                IE_LOG(ERROR, "bad max_enqueue_seconds: value [%s]", iPair[1].c_str());
                return false;
            }
            maxEnQueueTime = maxEnQueueSeconds * 1000 * 1000;
            continue;
        }
        IE_LOG(ERROR, "bad strategy param key [%s]", iPair[0].c_str());
        return false;
    }
    return true;
}

void BatchProcessor::processDoc(const document::RawDocumentPtr &rawDoc)
{
    if (!rawDoc) { return; }

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

void BatchProcessor::stop()
{
    _loopThreadPtr.reset();
    {
        ScopedLock lock(_queueMutex);
        if (_docQueue) {
            FlushDocQueue();
        }
    }
    Processor::stop();
}

void BatchProcessor::resetDocQueue() {
    _docQueue.reset(new RawDocumentVec);
    _docQueue->reserve(_batchSize);
    _inQueueTime = TimeUtility::currentTime();
}

void BatchProcessor::FlushDocQueue() {
    if (_docQueue) {
        assert(!_docQueue->empty());
        REPORT_METRIC(_flushQueueSizeMetric, _docQueue->size());
        
        ProcessorWorkItem *workItem = new ProcessorWorkItem(
                getProcessorChains(), _chainSelector, &_reporter);

        if (_docQueue->size() == 1) {
            workItem->initProcessData((*_docQueue)[0]);
        } else {
            workItem->initBatchProcessData(_docQueue);
        }

        _reporter.increaseDocCount(_docQueue->size());
        if (_processDocCountCounter) {
            _processDocCountCounter->Increase(_docQueue->size());        
        }
        if (!_executor->push(workItem)) {
            BS_LOG(WARN, "push work item failed, drop it");
            delete workItem;
        }
    }
    int64_t newFlushTime = TimeUtility::currentTime();
    REPORT_METRIC(_flushIntervalMetric, newFlushTime - _lastFlushTime);
    _lastFlushTime = newFlushTime;

    _docQueue.reset();
    _inQueueTime = std::numeric_limits<int64_t>::max();
}

}
}
