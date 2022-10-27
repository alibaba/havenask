#include "build_service/workflow/SwiftProcessedDocConsumer.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/BeeperCollectorDefine.h"

#include "build_service/common/Locator.h"
#include "build_service/common/PkTracer.h"
#include "build_service/util/Monitor.h"
#include <indexlib/util/counter/state_counter.h>
#include <indexlib/util/counter/accumulative_counter.h>
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/misc/doc_tracer.h>
#include <autil/DataBuffer.h>
#include <unistd.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
SWIFT_USE_NAMESPACE(protocol);
SWIFT_USE_NAMESPACE(client);

using namespace build_service::document;
using namespace build_service::common;

namespace build_service {
namespace workflow {

SwiftWriterWithMetric::SwiftWriterWithMetric()
    : unsendMsgSizeMetric(nullptr)
    , uncommittedMsgSizeMetric(nullptr)
    , sendBufferFullQpsMetric(nullptr)
{
}

bool SwiftWriterWithMetric::isSendingDoc() const {
    swift::client::WriterMetrics writerMetric;
    swiftWriter->getWriterMetrics(zkRootPath, topicName, writerMetric);
    return writerMetric.uncommittedMsgCount > 0;
}

void SwiftWriterWithMetric::updateMetrics() {
    swift::client::WriterMetrics writerMetric;
    swiftWriter->getWriterMetrics(zkRootPath, topicName, writerMetric);
    REPORT_METRIC(unsendMsgSizeMetric, writerMetric.unsendMsgSize);

    REPORT_METRIC(uncommittedMsgSizeMetric, writerMetric.uncommittedMsgSize);
}

BS_LOG_SETUP(workflow, SwiftProcessedDocConsumer);

SwiftProcessedDocConsumer::SwiftProcessedDocConsumer()
    : _src(0)
    , _latestInvalidDocCheckpoint(-1)
    , _hasCounter(false)
{
}

SwiftProcessedDocConsumer::~SwiftProcessedDocConsumer() {
    if (_syncCounterThread) {
        _syncCounterThread->stop();
    }    
    if (_updateMetricThread) {
        _updateMetricThread->stop();
    }
}

bool SwiftProcessedDocConsumer::initCounters(const CounterMapPtr& counterMap) {
    if (!counterMap) {
        BS_LOG(ERROR, "counter map should not be empty");
        return false;        
    }

    vector<CounterPtr> targetCounter = counterMap->FindCounters(BS_COUNTER(locatorSource));
    if (targetCounter.size() > 1) {
        BS_LOG(ERROR, "init counter [locatorSource] failed");
        return false;
    }
    if (targetCounter.size() == 0) {
        _srcCounter = counterMap->GetStateCounter(BS_COUNTER(locatorSource));
        if (!_srcCounter) {
            BS_LOG(ERROR, "init counter [locatorSource] failed");
            return false;
        }
        _srcCounter->Set(0);
    } else {
        _srcCounter = DYNAMIC_POINTER_CAST(StateCounter, targetCounter[0]);
        if (!_srcCounter) {
            BS_LOG(ERROR, "init counter [locatorSource] failed");
            return false;            
        }
    }

    targetCounter = counterMap->FindCounters(BS_COUNTER(locatorCheckpoint));
    if (targetCounter.size() > 1) {
        BS_LOG(ERROR, "init counter [locatorCheckpoint] failed");
        return false;
    }
    if (targetCounter.size() == 0) {
        _checkpointCounter = counterMap->GetStateCounter(BS_COUNTER(locatorCheckpoint));
        if (!_checkpointCounter) {
            BS_LOG(ERROR, "init counter [locatorCheckpoint] failed");
            return false;            
        }
        _checkpointCounter->Set(-1);
    } else {
        _checkpointCounter = DYNAMIC_POINTER_CAST(StateCounter, targetCounter[0]);
        if (!_checkpointCounter) {
            BS_LOG(ERROR, "init counter [locatorCheckpoint] failed");
            return false; 
        }
    }

    _consumedDocCountCounter = GET_ACC_COUNTER(counterMap, consumedDocCount);
    if (!_consumedDocCountCounter) {
        return false;
    }
    int64_t remoteSrc = _srcCounter->Get();
    if (remoteSrc >= 0 && (uint64_t)remoteSrc >= _src) {
        _cachedLocator.setSrc(remoteSrc);
        _cachedLocator.setOffset(_checkpointCounter->Get());
    }
    _hasCounter = true;
    return true;
}

bool SwiftProcessedDocConsumer::init(const map<string, SwiftWriterWithMetric> &writers,
                                     const CounterSynchronizerPtr &counterSynchronizer,
                                     int64_t checkpointInterval,
                                     uint64_t srcSignature)
{
    _src = srcSignature;
    _writers = writers;
    if (counterSynchronizer) {
        _counterSynchronizer = counterSynchronizer;
        if (!initCounters(_counterSynchronizer->getCounterMap())) {
            BS_LOG(ERROR, "initCounters failed");
        }
    } else {
        BS_LOG(ERROR, "counterSynchronizer is NULL");
    }

    _syncCounterThread = autil::LoopThread::createLoopThread(
            std::tr1::bind(&SwiftProcessedDocConsumer::syncCountersWrapper, this),
            checkpointInterval * 1000 * 1000, "BsSwiftCounter");
    if (!_syncCounterThread) {
        string errorMsg = "create [sync counter] thread failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    _updateMetricThread = autil::LoopThread::createLoopThread(
            std::tr1::bind(&SwiftProcessedDocConsumer::updateMetric, this),
            1 * 1000 * 1000, "BsSwiftMetric");

    return true;
}

FlowError SwiftProcessedDocConsumer::consume(
        const ProcessedDocumentVecPtr &item)
{
    autil::ScopedLock lock(_consumeLock);
    for (size_t i = 0; i < (*item).size(); i++) {
        const ProcessedDocumentPtr &processedDoc = (*item)[i];
        const ProcessedDocument::DocClusterMetaVec &metaVec =
            processedDoc->getDocClusterMetaVec();
        const DocumentPtr &document = processedDoc->getDocument();
        const common::Locator &locator = processedDoc->getLocator();

        if (!document || document->GetDocOperateType() == SKIP_DOC ||
            document->GetDocOperateType() == UNKNOWN_OP) {
            int64_t docCheckpoint = locator.getOffset();
            _latestInvalidDocCheckpoint = max(_latestInvalidDocCheckpoint, docCheckpoint);
            IE_INDEX_DOC_TRACE(document, "processed doc to skip");
            continue;
        }

        string docString = transToDocStr(document);

        for (size_t j = 0; j < metaVec.size(); j++) {
            const string &clusterName = metaVec[j].clusterName;
            auto it = _writers.find(clusterName);
            if (it == _writers.end()) {
                string errorMsg = "cluster[" + clusterName + "] not exist for build";
                BS_LOG(WARN, "%s", errorMsg.c_str());
                continue;
            }
            MessageInfo message;
            message.data = docString;
            message.uint16Payload = metaVec[j].hashId;
            message.uint8Payload = metaVec[j].buildType;
            message.checkpointId = locator.getOffset();
            ErrorCode ec = it->second.swiftWriter->write(message);
            if (ec != ERROR_NONE) {
                if (ec == ERROR_CLIENT_SEND_BUFFER_FULL) {
                    INCREASE_QPS(it->second.sendBufferFullQpsMetric);
                    BS_INTERVAL_LOG(300, WARN, "swift send buffer is full");
                    BEEPER_INTERVAL_REPORT(120, WORKER_ERROR_COLLECTOR_NAME, "swift send buffer is full");
                } else {
                    string errorMsg = "write error for cluster[" + clusterName + "]";
                    BS_LOG(ERROR, "%s, errorCode[%u]", errorMsg.c_str(), ec);
                    BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
                }
                return FE_EXCEPTION;
            }
        }
        PkTracer::toSwiftTrace(document->GetPrimaryKey(),
                               document->GetLocator().ToString());
        IE_INDEX_DOC_TRACE(document, "write processed doc to swift");
        _src = locator.getSrc();
    }
    if (_consumedDocCountCounter) {
        _consumedDocCountCounter->Increase(1);
    }
    return FE_OK;
}

void SwiftProcessedDocConsumer::updateMetric() {
    for (auto it = _writers.begin(); it != _writers.end(); ++it) {
        it->second.updateMetrics();
    }
}

void SwiftProcessedDocConsumer::syncCountersWrapper() {
    syncCounters(getMinCheckpoint());
}

bool SwiftProcessedDocConsumer::syncCounters(int64_t checkpointId) {
    if (checkpointId < 0) {
        return true;
    }

    if (!_hasCounter) {
        autil::ScopedLock lock(_lock);
        if (_src != _cachedLocator.getSrc() ||
            _cachedLocator.getOffset() < checkpointId) {
            _cachedLocator.setSrc(_src);
            _cachedLocator.setOffset(checkpointId);
        }
        return true;
    }

    uint64_t remoteSrc = _srcCounter->Get();
    int64_t remoteCheckpoint = _checkpointCounter->Get();

    if (remoteSrc != _src || checkpointId > remoteCheckpoint) {
        BS_INTERVAL_LOG(10, INFO, "serialize checkpoint[%lu:%ld]",
                        _src, checkpointId);
        {
            autil::ScopedLock lock(_lock);
            if (_src != _cachedLocator.getSrc() ||
                _cachedLocator.getOffset() < checkpointId) {
                _cachedLocator.setSrc(_src);
                _cachedLocator.setOffset(checkpointId);
            }
        }
        _srcCounter->Set(_src);
        _checkpointCounter->Set(checkpointId);
        if (!_counterSynchronizer->sync()) {
            _srcCounter->Set(remoteSrc);
            _checkpointCounter->Set(remoteCheckpoint);
            return false;
        }

    }
    return true;
}

int64_t SwiftProcessedDocConsumer::getMinCheckpoint() const {
    autil::ScopedLock lock(_consumeLock);
    int64_t sendingCheckpoint = numeric_limits<int64_t>::max();
    bool allSend = true;
    int64_t allSendCheckpoint = numeric_limits<int64_t>::min();
    for (auto it = _writers.begin(); it != _writers.end(); ++it) {
        bool isSending = it->second.isSendingDoc();
        int64_t commitedCheckpointId = it->second.swiftWriter->getCommittedCheckpointId();
        if (isSending) {
            allSend = false;
            sendingCheckpoint = min(sendingCheckpoint, commitedCheckpointId);
        }

        if (allSend) {
            allSendCheckpoint = max(allSendCheckpoint, commitedCheckpointId);
        }
    }

    if (allSend) {
        return allSendCheckpoint;
    }
    return sendingCheckpoint;
}

string SwiftProcessedDocConsumer::transToDocStr(
        const DocumentPtr &document)
{
    DataBuffer dataBuffer;
    dataBuffer.write(document);
    return string(dataBuffer.getData(), dataBuffer.getDataLen());
}

bool SwiftProcessedDocConsumer::getLocator(common::Locator &locator) const {
    autil::ScopedLock lock(_lock);
    if (_cachedLocator.getOffset() != -1) {
        locator = _cachedLocator;
    }
    return true;
}

bool SwiftProcessedDocConsumer::stop(FlowError lastError) {
    while (!isFinished()) {
        BS_LOG(INFO, "waiting swift message flush");
        usleep(1 * 1000 * 1000);
    }

    if (_syncCounterThread) {
        _syncCounterThread->stop();
        _syncCounterThread.reset();
    }
    int64_t checkpointId = max(_latestInvalidDocCheckpoint, getMinCheckpoint());
    int64_t retry = 0;
    while (!syncCounters(checkpointId)) {
        if (++retry > MAX_SYNC_RETRY) {
            break;
        }
        usleep(1 * 1000 * 1000);
    }

    return true;
}

bool SwiftProcessedDocConsumer::isFinished() const {
    for (auto it = _writers.begin(); it != _writers.end(); ++it) {
        ErrorCode ec = it->second.swiftWriter->waitFinished(200 * 1000);
        if (ec != ERROR_NONE) {
            return false;
        }
    }
    return true;
}

}
}
