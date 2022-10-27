#include "build_service/builder/Builder.h"
#include "build_service/util/Monitor.h"
#include "build_service/common/PkTracer.h"
#include "build_service/proto/ProtoUtil.h"
#include <indexlib/util/counter/accumulative_counter.h>
#include <indexlib/util/counter/state_counter.h>
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/config/build_config.h>
#include <indexlib/partition/index_partition.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(util);

using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::config;

namespace build_service {
namespace builder {

BS_LOG_SETUP(builder, Builder);

#define LOG_PREFIX _buildId.ShortDebugString().c_str()

Builder::Builder(const IndexBuilderPtr &indexBuilder, fslib::fs::FileLock *fileLock,
                 const proto::BuildId &buildId)
    : _indexBuilder(indexBuilder)
    , _fileLock(fileLock)
    , _buildId(buildId)
    , _fatalError(false)
    , _speedLimiter(_buildId.ShortDebugString())
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
    initBeeperTag(buildId);
}

Builder::~Builder() {
    DELETE_AND_SET_NULL(_fileLock);
}

bool Builder::init(const BuilderConfig &builderConfig,
                   IE_NAMESPACE(misc)::MetricProviderPtr metricProvider)
{
    bool isKVorKKV = false;
    if (_indexBuilder) {
        CounterMapPtr counterMap = _indexBuilder->GetCounterMap();
        if (!counterMap) {
            BS_LOG(ERROR, "init Builder failed: counterMap is NULL");
        } else {
            _totalDocCountCounter = GET_ACC_COUNTER(counterMap, totalDocCount);
            if (!_totalDocCountCounter) {
                BS_LOG(ERROR, "can not get totalDocCountCounter");
            }
	    _builderDocCountCounter = GET_STATE_COUNTER(counterMap, builderDocCount);
	    if (!_builderDocCountCounter) {
	        BS_LOG(ERROR, "can not get builderDocCountCounter");
	    } else {
	        _builderDocCountCounter->Set(0);
	    }
        }
        heavenask::indexlib::TableType tableType = _indexBuilder->GetIndexPartition()->GetSchema()->GetTableType();
        if (tableType == heavenask::indexlib::tt_kkv
            || tableType == heavenask::indexlib::tt_kv) {
            isKVorKKV = true;
        }
    }
    _speedLimiter.setTargetQps(builderConfig.sleepPerdoc,
                               builderConfig.buildQpsLimit);
    return _builderMetrics.declareMetrics(metricProvider, isKVorKKV); 
}

bool Builder::build(const DocumentPtr &doc) {
    assert(doc);
    ServiceErrorCode ec = SERVICE_ERROR_NONE;
    string errorMsg = "build failed: ";
    bool buildSuccess = false;
    bool hasException = true;
    ScopedLock lock(_indexBuilderMutex);
    if (hasFatalError()) {
        return false;
    }
    try {
        _lastPk = doc->GetPrimaryKey();
        _speedLimiter.limitSpeed();
        buildSuccess = doBuild(doc);
        int32_t originDocOperator = doc->GetOriginalOperateType();
        string originDocOperatorStr = docOperateTypeToStr(originDocOperator);
        int32_t docOperator = doc->GetDocOperateType();
        string docOperatorStr = docOperateTypeToStr(docOperator);
        if (buildSuccess) {
            PkTracer::toBuildTrace(_lastPk, originDocOperator, docOperator);
            IE_INDEX_DOC_FORMAT_TRACE(doc, "build doc success, originalOp is [%s], docOp is [%s]",
                    originDocOperatorStr.c_str(),
                    docOperatorStr.c_str());
        } else {
            BS_PREFIX_LOG(TRACE1, "origin operator[%s] operator[%s] pk[%s] failed",
                          originDocOperatorStr.c_str(), docOperatorStr.c_str(), _lastPk.c_str());
            PkTracer::buildFailTrace(_lastPk, originDocOperator, docOperator);
            IE_INDEX_DOC_FORMAT_TRACE(doc, "build doc failed, originalOp is [%s], docOp is [%s]",
                    originDocOperatorStr.c_str(), docOperatorStr.c_str());
        }
        hasException = false;
    } catch (const FileIOException &e) {
        setFatalError();
        ec = BUILDER_ERROR_BUILD_FILEIO;
        errorMsg += e.what();
    } catch (const ReachMaxResourceException &e) {
        ec = BUILDER_ERROR_REACH_MAX_RESOURCE;
        errorMsg += e.what();
    } catch (const ExceptionBase &e) {
        setFatalError();
        ec = BUILDER_ERROR_UNKNOWN;
        errorMsg += e.what();
    } catch (...) {
        setFatalError();
        ec = BUILDER_ERROR_UNKNOWN;
        errorMsg += "unknown exception";
    }
    auto lastConsumedMessageCount = _indexBuilder->GetLastConsumedMessageCount();
    _builderMetrics.reportMetrics(doc->GetMessageCount(), lastConsumedMessageCount, doc->GetDocOperateType());
    if (ec != SERVICE_ERROR_NONE) {
        REPORT_ERROR_WITH_ADVICE(ec, errorMsg, BS_RETRY);
    }
    if (lastConsumedMessageCount > 0) {
        if (_totalDocCountCounter) {
            _totalDocCountCounter->Increase(lastConsumedMessageCount);
        }
        if (_builderDocCountCounter) {
            _builderDocCountCounter->Set(_builderMetrics.getTotalDocCount());
        }
    }
    return !hasException && !hasFatalError();
}

std::string Builder::docOperateTypeToStr(int32_t type)
{
    if (type == UNKNOWN_OP) {
        return "unkown";
    }
    if (type == ADD_DOC) {
        return "add";
    }
    if (type == DELETE_DOC) {
        return "delete";
    }
    if (type == UPDATE_FIELD) {
        return "update";
    }
    if (type == SKIP_DOC) {
        return "skip";
    }
    if (type == DELETE_SUB_DOC) {
        return "delete_sub";
    }
    return "unkown";
}


bool Builder::tryDump() {
    if (_indexBuilder && !_indexBuilder->IsDumping()) {
        ScopedLock lock(_indexBuilderMutex);
        if (hasFatalError()) {
            return false;
        }

        BEEPER_REPORT(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "Builder tryDump: memory_prefer processor needUpdateCommitedCheckpoint, "
                "but builder read no more msg over no_more_msg_period.");
        
        ServiceErrorCode ec = SERVICE_ERROR_NONE;
        string errorMsg = "try dump failed: ";
        bool hasException = true;
        try {
            _indexBuilder->DumpSegment();
            hasException = false;
        }  catch (const FileIOException &e) {
            setFatalError();
            ec = BUILDER_ERROR_BUILD_FILEIO;
            errorMsg += e.what();
        } catch (const ReachMaxResourceException &e) {
            ec = BUILDER_ERROR_REACH_MAX_RESOURCE;
            errorMsg += e.what();
        } catch (const ExceptionBase &e) {
            setFatalError();
            ec = BUILDER_ERROR_UNKNOWN;
            errorMsg += e.what();
        } catch (...) {
            setFatalError();
            ec = BUILDER_ERROR_UNKNOWN;
            errorMsg += "unknown exception";
        }
        if (ec != SERVICE_ERROR_NONE) {
            REPORT_ERROR_WITH_ADVICE(ec, errorMsg, BS_RETRY);
        }        
        return !hasException && !hasFatalError();
    }
    return false;    
}

bool Builder::merge(const IndexPartitionOptions &options) {
    ServiceErrorCode ec = SERVICE_ERROR_NONE;
    string errorMsg = "merge failed: ";
    ScopedLock lock(_indexBuilderMutex);
    try {
        doMerge(options);
    } catch (const FileIOException &ioe) {
        setFatalError();
        ec = BUILDER_ERROR_MERGE_FILEIO;
        errorMsg += ioe.what();
    } catch (const ExceptionBase &e) {
        setFatalError();
        ec = BUILDER_ERROR_UNKNOWN;
        errorMsg += e.what();
    } catch (...) {
        setFatalError();
        ec = BUILDER_ERROR_UNKNOWN;
        errorMsg += "unknown exception";
    }
    if (ec != SERVICE_ERROR_NONE) {
        REPORT_ERROR_WITH_ADVICE(ec, errorMsg, BS_RETRY);
    }
    return !hasFatalError();
}

bool Builder::doBuild(const DocumentPtr &doc) {
    return _indexBuilder->Build(doc);
}

void Builder::doMerge(const IndexPartitionOptions &options) {
    _indexBuilder->Merge(options);
}

void Builder::stop(int64_t stopTimestamp) {
    if (hasFatalError()) {
        BS_PREFIX_LOG(INFO, "fatal error happened, do not endIndex");
        return;
    }
    BS_PREFIX_LOG(INFO, "builder stop begin, last pk [%s]", _lastPk.c_str());
    ScopedLock lock(_indexBuilderMutex);
    ServiceErrorCode ec = SERVICE_ERROR_NONE;
    string errorMsg = "EndIndex failed: ";
    try {
        BS_LOG(INFO, "end index with stopTimestamp = [%ld]", stopTimestamp);
        _indexBuilder->EndIndex(stopTimestamp);
    } catch (const FileIOException &e) {
        ec = BUILDER_ERROR_DUMP_FILEIO;
        errorMsg += e.what();
    } catch (const ExceptionBase &e) {
        ec = BUILDER_ERROR_UNKNOWN;
        errorMsg += e.what();
    } catch (...) {
        ec = BUILDER_ERROR_UNKNOWN;
        errorMsg += "unknown exception";
    }
    if (ec != SERVICE_ERROR_NONE) {
        setFatalError();
        REPORT_ERROR_WITH_ADVICE(ec, errorMsg, BS_RETRY);
    }
    BS_PREFIX_LOG(INFO, "builder stop end");
}

Builder::RET_STAT Builder::getIncVersionTimestampNonBlocking(int64_t& ts) const
{
    if (_indexBuilder->GetIncVersionTimestamp(ts))
    {
        return RS_OK;
    }
    return RS_LOCK_BUSY;
}

int64_t Builder::getIncVersionTimestamp() const {
    return _indexBuilder->GetIncVersionTimestamp();
}

Builder::RET_STAT Builder::getLastLocatorNonBlocking(common::Locator &locator) const
{
    locator = common::INVALID_LOCATOR;
    try {
        string locatorStr;
        if (!_indexBuilder->GetLastLocator(locatorStr))
        {
            return RS_LOCK_BUSY;
        }
        if (!locatorStr.empty() && !locator.fromString(locatorStr)) {
            string errorMsg = string("deserialize [") + locatorStr + "] failed";
            REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
            return RS_FAIL;
        }
    } catch (const ExceptionBase &e) {
        string errorMsg = "GetLastLocator failed, exception: " + string(e.what());
        REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
        return RS_FAIL;
    }
    return RS_OK;
}

bool Builder::getLastLocator(common::Locator &locator) const {
    locator = common::INVALID_LOCATOR;
    try {
        string locatorStr = _indexBuilder->GetLastLocator();
        if (!locatorStr.empty() && !locator.fromString(locatorStr)) {
            string errorMsg = string("deserialize [") + locatorStr + "] failed";
            REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
            return false;
        }
    } catch (const ExceptionBase &e) {
        string errorMsg = "GetLastLocator failed, exception: " + string(e.what());
        REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
        return false;
    }
    return true;
}

bool Builder::getLatestVersionLocator(common::Locator &locator) const {
    try {
        string locatorStr = _indexBuilder->GetLocatorInLatestVersion();
        if (!locatorStr.empty() && !locator.fromString(locatorStr)) {
            BS_PREFIX_LOG(ERROR, "deserialize locator from [%s] failed", locatorStr.c_str());
            return false;
        }
    } catch (const ExceptionBase &e) {
        string errorMsg = "GetLastLocator failed, exception: " + string(e.what());
        REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
        return false;
    }
    return true;
}

bool Builder::getLastFlushedLocator(common::Locator &locator) {
    if (hasFatalError()) {
        return false;
    }
    try {
        string locatorStr = _indexBuilder->GetLastFlushedLocator();
        if (!locatorStr.empty() && !locator.fromString(locatorStr)) {
            BS_PREFIX_LOG(ERROR, "deserialize locator from [%s] failed", locatorStr.c_str());
            return false;
        }
    } catch (const ExceptionBase &e) {
        string errorMsg = "GetLastFlushedLocator failed, exception: " + string(e.what());
        REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
        return false;
    }
    return true;
}

bool Builder::hasFatalError() const {
    return _fatalError;
}

void Builder::setFatalError() {
    _fatalError = true;
}

BuildStep Builder::TEST_GET_BUILD_STEP() {
    return _indexBuilder->TEST_GET_INDEX_PARTITION_OPTIONS().GetBuildConfig(false).buildPhase == BuildConfig::BP_FULL ? BUILD_STEP_FULL : BUILD_STEP_INC;
}


#undef LOG_PREFIX

}
}
