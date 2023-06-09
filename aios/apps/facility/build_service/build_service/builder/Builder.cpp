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
#include "build_service/builder/Builder.h"

#include "autil/EnvUtil.h"
#include "build_service/common/PeriodDocCounter.h"
#include "build_service/common/PkTracer.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/util/Monitor.h"
#include "indexlib/config/build_config.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/misc/doc_tracer.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"

using namespace std;
using namespace autil;

using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::util;

using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::config;
using namespace build_service::util;

namespace build_service { namespace builder {

BS_LOG_SETUP(builder, Builder);

#define LOG_PREFIX _buildId.ShortDebugString().c_str()

Builder::Builder(const IndexBuilderPtr& indexBuilder, fslib::fs::FileLock* fileLock, const proto::BuildId& buildId)
    : _indexBuilder(indexBuilder)
    , _fileLock(fileLock)
    , _buildId(buildId)
    , _fatalError(false)
    , _speedLimiter(_buildId.ShortDebugString())
    , _fieldId(INVALID_FIELDID)
    , _fieldType(FieldType::ft_unknown)
    , _docCounter(NULL)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
    initBeeperTag(buildId);
}

Builder::~Builder() { DELETE_AND_SET_NULL(_fileLock); }

bool Builder::init(const BuilderConfig& builderConfig, indexlib::util::MetricProviderPtr metricProvider)
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
        indexlib::TableType tableType = _indexBuilder->GetIndexPartition()->GetSchema()->GetTableType();
        if (tableType == indexlib::tt_kkv || tableType == indexlib::tt_kv) {
            isKVorKKV = true;
        }
    }
    string traceField;
    if (EnvUtil::getEnvWithoutDefault(BS_ENV_DOC_TRACE_FIELD.c_str(), traceField)) {
        auto schema = _indexBuilder->GetIndexPartition()->GetSchema();
        auto attrSchema = schema->GetAttributeSchema();
        auto attrConfig = attrSchema->GetAttributeConfig(traceField);
        if (attrConfig) {
            _fieldId = attrConfig->GetFieldId();
            _fieldType = attrConfig->GetFieldType();
            _docCounter = PeriodDocCounterHelper::create(_fieldType);
        }
    }
    _speedLimiter.setTargetQps(builderConfig.sleepPerdoc, builderConfig.buildQpsLimit);
    return _builderMetrics.declareMetrics(metricProvider, isKVorKKV);
}

void Builder::MaybeTraceDocType(Document* doc, int32_t docOperator, int32_t originDocOperator, bool buildSuccess)
{
    const std::string& pk = doc->GetPrimaryKey();
    if (buildSuccess) {
        PkTracer::toBuildTrace(pk, originDocOperator, docOperator);
    } else {
        PkTracer::buildFailTrace(pk, originDocOperator, docOperator);
    }
    if (doc && doc->NeedTrace()) {
        std::string originDocOperatorStr = docOperateTypeToStr(originDocOperator);
        std::string docOperatorStr = docOperateTypeToStr(docOperator);
        std::string result = buildSuccess ? "success" : "failed";
        beeper::EventTags traceTags;
        traceTags.AddTag("pk", pk);
        char msg[1024];
        sprintf(msg, "build doc %s, originalOp is [%s], docOp is [%s]", result.c_str(), originDocOperatorStr.c_str(),
                docOperatorStr.c_str());
        BEEPER_REPORT(IE_DOC_TRACER_COLLECTOR_NAME, msg, traceTags);
    }
}

void Builder::MaybeTraceDocField(Document* doc, fieldid_t fieldId, FieldType fieldType,
                                 common::PeriodDocCounterBase* docCounter)
{
    if (fieldId == INVALID_FIELDID) {
        return;
    }
    NormalDocument* normalDoc = dynamic_cast<NormalDocument*>(doc);
    if (normalDoc == nullptr) {
        return;
    }
    const AttributeDocumentPtr& attrDoc = normalDoc->GetAttributeDocument();
    const autil::StringView& field = attrDoc->GetField(fieldId);
    if (field != autil::StringView::empty_instance()) {
        PeriodDocCounterHelper::count(PeriodDocCounterType::Builder, fieldType, field.data(), field.size(), docCounter);
    }
}

bool Builder::build(const DocumentPtr& doc)
{
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
        if (CHECKPOINT_DOC == doc->GetOriginalOperateType() || CHECKPOINT_DOC == doc->GetDocOperateType()) {
            return true;
        }
        buildSuccess = doBuild(doc);
        MaybeTraceDocField(doc.get(), _fieldId, _fieldType, _docCounter);
        MaybeTraceDocType(doc.get(), doc->GetDocOperateType(), doc->GetOriginalOperateType(), buildSuccess);
        hasException = false;
    } catch (const FileIOException& e) {
        setFatalError();
        ec = BUILDER_ERROR_BUILD_FILEIO;
        errorMsg += e.what();
    } catch (const ReachMaxResourceException& e) {
        ec = BUILDER_ERROR_REACH_MAX_RESOURCE;
        errorMsg += e.what();
    } catch (const ExceptionBase& e) {
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
    ReportMetrics(doc.get());
    return !hasException && !hasFatalError();
}

void Builder::ReportMetrics(Document* doc)
{
    auto lastConsumedMessageCount = _indexBuilder->GetLastConsumedMessageCount();
    _builderMetrics.reportMetrics(doc->GetDocumentCount(), lastConsumedMessageCount, doc->GetDocOperateType());
    if (lastConsumedMessageCount > 0) {
        if (_totalDocCountCounter) {
            _totalDocCountCounter->Increase(lastConsumedMessageCount);
        }
        if (_builderDocCountCounter) {
            _builderDocCountCounter->Set(_builderMetrics.getTotalDocCount());
        }
    }
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
    if (type == CHECKPOINT_DOC) {
        return "checkpoint";
    }
    return "unkown";
}

bool Builder::tryDump()
{
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
            if (_docCounter) {
                _docCounter->flush();
            }
        } catch (const FileIOException& e) {
            setFatalError();
            ec = BUILDER_ERROR_BUILD_FILEIO;
            errorMsg += e.what();
        } catch (const ReachMaxResourceException& e) {
            ec = BUILDER_ERROR_REACH_MAX_RESOURCE;
            errorMsg += e.what();
        } catch (const ExceptionBase& e) {
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

bool Builder::merge(const IndexPartitionOptions& options)
{
    ServiceErrorCode ec = SERVICE_ERROR_NONE;
    string errorMsg = "merge failed: ";
    ScopedLock lock(_indexBuilderMutex);
    try {
        doMerge(options);
    } catch (const FileIOException& ioe) {
        setFatalError();
        ec = BUILDER_ERROR_MERGE_FILEIO;
        errorMsg += ioe.what();
    } catch (const ExceptionBase& e) {
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

bool Builder::doBuild(const DocumentPtr& doc) { return _indexBuilder->Build(doc); }

void Builder::doMerge(const IndexPartitionOptions& options) { _indexBuilder->Merge(options); }

void Builder::stop(int64_t stopTimestamp)
{
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
    } catch (const FileIOException& e) {
        ec = BUILDER_ERROR_DUMP_FILEIO;
        errorMsg += e.what();
    } catch (const ExceptionBase& e) {
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
    if (_indexBuilder->GetIncVersionTimestamp(ts)) {
        return RS_OK;
    }
    return RS_LOCK_BUSY;
}

int64_t Builder::getIncVersionTimestamp() const { return _indexBuilder->GetIncVersionTimestamp(); }

Builder::RET_STAT Builder::getLastLocatorNonBlocking(common::Locator& locator) const
{
    locator = common::Locator();
    try {
        string locatorStr;
        if (!_indexBuilder->GetLastLocator(locatorStr)) {
            return RS_LOCK_BUSY;
        }
        if (!locatorStr.empty() && !locator.Deserialize(locatorStr)) {
            string errorMsg = string("deserialize [") + locatorStr + "] failed";
            REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
            return RS_FAIL;
        }
    } catch (const ExceptionBase& e) {
        string errorMsg = "GetLastLocator failed, exception: " + string(e.what());
        REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
        return RS_FAIL;
    }
    return RS_OK;
}

bool Builder::getLastLocator(common::Locator& locator) const
{
    locator = common::Locator();
    try {
        string locatorStr = _indexBuilder->GetLastLocator();
        if (!locatorStr.empty() && !locator.Deserialize(locatorStr)) {
            string errorMsg = string("deserialize [") + locatorStr + "] failed";
            REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
            return false;
        }
    } catch (const ExceptionBase& e) {
        string errorMsg = "GetLastLocator failed, exception: " + string(e.what());
        REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
        return false;
    }
    return true;
}

bool Builder::getLatestVersionLocator(common::Locator& locator) const
{
    try {
        string locatorStr = _indexBuilder->GetLocatorInLatestVersion();
        if (!locatorStr.empty() && !locator.Deserialize(locatorStr)) {
            BS_PREFIX_LOG(ERROR, "deserialize locator from [%s] failed", locatorStr.c_str());
            return false;
        }
    } catch (const ExceptionBase& e) {
        string errorMsg = "GetLastLocator failed, exception: " + string(e.what());
        REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
        return false;
    }
    return true;
}

void Builder::storeBranchCkp()
{
    if (hasFatalError()) {
        return;
    }
    if (_indexBuilder) {
        _indexBuilder->StoreBranchCkp();
    }
}

bool Builder::getLastFlushedLocator(common::Locator& locator)
{
    if (hasFatalError()) {
        return false;
    }
    try {
        string locatorStr = _indexBuilder->GetLastFlushedLocator();
        if (!locatorStr.empty() && !locator.Deserialize(locatorStr)) {
            BS_PREFIX_LOG(ERROR, "deserialize locator from [%s] failed", locatorStr.c_str());
            return false;
        }
    } catch (const ExceptionBase& e) {
        string errorMsg = "GetLastFlushedLocator failed, exception: " + string(e.what());
        REPORT_ERROR_WITH_ADVICE(BUILDER_ERROR_GET_LAST_LOCATOR, errorMsg, BS_RETRY);
        return false;
    }
    return true;
}

bool Builder::hasFatalError() const { return _fatalError; }

void Builder::setFatalError() { _fatalError = true; }

BuildStep Builder::TEST_GET_BUILD_STEP()
{
    return _indexBuilder->TEST_GET_INDEX_PARTITION_OPTIONS().GetBuildConfig(false).buildPhase == BuildConfig::BP_FULL
               ? BUILD_STEP_FULL
               : BUILD_STEP_INC;
}

void Builder::flush()
{
    if (_indexBuilder) {
        _indexBuilder->Flush();
    }
}

void Builder::switchToConsistentMode()
{
    if (_indexBuilder) {
        _indexBuilder->SwitchToConsistentMode();
    }
}

#undef LOG_PREFIX

}} // namespace build_service::builder
