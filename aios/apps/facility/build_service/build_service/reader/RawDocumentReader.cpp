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
#include "build_service/reader/RawDocumentReader.h"

#include <memory>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/PeriodDocCounter.h"
#include "build_service/common/PkTracer.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/reader/ParserCreator.h"
#include "build_service/util/Monitor.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/IDocumentFactory.h"
#include "indexlib/document/query/query_parser_creator.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/misc/doc_tracer.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::common;
using namespace build_service::proto;
using namespace build_service::document;
using namespace build_service::util;

using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::util;

namespace build_service { namespace reader {

BS_LOG_SETUP(reader, RawDocumentReader);

RawDocumentReader::RawDocumentReader()
    : _parser(NULL)
    , _hashMapManager(new RawDocumentHashMapManager)
    , _timestampFieldName(HA_RESERVED_TIMESTAMP)
    , _sourceFieldName(HA_RESERVED_SOURCE)
    , _needDocTag(false)
    , _batchBuildSize(1)
    , _docCounter(NULL)
    , _checkpointStep(DEFAULT_TRIGGER_CHECKPOINT_STEP)
    , _enableIngestionTimestamp(false)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
}

RawDocumentReader::~RawDocumentReader() { DELETE_AND_SET_NULL(_parser); }

void RawDocumentReader::initBeeperTagsFromPartitionId(const proto::PartitionId& pid) { initBeeperTag(pid); }

bool RawDocumentReader::initialize(const ReaderInitParam& initParams)
{
    ReaderInitParam params = initParams;

    _timestampFieldName = getValueFromKeyValueMap(params.kvMap, TIMESTAMP_FIELD, _timestampFieldName);
    _sourceFieldName = getValueFromKeyValueMap(params.kvMap, SOURCE_FIELD, _sourceFieldName);
    _sourceFieldDefaultValue = getValueFromKeyValueMap(params.kvMap, SOURCE_DEFAULT, _sourceFieldDefaultValue);
    _traceField = getValueFromKeyValueMap(params.kvMap, TRACE_FIELD);
    _extendFieldName = getValueFromKeyValueMap(params.kvMap, EXTEND_FIELD_NAME);
    _extendFieldValue = getValueFromKeyValueMap(params.kvMap, EXTEND_FIELD_VALUE);
    const std::string enableIngestionTimestamp = getValueFromKeyValueMap(params.kvMap, ENABLE_INGESTION_TIMESTAMP);
    if (!StringUtil::fromString(enableIngestionTimestamp, _enableIngestionTimestamp)) {
        BS_LOG(WARN, "de-serialize enable_ingestion_timestamp failed.");
        _enableIngestionTimestamp = false;
    }

    params.kvMap[indexlib::TIMESTAMP_FIELD] = _timestampFieldName;

    _e2eLatencyReporter.init(params.metricProvider);
    if (!initDocumentFactoryWrapper(params)) {
        BS_LOG(ERROR, "init document factory wrapper failed");
        return false;
    }

    if (!initDocumentFactoryV2(params)) {
        BS_LOG(ERROR, "init tablet factory failed");
        return false;
    }

    if (!initBatchBuild(params)) {
        BS_LOG(ERROR, "init batch build size failed");
        return false;
    }

    if (!initRawDocQueryEvaluator(params)) {
        BS_LOG(ERROR, "init raw document query evaluator failed");
        return false;
    }

    assert(!_parser);
    _parser = createRawDocumentParser(params);
    if (!_parser) {
        string errorMsg = "create RawDocumentParser failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!_parser->init(params.kvMap)) {
        BS_LOG(ERROR, "parser init failed");
        return false;
    }

    if (!initMetrics(params.metricProvider)) {
        string errorMsg = "reader initMetrics failed";
        REPORT_ERROR_WITH_ADVICE(READER_ERROR_INIT_METRICS, errorMsg, BS_RETRY);
        return false;
    }

    if (!params.counterMap) {
        BS_LOG(ERROR, "create parse counter failed.");
    }

    _parseFailCounter = GET_ACC_COUNTER(params.counterMap, parseFailCount);
    if (!_parseFailCounter) {
        BS_LOG(ERROR, "create parse counter failed.");
    }
    _traceField = EnvUtil::getEnv(BS_ENV_DOC_TRACE_FIELD, _traceField);
    if (!_traceField.empty()) {
        _docCounter = PeriodDocCounterHelper::create(FieldType::ft_string);
    }

    _checkpointStep = EnvUtil::getEnv(BS_ENV_READER_CHECKPOINT_STEP, _checkpointStep);
    return init(params);
}

bool RawDocumentReader::init(const ReaderInitParam& params)
{
    string needPrintDocTag = "false";
    needPrintDocTag = getValueFromKeyValueMap(params.kvMap, NEED_PRINT_DOC_TAG, needPrintDocTag);
    _needDocTag = (needPrintDocTag == "true");
    return true;
}

bool RawDocumentReader::initMetrics(MetricProviderPtr metricProvider)
{
    _rawDocSizeMetric = DECLARE_METRIC(metricProvider, "perf/rawDocSize", kmonitor::GAUGE, "byte");
    _rawDocSizePerSecMetric = DECLARE_METRIC(metricProvider, "perf/rawDocSizePerSec", kmonitor::QPS, "byte");
    _readLatencyMetric = DECLARE_METRIC(metricProvider, "basic/readLatency", kmonitor::GAUGE, "ms");
    _getNextDocLatencyMetric = DECLARE_METRIC(metricProvider, "basic/getNextDocLatency", kmonitor::GAUGE, "ms");
    _parseLatencyMetric = DECLARE_METRIC(metricProvider, "perf/parseLatency", kmonitor::GAUGE, "ms");
    _parseErrorQpsMetric = DECLARE_METRIC(metricProvider, "perf/parseErrorQps", kmonitor::QPS, "count");
    _readDocQpsMetric = DECLARE_METRIC(metricProvider, "perf/readDocQps", kmonitor::QPS, "count");
    _filterDocQpsMetric = DECLARE_METRIC(metricProvider, "perf/filterDocQps", kmonitor::QPS, "count");
    return true;
}

RawDocumentReader::ErrorCode RawDocumentReader::read(document::RawDocumentPtr& rawDoc, Checkpoint* checkpoint)
{
    if (!_documentFactoryWrapper) {
        // for test case : make EOF effective
        string docStr;
        DocInfo docInfo(0, INVALID_TIMESTAMP, 0, 0);
        ErrorCode ec = readDocStr(docStr, checkpoint, docInfo);
        if (ec != ERROR_NONE) {
            return ec;
        }

        string errorMsg = string("no document factory wrapper");
        REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg, BS_STOP);
        return ERROR_EXCEPTION;
    }
    if (_batchBuildSize != 1) {
        assert(!_documentFactoryV2);
        rawDoc.reset(_documentFactoryWrapper->CreateMultiMessageRawDocument());
    } else {
        if (_documentFactoryV2) {
            rawDoc.reset(_documentFactoryV2->CreateRawDocument(_tabletSchema).release());
        } else {
            rawDoc.reset(_documentFactoryWrapper->CreateRawDocument());
        }
    }
    if (!rawDoc) {
        string errorMsg = string("create raw document fail!");
        REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg, BS_STOP);
        return ERROR_EXCEPTION;
    }

    int64_t count = 0;
    do {
        ErrorCode ec = read(*rawDoc, checkpoint);
        if (ec != ERROR_NONE) {
            return ec;
        }

        if (!_queryEvaluator) {
            INCREASE_QPS(_readDocQpsMetric);
            fillDocTags(*rawDoc);
            return ERROR_NONE;
        }
        indexlib::document::EvaluatorState ret = _queryEvaluator->Evaluate(rawDoc, _evaluateParam);
        if (ret == ES_TRUE || ret == ES_PENDING) {
            INCREASE_QPS(_readDocQpsMetric);
            fillDocTags(*rawDoc);
            return ERROR_NONE;
        }

        INCREASE_QPS(_filterDocQpsMetric);
        assert(ret == ES_FALSE); // skip current rawDoc
        assert(_batchBuildSize == 1);
        rawDoc.reset(_documentFactoryWrapper->CreateRawDocument());
        if (unlikely(!rawDoc)) {
            string errorMsg = string("create raw document fail!");
            REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg, BS_STOP);
            return ERROR_EXCEPTION;
        }
        ++count;
        // _checkpointStep <= 0 will not generate checkpoint doc
        if (_checkpointStep > 0 && (count % _checkpointStep) == 0) {
            rawDoc->setDocOperateType(CHECKPOINT_DOC);
            return ERROR_NONE;
        }
    } while (true);
    return ERROR_NONE;
}

RawDocumentReader::ErrorCode RawDocumentReader::getNextRawDoc(document::RawDocument& rawDoc, Checkpoint* checkpoint,
                                                              DocInfo& docInfo)
{
    size_t rawDocSize = 0;
    string docStr;
    {
        ScopeLatencyReporter reporter(_readLatencyMetric.get());
        ErrorCode ec = ERROR_NONE;
        if (_batchBuildSize != 1) {
            _msgBuffer.clear();
            ec = readDocStr(_msgBuffer, checkpoint, _batchBuildSize);
            for (const auto& m : _msgBuffer) {
                rawDocSize += m.data.size();
            }
            if (!_msgBuffer.empty()) {
                docInfo.timestamp = _msgBuffer.rbegin()->timestamp;
                docInfo.hashId = _msgBuffer.rbegin()->hashId;
            }
        } else {
            ec = readDocStr(docStr, checkpoint, docInfo);
            rawDocSize = docStr.size();
        }
        if (ec != ERROR_NONE) {
            if (ec == ERROR_EXCEPTION) {
                string errorMsg = string("read document : ") + docStr + " failed";
                REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg, BS_IGNORE);
            }
            return ec;
        }
    }

    REPORT_METRIC(_rawDocSizeMetric, rawDocSize);
    INCREASE_VALUE(_rawDocSizePerSecMetric, rawDocSize);

    {
        ScopeLatencyReporter reporter(_parseLatencyMetric.get());
        bool hasValidDoc = false;
        int64_t msgCount = 0;
        if (_batchBuildSize != 1) {
            msgCount = _msgBuffer.size();
            hasValidDoc = _parser->parseMultiMessage(_msgBuffer, rawDoc);
        } else {
            msgCount = 1;
            indexlibv2::document::IRawDocumentParser::Message msg;
            msg.data = std::move(docStr);
            msg.timestamp = docInfo.timestamp;
            msg.hashId = docInfo.hashId;
            hasValidDoc = _parser->parse(msg, rawDoc);
        }
        uint32_t messageCount = 1;
        const auto& indexDocument = dynamic_cast<indexlib::document::Document*>(&rawDoc);
        if (indexDocument) {
            messageCount = indexDocument->GetMessageCount();
        }
        int64_t docCount = hasValidDoc ? messageCount : 0;
        int64_t parseErrorCount = msgCount - docCount;
        if (parseErrorCount > 0) {
            if (_parseFailCounter) {
                _parseFailCounter->Increase(parseErrorCount);
            }
            INCREASE_VALUE(_parseErrorQpsMetric, parseErrorCount);
        }
        if (!hasValidDoc) {
            return ERROR_PARSE;
        }
    }
    return ERROR_NONE;
}

RawDocumentReader::ErrorCode RawDocumentReader::read(document::RawDocument& rawDoc, Checkpoint* checkpoint)
{
    DocInfo docInfo;
    int64_t timestamp = INVALID_TIMESTAMP;
    ErrorCode ec = ERROR_NONE;

    {
        // add latency metric for readers  which override getNextRawDoc interface
        ScopeLatencyReporter reporter(_getNextDocLatencyMetric.get());
        ec = getNextRawDoc(rawDoc, checkpoint, docInfo);
        timestamp = docInfo.timestamp;
    }

    if (ec != ERROR_NONE) {
        return ec;
    }

    // used to report goc metric
    if (_enableIngestionTimestamp) {
        rawDoc.SetIngestionTimestamp(timestamp);
    }

    if (rawDoc.GetUserTimestamp() != INVALID_TIMESTAMP) {
        timestamp = rawDoc.GetUserTimestamp();
    }

    if (rawDoc.exist(_timestampFieldName)) {
        string reservedTimestampStr = rawDoc.getField(_timestampFieldName);
        int64_t retTimestamp = StringUtil::strToInt64WithDefault(reservedTimestampStr.c_str(), INVALID_TIMESTAMP);
        if (retTimestamp != INVALID_TIMESTAMP) {
            timestamp = retTimestamp;
        }
    }

    int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
    if (timestamp != (int64_t)INVALID_TIMESTAMP) {
        int64_t end2EndLatency = TimeUtility::us2ms(currentTime - timestamp);
        string rawDocSource = rawDoc.getField(_sourceFieldName);
        if (rawDocSource.empty()) {
            rawDocSource = _sourceFieldDefaultValue;
        }
        rawDoc.setDocSource(rawDocSource);
        _e2eLatencyReporter.report(rawDocSource, end2EndLatency);
    } else {
        timestamp = currentTime;
    }

    reportTimestampFieldValue(timestamp);
    rawDoc.setDocTimestamp(timestamp);
    rawDoc.SetDocInfo(docInfo);
    if (!_traceField.empty()) {
        string traceValue = rawDoc.getField(_traceField);
        PkTracer::fromReadTrace(traceValue, checkpoint->offset);
        if (_docCounter) {
            PeriodDocCounterHelper::count(PeriodDocCounterType::Reader, traceValue, _docCounter);
        }
    }
    RawDocument* tmpRawDoc = &rawDoc;
    IE_RAW_DOC_TRACE(tmpRawDoc, "read doc from data source");
    if (!_extendFieldName.empty() && !_extendFieldValue.empty()) {
        rawDoc.setField(_extendFieldName, _extendFieldValue);
    }

    return ERROR_NONE;
}

RawDocumentParser* RawDocumentReader::createRawDocumentParser(const ReaderInitParam& params)
{
    assert(_documentFactoryWrapper);
    ParserCreator parserCreator(_documentFactoryWrapper, params.hologresInterface);
    RawDocumentParser* parser = parserCreator.create(params.kvMap);
    if (parser == NULL) {
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, parserCreator.getLastError(), BS_STOP);
        return NULL;
    }
    return parser;
}

bool RawDocumentReader::initDocumentFactoryV2(const ReaderInitParam& params)
{
    _tabletSchema = params.schemaV2;
    if (!_tabletSchema) {
        return true;
    }
    const auto& tableType = _tabletSchema->GetTableType();
    auto tabletFactoryCreator = indexlibv2::framework::TabletFactoryCreator::GetInstance();
    auto tabletFactory = tabletFactoryCreator->Create(tableType);
    if (!tabletFactory) {
        BS_LOG(ERROR, "create tablet factory with type [%s] failed, registered types [%s]", tableType.c_str(),
               autil::legacy::ToJsonString(tabletFactoryCreator->GetRegisteredType()).c_str());
        return false;
    }
    std::shared_ptr<indexlibv2::config::TabletOptions> options;
    if (params.resourceReader && params.partitionId.clusternames_size() == 1) {
        const auto& clusterName = params.partitionId.clusternames(0);
        options = params.resourceReader->getTabletOptions(clusterName);
    }
    if (!options) {
        options = std::make_shared<indexlibv2::config::TabletOptions>();
    }
    if (!tabletFactory->Init(options, nullptr)) {
        BS_LOG(ERROR, "init tablet factory with type [%s] failed", tableType.c_str());
        return false;
    }
    _documentFactoryV2 = tabletFactory->CreateDocumentFactory(_tabletSchema);
    return true;
}

bool RawDocumentReader::initDocumentFactoryWrapper(const ReaderInitParam& params)
{
    string tableName;
    auto it = params.kvMap.find(RAW_DOCUMENT_SCHEMA_NAME);
    if (it != params.kvMap.end()) {
        tableName = it->second;
    }

    IndexPartitionSchemaPtr schema;
    if (!tableName.empty() && params.resourceReader) {
        schema = params.resourceReader->getSchemaBySchemaTableName(tableName);
    }
    if (!schema) {
        schema = params.schema;
    }
    if (schema) {
        BS_LOG(INFO,
               "RawDocumentReader init DocumentFactoryWrapper "
               "by using raw_doc_schema_name [%s]",
               tableName.c_str());
    } else {
        BS_LOG(WARN, "RawDocumentReader initialize with null schema! ");
    }

    string pluginPath = "";
    if (params.resourceReader) {
        pluginPath = params.resourceReader->getPluginPath();
    }
    _documentFactoryWrapper = DocumentFactoryWrapper::CreateDocumentFactoryWrapper(
        schema, CUSTOMIZED_DOCUMENT_CONFIG_RAWDOCUMENT, pluginPath);
    return _documentFactoryWrapper != nullptr;
}

bool RawDocumentReader::initRawDocQueryEvaluator(const ReaderInitParam& params)
{
    if (_batchBuildSize != 1) {
        return true;
    }
    string enableRawDocQuery = "true";
    enableRawDocQuery = getValueFromKeyValueMap(params.kvMap, READER_ENABLE_RAW_DOCUMENT_QUERY, enableRawDocQuery);
    if (enableRawDocQuery == "false") {
        return true;
    }
    string rawDocQuery;
    rawDocQuery = getValueFromKeyValueMap(params.kvMap, RAW_DOCUMENT_QUERY_STRING, rawDocQuery);
    if (rawDocQuery.empty()) {
        return true;
    }
    assert(_batchBuildSize == 1);
    string rawDocQueryType = "json";
    rawDocQueryType = getValueFromKeyValueMap(params.kvMap, RAW_DOCUMENT_QUERY_TYPE, rawDocQueryType);
    QueryParserPtr queryParser = QueryParserCreator::Create(rawDocQueryType);
    if (!queryParser) {
        BS_LOG(ERROR, "create rawDoc query parser fail, type [%s]", rawDocQueryType.c_str());
        return false;
    }

    QueryBasePtr query = queryParser->Parse(rawDocQuery);
    if (!query) {
        BS_LOG(ERROR, "create rawDoc Query fail, query [%s]", rawDocQuery.c_str());
        return false;
    }

    string pluginPath = "";
    if (params.resourceReader) {
        pluginPath = params.resourceReader->getPluginPath();
    }
    FunctionExecutorResource resource;
    if (params.resourceReader) {
        params.resourceReader->getRawDocumentFunctionResource(resource);
    }

    _evaluatorCreator.reset(new QueryEvaluatorCreator);
    if (!_evaluatorCreator->Init(pluginPath, resource)) {
        BS_LOG(ERROR, "init query evaluator creator fail, pluginPath [%s], resource [%s]!", pluginPath.c_str(),
               ToJsonString(resource).c_str());
        return false;
    }

    _queryEvaluator = _evaluatorCreator->Create(query);
    if (!_queryEvaluator) {
        BS_LOG(ERROR, "create query evaluator fail, query [%s]", rawDocQuery.c_str());
        return false;
    }

    string readerEnableStrictEvaluate = "false";
    readerEnableStrictEvaluate =
        getValueFromKeyValueMap(params.kvMap, READER_ENABLE_STRICT_EVALUATE, readerEnableStrictEvaluate);
    if (readerEnableStrictEvaluate == "true") {
        _evaluateParam.pendingForFieldNotExist = false;
    } else {
        _evaluateParam.pendingForFieldNotExist = true;
    }
    return true;
}

bool RawDocumentReader::initBatchBuild(const ReaderInitParam& params)
{
    if (params.partitionId.clusternames_size() != 1) {
        return true;
    }
    if (params.partitionId.role() == proto::ROLE_PROCESSOR) {
        return true;
    }

    string enableRawDocQuery = "false";
    enableRawDocQuery = getValueFromKeyValueMap(params.kvMap, READER_ENABLE_RAW_DOCUMENT_QUERY, enableRawDocQuery);
    if (enableRawDocQuery == "true") {
        return true;
    }

    const auto& clusterName = params.partitionId.clusternames(0);
    string clusterFileName = params.resourceReader->getClusterConfRelativePath(clusterName);
    config::BuilderConfig builderConfig;
    if (!params.resourceReader->getConfigWithJsonPath(clusterFileName, "build_option_config", builderConfig)) {
        string errorMsg = "get builder config from cluster file[" + clusterFileName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (builderConfig.batchBuildSize != 1) {
        document::RawDocumentPtr rawDoc(_documentFactoryWrapper->CreateMultiMessageRawDocument());
        if (rawDoc) {
            _batchBuildSize = builderConfig.batchBuildSize;
        } else {
            BS_LOG(WARN, "batch build size is [%lu], but document factory do not support multi message doc",
                   builderConfig.batchBuildSize);
        }
    }
    return true;
}

void RawDocumentReader::fillDocTags(document::RawDocument& rawDoc)
{
    if (!_needDocTag) {
        return;
    }
    doFillDocTags(rawDoc);
    rawDoc.AddTag("__print_document_tags__", "true");
}

}} // namespace build_service::reader
