#include "build_service/reader/RawDocumentReader.h"
#include "build_service/reader/ParserCreator.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/common/PkTracer.h"
#include "build_service/util/Monitor.h"
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/util/counter/accumulative_counter.h>
#include <autil/StringUtil.h>
#include <memory>

using namespace std;
using namespace autil;
using namespace build_service::config;
using namespace build_service::common;
using namespace build_service::proto;
using namespace build_service::document;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);

namespace build_service {
namespace reader {

BS_LOG_SETUP(reader, RawDocumentReader);

static string sourceFieldDefaultValue;

RawDocumentReader::RawDocumentReader()
    : _parser(NULL)
    , _hashMapManager(new RawDocumentHashMapManager)
    , _timestampFieldName(HA_RESERVED_TIMESTAMP)
    , _sourceFieldName(HA_RESERVED_SOURCE)
    , _batchBuildSize(1)
{
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);
}

RawDocumentReader::~RawDocumentReader() {
    DELETE_AND_SET_NULL(_parser);
}

void RawDocumentReader::initBeeperTagsFromPartitionId(const proto::PartitionId& pid)
{
    initBeeperTag(pid);
}

bool RawDocumentReader::initialize(const ReaderInitParam &params) {
    _e2eLatencyReporter.init(params.metricProvider);
    if (!initDocumentFactoryWrapper(params))
    {
        string errorMsg = "init document factory wrapper failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!initBatchBuild(params)) {
        string errorMsg = "init batch build size failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    _parser = createRawDocumentParser(params);
    if (!_parser) {
        string errorMsg = "create RawDocumentParser failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!initMetrics(params.metricProvider)) {
        string errorMsg = "reader initMetrics failed";
        REPORT_ERROR_WITH_ADVICE(READER_ERROR_INIT_METRICS, errorMsg, BS_RETRY);
        return false;
    }
    _timestampFieldName = getValueFromKeyValueMap(params.kvMap,
            TIMESTAMP_FIELD, _timestampFieldName);
    _sourceFieldName = getValueFromKeyValueMap(params.kvMap,
            SOURCE_FIELD, _sourceFieldName);
    sourceFieldDefaultValue = getValueFromKeyValueMap(params.kvMap,
            SOURCE_DEFAULT, sourceFieldDefaultValue);
    _traceField = getValueFromKeyValueMap(params.kvMap, TRACE_FIELD);
    _extendFieldName = getValueFromKeyValueMap(params.kvMap, EXTEND_FIELD_NAME);
    _extendFieldValue = getValueFromKeyValueMap(params.kvMap, EXTEND_FIELD_VALUE);

    if (!params.counterMap) {
        BS_LOG(ERROR, "create parse counter failed.");
    }

    _parseFailCounter = GET_ACC_COUNTER(params.counterMap, parseFailCount);
    if (!_parseFailCounter) {
        BS_LOG(ERROR, "create parse counter failed.");
    }
    return init(params);
}

bool RawDocumentReader::init(const ReaderInitParam &params){
    return true;
}

bool RawDocumentReader::initMetrics(MetricProviderPtr metricProvider) {
    _rawDocSizeMetric = DECLARE_METRIC(metricProvider, perf/rawDocSize, kmonitor::GAUGE, "byte");
    _rawDocSizePerSecMetric = DECLARE_METRIC(metricProvider, perf/rawDocSizePerSec, kmonitor::QPS, "byte");
    _readLatencyMetric = DECLARE_METRIC(metricProvider, basic/readLatency, kmonitor::GAUGE, "ms");
    _parseLatencyMetric = DECLARE_METRIC(metricProvider, perf/parseLatency, kmonitor::GAUGE, "ms");
    _parseErrorQpsMetric = DECLARE_METRIC(metricProvider, perf/parseErrorQps, kmonitor::QPS, "count");
    return true;
}

RawDocumentReader::ErrorCode RawDocumentReader::read(
        document::RawDocumentPtr &rawDoc, int64_t &offset)
{
    if (!_documentFactoryWrapper)
    {
        // for test case : make EOF effective
        string docStr;
        int64_t timestamp = INVALID_TIMESTAMP;
        ErrorCode ec = readDocStr(docStr, offset, timestamp);
        if (ec != ERROR_NONE) {
            return ec;
        }

        string errorMsg = string("no document factory wrapper");
        REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg, BS_STOP);
        return ERROR_EXCEPTION;
    }
    if (_batchBuildSize != 1) {
        rawDoc.reset(_documentFactoryWrapper->CreateMultiMessageRawDocument());
    } else {
        rawDoc.reset(_documentFactoryWrapper->CreateRawDocument());
    }
    if (!rawDoc)
    {
        string errorMsg = string("create raw document fail!");
        REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg, BS_STOP);
        return ERROR_EXCEPTION;
    }
    return read(*rawDoc, offset);
}

RawDocumentReader::ErrorCode RawDocumentReader::read(
        document::RawDocument &rawDoc, int64_t &offset)
{
    string docStr;
    int64_t timestamp = INVALID_TIMESTAMP;
    size_t rawDocSize = 0;
    {
        ScopeLatencyReporter reporter(_readLatencyMetric.get());
        ErrorCode ec = ERROR_NONE;
        if (_batchBuildSize != 1) {
            _msgBuffer.clear();
            ec = readDocStr(_msgBuffer, offset, _batchBuildSize);
            for (const auto &m : _msgBuffer) {
                rawDocSize += m.data.size();
            }
            if (!_msgBuffer.empty()) {
                timestamp = _msgBuffer.rbegin()->timestamp;
            }
        } else {
            ec = readDocStr(docStr, offset, timestamp);
            rawDocSize = docStr.size();
        }
        if (ec != ERROR_NONE) {
            if (ec == ERROR_EXCEPTION) {
                string errorMsg = string( "read document : ") + docStr + " failed";
                REPORT_ERROR_WITH_ADVICE(READER_ERROR_READ_EXCEPTION, errorMsg, BS_IGNORE);
            }
            return ec;
        }
    }

    REPORT_METRIC(_rawDocSizeMetric, rawDocSize);
    INCREASE_VALUE(_rawDocSizePerSecMetric, rawDocSize);

    {
        ScopeLatencyReporter reporter(_parseLatencyMetric.get());
        bool ret = false;
        if (_batchBuildSize != 1) {
            ret = _parser->parseMultiMessage(_msgBuffer, rawDoc);
        } else {
            ret = _parser->parse(docStr, rawDoc);
        }
        if (!ret) {
            if (_parseFailCounter) {
                _parseFailCounter->Increase(1);
            }
            INCREASE_VALUE(_parseErrorQpsMetric, 1);
            return ERROR_PARSE;
        }
    }

    if (rawDoc.exist(_timestampFieldName)) {
        string reservedTimestampStr = rawDoc.getField(_timestampFieldName);
        int64_t retTimestamp = StringUtil::strToInt64WithDefault(
                reservedTimestampStr.c_str(), INVALID_TIMESTAMP);
        if (retTimestamp != INVALID_TIMESTAMP) {
            timestamp = retTimestamp;
        }
    }

    int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
    if (timestamp != (int64_t)INVALID_TIMESTAMP) {
        int64_t end2EndLatency = TimeUtility::us2ms(currentTime - timestamp);
        string rawDocSource = rawDoc.getField(_sourceFieldName);
        if (rawDocSource.empty()) {
            rawDocSource = sourceFieldDefaultValue;
        }
        rawDoc.setDocSource(rawDocSource);
        _e2eLatencyReporter.report(rawDocSource, end2EndLatency);
    } else {
        timestamp = currentTime;
    }

    rawDoc.setDocTimestamp(timestamp);
    if (!_traceField.empty()) {
        PkTracer::fromReadTrace(rawDoc.getField(_traceField),
                                offset);
    }
    RawDocument* tmpRawDoc = &rawDoc;
    IE_RAW_DOC_TRACE(tmpRawDoc, "read doc from data source");
    if (!_extendFieldName.empty() && !_extendFieldValue.empty()) {
        rawDoc.setField(_extendFieldName, _extendFieldValue);
    }
    return ERROR_NONE;
}

RawDocumentParser *RawDocumentReader::createRawDocumentParser(const ReaderInitParam &params) {
    assert(_documentFactoryWrapper);
    ParserCreator parserCreator(_documentFactoryWrapper);
    RawDocumentParser* parser = parserCreator.create(params.kvMap);
    if (parser == NULL) {
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, parserCreator.getLastError(), BS_STOP);
        return NULL;
    }
    return parser;
}

bool RawDocumentReader::initDocumentFactoryWrapper(const ReaderInitParam &params) {
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
        BS_LOG(INFO, "RawDocumentReader init DocumentFactoryWrapper "
               "by using raw_doc_schema_name [%s]", tableName.c_str());
    }
    else {
        BS_LOG(WARN, "RawDocumentReader initialize with null schema! ");
    }

    string pluginPath = "";
    if (params.resourceReader)
    {
        pluginPath = params.resourceReader->getPluginPath();
    }
    _documentFactoryWrapper = DocumentFactoryWrapper::CreateDocumentFactoryWrapper(
        schema, CUSTOMIZED_DOCUMENT_CONFIG_RAWDOCUMENT, pluginPath);
    return _documentFactoryWrapper != nullptr;
}

bool RawDocumentReader::initBatchBuild(const ReaderInitParam &params) {
    if (params.partitionId.clusternames_size() != 1) {
        return true;
    }
    if (params.partitionId.role() == proto::ROLE_PROCESSOR) {
        return true;
    }
    const auto &clusterName = params.partitionId.clusternames(0);
    string clusterFileName = params.resourceReader->getClusterConfRelativePath(clusterName);
    config::BuilderConfig builderConfig;
    if (!params.resourceReader->getConfigWithJsonPath(clusterFileName, "build_option_config",
                                                      builderConfig)) {
        string errorMsg = "get builder config from cluster file[" + clusterFileName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (builderConfig.batchBuildSize != 1) {
        document::RawDocumentPtr rawDoc(_documentFactoryWrapper->CreateMultiMessageRawDocument());
        if (rawDoc) {
            _batchBuildSize = builderConfig.batchBuildSize;
        } else {
            BS_LOG(WARN, "batch build size is [%lu], "
                   "but document factory do not support multi message doc",
                   builderConfig.batchBuildSize);
        }
    }
    return true;
}

int64_t RawDocumentReader::getFreshness() {
    return numeric_limits<int64_t>::max();
}

bool RawDocumentReader::getMaxTimestampAfterStartTimestamp(int64_t &timestamp) {
    BS_LOG(WARN, "getMaxTimestamp failed because reader not supported.");
    return false;
}

}
}
