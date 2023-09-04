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
#include "build_service_tasks/extract_doc/ExtractDocTask.h"

#include <mutex>

#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/document/RawDocument.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/reader/MultiIndexDocReader.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/MemUtil.h"
#include "build_service/util/RangeUtil.h"

using namespace std;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::reader;
using namespace build_service::util;
using namespace build_service::io;
using namespace autil::legacy;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::partition;
using namespace indexlib::config;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, ExtractDocTask);

const string ExtractDocTask::TASK_NAME = BS_EXTRACT_DOC;

ExtractDocTask::ExtractDocTask() : _docCountLimit(-1), _commitInterval(60), _lastCommitTimestamp(0) {}

ExtractDocTask::~ExtractDocTask() {}

bool ExtractDocTask::init(TaskInitParam& initParam)
{
    _taskInitParam = initParam;
    _pid = initParam.pid;
    if (_pid.clusternames_size() == 0) {
        string errorMsg = "partition id has no cluster name";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_RETRY);
    }
    _metricProvider = _taskInitParam.metricProvider;
    prepareMetrics();
    return true;
}

void ExtractDocTask::prepareMetrics()
{
    _extractDocCountMetric = DECLARE_METRIC(_metricProvider, "extractDocCount", kmonitor::STATUS, "count");
    _extractQpsMetric = DECLARE_METRIC(_metricProvider, "extractQps", kmonitor::QPS, "count");
    _commitLatencyMetric = DECLARE_METRIC(_metricProvider, "commitLatency", kmonitor::GAUGE, "ms");
    _writeLatencyMetric = DECLARE_METRIC(_metricProvider, "writeLatency", kmonitor::GAUGE, "ms");
}

string ExtractDocTask::getCheckpointFileName()
{
    string roleId;
    if (!proto::ProtoUtil::partitionIdToRoleId(_taskInitParam.pid, roleId)) {
        return "";
    }
    return string("task.extract_doc.") + roleId + ".instanceId_" +
           StringUtil::toString(_taskInitParam.instanceInfo.instanceId) + ".checkpoint";
}

bool ExtractDocTask::readCheckpoint(const std::string& generationRoot, config::TaskTarget& checkpoint)
{
    auto checkpointFileName = getCheckpointFileName();
    if (checkpointFileName.empty()) {
        return false;
    }
    string checkpointFile = fslib::util::FileUtil::joinFilePath(generationRoot, checkpointFileName);
    string content;
    if (!fslib::util::FileUtil::readWithBak(checkpointFile, content)) {
        BS_LOG(ERROR, "read checkpoint file[%s] failed", checkpointFile.c_str());
        return false;
    }
    if (content.empty()) {
        BS_LOG(INFO, "checkpoint file[%s] does not exist", checkpointFile.c_str());
        return true;
    } else {
        config::TaskTarget tmp;
        try {
            FromJsonString(tmp, content);
        } catch (const ExceptionBase& e) {
            BS_LOG(ERROR, "jsonize targetDescription failed, original str : [%s]", content.c_str());
            return false;
        }
        checkpoint = std::move(tmp);
    }
    return true;
}

bool ExtractDocTask::writeCheckpoint(const std::string& generationRoot, const config::TaskTarget& checkpoint)
{
    auto checkpointFileName = getCheckpointFileName();
    if (checkpointFileName.empty()) {
        return false;
    }
    string checkpointFile = fslib::util::FileUtil::joinFilePath(generationRoot, checkpointFileName);
    return fslib::util::FileUtil::writeWithBak(checkpointFile, ToJsonString(checkpoint));
}

bool ExtractDocTask::Checkpoint::fromString(const std::string& checkpointStr)
{
    vector<string> strs;
    autil::StringUtil::fromString(checkpointStr, strs, ";");
    if (strs.size() != 2) {
        return false;
    }
    if (!autil::StringUtil::fromString(strs[0], docCount)) {
        docCount = offset = 0;
        return false;
    }
    if (!autil::StringUtil::fromString(strs[1], offset)) {
        docCount = offset = 0;
        return false;
    }
    return true;
}

bool ExtractDocTask::handleTarget(const config::TaskTarget& target)
{
    if (isDone(target)) {
        BS_INTERVAL_LOG(300, INFO, "task has been done");
        return true;
    }
    BuildServiceConfig serviceConfig;
    if (!getServiceConfig(*(_taskInitParam.resourceReader), serviceConfig)) {
        BS_LOG(ERROR, "get service config failed");
        return false;
    }
    string indexRoot = serviceConfig.getIndexRoot();
    auto generationRoot = IndexPathConstructor::constructGenerationPath(indexRoot, _pid);

    config::TaskTarget checkpoint;
    if (!readCheckpoint(generationRoot, checkpoint)) {
        BS_LOG(ERROR, "read checkpoint failed");
        return false;
    }
    if (isTargetEqual(checkpoint, target)) {
        BS_INTERVAL_LOG(300, INFO, "task has been done");
        std::lock_guard<std::mutex> lg(_mutex);
        _currentFinishedTarget = target;
        return true;
    }

    // Do Task

    string checkpointStr;
    if (!target.getTargetDescription(BS_EXTRACT_DOC_CHECKPOINT, checkpointStr)) {
        BS_LOG(ERROR, "get check point failed");
    } else {
        std::lock_guard<std::mutex> lg(_mutex);
        _checkpoint.fromString(checkpointStr);
    }
    string taskParamStr;
    if (!target.getTargetDescription("task_param", taskParamStr)) {
        BS_LOG(ERROR, "get task param failed from [%s]", ToJsonString(target).c_str());
        return false;
    }
    KeyValueMap taskParam;
    try {
        FromJsonString(taskParam, taskParamStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "parse task param [%s] failed, exception [%s]", taskParamStr.c_str(), e.what());
        return false;
    }
    if (!prepareReader(generationRoot, taskParam, target)) {
        return false;
    }

    if (!_rawDocReader->seek(reader::Checkpoint(_checkpoint.offset, /*userData=*/""))) {
        BS_LOG(ERROR, "seek to checkpoint [%ld] failed", _checkpoint.offset);
        return false;
    }

    // prepare output
    string taskConfigPath;
    if (!target.getTaskConfigPath(taskConfigPath)) {
        BS_LOG(ERROR, "get task config path failed");
        return false;
    }

    TaskConfig taskConfig;
    bool isExist = false;
    string taskConfigDir = taskConfigPath.substr(0, taskConfigPath.find("clusters"));
    if (!_taskInitParam.resourceReader->getTaskConfigWithJsonPath(taskConfigDir, _taskInitParam.clusterName, TASK_NAME,
                                                                  "", taskConfig, isExist) ||
        !isExist) {
        BS_LOG(ERROR, "get task config with json path[%s] failed", taskConfigPath.c_str());
        return false;
    }
    if (!taskConfig.getTaskParam("commit_interval", _commitInterval)) {
        _commitInterval = 60;
    }
    if (!prepareOutput(taskParam, taskConfig)) {
        return false;
    }

    // TODO: (yiping.typ) add retry logic when read/write has something wrong
    int64_t offset = 0;
    Checkpoint currentCheckpoint = _checkpoint;
    while (true) {
        if (_docCountLimit > 0 && currentCheckpoint.docCount >= _docCountLimit) {
            break;
        }
        document::RawDocumentPtr rawDoc;
        reader::Checkpoint checkpoint;
        RawDocumentReader::ErrorCode ec = _rawDocReader->read(rawDoc, &checkpoint);
        if (ec == RawDocumentReader::ERROR_EOF) {
            BS_LOG(INFO, "extract doc task read eof, done");
            break;
        }
        if (ec != RawDocumentReader::ERROR_NONE) {
            return false;
        }
        currentCheckpoint.docCount++;
        currentCheckpoint.offset = offset;
        {
            indexlib::util::ScopeLatencyReporter scopeTime(_writeLatencyMetric.get());
            indexlib::document::RawDocFieldIteratorPtr iterator(rawDoc->CreateIterator());

            // extrcat raw doc
            DocFields docFields;
            while (iterator->IsValid()) {
                string fieldName = iterator->GetFieldName().to_string();
                string fieldValue = iterator->GetFieldValue().to_string();
                docFields[fieldName] = fieldValue;
                iterator->MoveToNext();
            }

            // append fields defined by user
            for (const auto& field : _appendFields) {
                docFields[field.first] = field.second;
            }

            Any any = docFields;
            if (!_output->write(any)) {
                BS_LOG(ERROR, "process doc output failed");
                return false;
            }
        }

        INCREASE_QPS(_extractQpsMetric);
        REPORT_METRIC(_extractDocCountMetric, currentCheckpoint.docCount);
        int64_t currentTime = TimeUtility::currentTimeInSeconds();
        if (currentTime - _lastCommitTimestamp > _commitInterval) {
            waitAllDocCommit();
            _lastCommitTimestamp = currentTime;
            std::lock_guard<std::mutex> lg(_mutex);
            _checkpoint = currentCheckpoint;
        }
    }
    {
        waitAllDocCommit();
        std::lock_guard<std::mutex> lg(_mutex);
        _checkpoint = currentCheckpoint;
    }
    if (!writeCheckpoint(generationRoot, target)) {
        BS_LOG(ERROR, "commit checkpoint failed");
        return false;
    }

    std::lock_guard<std::mutex> lg(_mutex);
    _currentFinishedTarget = target;
    return true;
}

bool ExtractDocTask::prepareReader(const std::string& generationRoot, KeyValueMap& taskParam,
                                   const config::TaskTarget& target)
{
    // prepare input reader
    ReaderInitParam readerInitParam;
    ResourceReader* resourceReader = new ResourceReader(_taskInitParam.resourceReader->getConfigPath());
    resourceReader->init();
    readerInitParam.metricProvider = _taskInitParam.metricProvider;
    readerInitParam.resourceReader.reset(resourceReader);
    readerInitParam.range = _pid.range();
    readerInitParam.schema = resourceReader->getSchema(_taskInitParam.clusterName);
    readerInitParam.partitionId = _pid;

    KeyValueMap& kvMap = readerInitParam.kvMap;
    kvMap["src"] = "index_document";
    kvMap["type"] = "index_document";
    kvMap[READ_INDEX_ROOT] = generationRoot;
    kvMap[PREFER_SOURCE_INDEX] = "true";
    if (taskParam["prefer_source_index"] == "false") {
        kvMap[PREFER_SOURCE_INDEX] = "false";
    }
    kvMap[READ_IGNORE_ERROR] = "false";
    kvMap[USER_DEFINE_INDEX_PARAM] = taskParam["query"];
    kvMap[READ_INDEX_CACHE_SIZE] = "1024";
    kvMap[USER_REQUIRED_FIELDS] = taskParam["required_fields"];
    auto iter = taskParam.find("extract_doc_count");
    if (iter != taskParam.end()) {
        int64_t docCountLimit;
        if (autil::StringUtil::fromString(iter->second, docCountLimit)) {
            _docCountLimit = docCountLimit;
        }
    }

    versionid_t versionId = INVALID_VERSION;
    if (!target.getTargetDescription(BS_SNAPSHOT_VERSION, versionId)) {
        BS_LOG(ERROR, "fail to get snapshot version");
        return false;
    }
    kvMap[READ_INDEX_VERSION] = autil::StringUtil::toString(versionId);

    _rawDocReader.reset(new MultiIndexDocReader());
    if (!_rawDocReader->initialize(readerInitParam)) {
        BS_LOG(ERROR, "initialize raw document reader failed");
        return false;
    }
    return true;
}

bool ExtractDocTask::prepareOutput(KeyValueMap& taskParam, const TaskConfig& taskConfig)
{
    string outputParamStr = taskParam["output_param"];
    KeyValueMap outputParam;
    try {
        FromJsonString(outputParam, outputParamStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "parse output param [%s] failed, exception [%s]", outputParamStr.c_str(), e.what());
        return false;
    }
    string outputName = outputParam["output_name"];
    if (outputName.empty()) {
        BS_LOG(ERROR, "expected non-empty outputName in output param");
        return false;
    }

    string appendFieldsStr = taskParam["append_fields"];
    if (!appendFieldsStr.empty()) {
        try {
            _appendFields.clear();
            map<string, string> appendFields;
            FromJsonString(appendFields, appendFieldsStr);
            for (auto& field : appendFields) {
                _appendFields.emplace_back(field.first, field.second);
            }
        } catch (const autil::legacy::ExceptionBase& e) {
            BS_LOG(ERROR,
                   "extract doc task parse appendFieldsStr[%s] failed,"
                   "exception [%s], will not use it",
                   appendFieldsStr.c_str(), e.what());
        }
    }

    const std::map<std::string, TaskOutputConfig>& outputConfigs = taskConfig.getOutputConfigs();
    auto configIter = outputConfigs.find(outputName);
    if (configIter == outputConfigs.end()) {
        BS_LOG(ERROR, "no output config [%s] in outputConfigs", outputName.c_str());
        return false;
    }
    TaskOutputConfig outputConfig = configIter->second;
    for (auto& param : outputParam) {
        outputConfig.addParameters(param.first, param.second);
    }
    auto ranges = RangeUtil::splitRange(0, 65535, _taskInitParam.instanceInfo.instanceCount);
    auto range = ranges[_taskInitParam.instanceInfo.instanceId];
    string rangeStr = std::to_string(range.from()) + "_" + std::to_string(range.to());
    outputConfig.addParameters("range_str", rangeStr);

    const OutputCreatorMap& outputCreators = _taskInitParam.outputCreators;
    auto creatorIter = outputCreators.find(outputName);
    if (creatorIter == outputCreators.end()) {
        BS_LOG(ERROR, "no output creator [%s] in outputCreators", outputName.c_str());
        return false;
    }
    OutputCreatorPtr outputCreator = creatorIter->second;
    _output = outputCreator->create(outputConfig.getParameters());
    if (!_output) {
        BS_LOG(ERROR, "create output [%s] failed", outputName.c_str());
        return false;
    }
    return true;
}

void ExtractDocTask::waitAllDocCommit()
{
    indexlib::util::ScopeLatencyReporter scopeTime(_commitLatencyMetric.get());
    BS_LOG(INFO, "waiting output commited begin");
    while (!_output->commit()) {
        usleep(50 * 1000);
    }
    BS_LOG(INFO, "waiting output commited end");
}

bool ExtractDocTask::isDone(const config::TaskTarget& target)
{
    std::lock_guard<std::mutex> lg(_mutex);
    return isTargetEqual(target, _currentFinishedTarget);
}

bool ExtractDocTask::isTargetEqual(const config::TaskTarget& lhs, const config::TaskTarget& rhs)
{
    config::TaskTarget lhsCopy = lhs;
    config::TaskTarget rhsCopy = rhs;
    lhsCopy.removeTargetDescription(BS_EXTRACT_DOC_CHECKPOINT);
    rhsCopy.removeTargetDescription(BS_EXTRACT_DOC_CHECKPOINT);
    return lhsCopy == rhsCopy;
}

indexlib::util::CounterMapPtr ExtractDocTask::getCounterMap() { return indexlib::util::CounterMapPtr(); }

string ExtractDocTask::getTaskStatus()
{
    std::lock_guard<std::mutex> lock(_mutex);
    config::TaskTarget current;
    if (_rawDocReader && _output) {
        string checkpointStr = _checkpoint.toString();
        current.updateTargetDescription(BS_EXTRACT_DOC_CHECKPOINT, checkpointStr);
    }
    return ToJsonString(current);
}

bool ExtractDocTask::getServiceConfig(config::ResourceReader& resourceReader, config::BuildServiceConfig& serviceConfig)
{
    if (!resourceReader.getConfigWithJsonPath("build_app.json", "", serviceConfig)) {
        BS_LOG(ERROR, "failed to get build service config");
        return false;
    }
    return true;
}

}} // namespace build_service::task_base
