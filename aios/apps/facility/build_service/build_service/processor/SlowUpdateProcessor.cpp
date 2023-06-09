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
#include "build_service/processor/SlowUpdateProcessor.h"

#include "autil/StringUtil.h"
#include "build_service/util/Monitor.h"
#include "fslib/fslib.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

using namespace build_service::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, SlowUpdateProcessor);

const string SlowUpdateProcessor::PROCESSOR_NAME = "SlowUpdateProcessor";
const string SlowUpdateProcessor::REALTIME_START_TIMESTAMP = "realtime_start_timestamp";
const string SlowUpdateProcessor::FILTER_KEY = "filter_key";
const string SlowUpdateProcessor::FILTER_VALUE = "filter_value";
const string SlowUpdateProcessor::ACTION = "action";

const string SlowUpdateProcessor::ACTION_SKIP_ALL = "skip_all";
const string SlowUpdateProcessor::ACTION_SKIP_REALTIME = "skip_realtime";

const string SlowUpdateProcessor::CONFIG_PATH = "config_path";
const string SlowUpdateProcessor::SYNC_INTERVAL = "sync_interval";

using Executor = SlowUpdateProcessor::ExecuteProcessor;

void SlowUpdateProcessor::Config::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("timestamp", timestamp, timestamp);
    json.Jsonize("begin_ts", beginTs, beginTs);

    if (json.GetMode() == FROM_JSON) {
        json.Jsonize(SlowUpdateProcessor::FILTER_KEY, filterKey, filterKey);
        if (!filterKey.empty()) {
            json.Jsonize(SlowUpdateProcessor::FILTER_VALUE, filterValue);
        }

        string actionStr = SlowUpdateProcessor::ACTION_SKIP_REALTIME;
        json.Jsonize(SlowUpdateProcessor::ACTION, actionStr, actionStr);
        action = SlowUpdateProcessor::str2Action(actionStr);

    } else {
        if (!filterKey.empty()) {
            json.Jsonize(SlowUpdateProcessor::FILTER_KEY, filterKey);
            json.Jsonize(SlowUpdateProcessor::FILTER_VALUE, filterValue);
        }
        auto actionStr = SlowUpdateProcessor::action2Str(action);
        json.Jsonize(SlowUpdateProcessor::ACTION, actionStr);
    }
}

SlowUpdateProcessor::ActionType SlowUpdateProcessor::str2Action(const std::string& str)
{
    if (str == ACTION_SKIP_ALL) {
        return ActionType::SKIP_ALL;
    } else if (str == ACTION_SKIP_REALTIME) {
        return ActionType::SKIP_REALTIME;
    } else {
        return ActionType::UNKNOWN;
    }
}

std::string SlowUpdateProcessor::action2Str(SlowUpdateProcessor::ActionType action)
{
    if (action == ActionType::SKIP_ALL) {
        return ACTION_SKIP_ALL;
    } else if (action == ActionType::SKIP_REALTIME) {
        return ACTION_SKIP_REALTIME;
    } else {
        return "unknown";
    }
}

SlowUpdateProcessor::SlowUpdateProcessor(bool autoSync)
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
    , _rwLock(autil::ReadWriteLock::PREFER_WRITER)
    , _autoSync(autoSync)
{
}

SlowUpdateProcessor::~SlowUpdateProcessor() { _syncThread.reset(); }

bool SlowUpdateProcessor::init(const DocProcessorInitParam& param)
{
    _configPath = getValueFromKeyValueMap(param.parameters, CONFIG_PATH);
    if (_configPath.empty()) {
        BS_LOG(WARN, "%s is empty, slow update disabled", CONFIG_PATH.c_str());
        return true;
    }
    if (!_autoSync) {
        BS_LOG(INFO, "auto sync off");
        return true;
    }
    int64_t syncInterval = DEFAULT_SYNC_INTERVAL * 1000 * 1000; // s -> us
    auto syncIntervalStr = getValueFromKeyValueMap(param.parameters, SYNC_INTERVAL);
    if (!syncIntervalStr.empty()) {
        if (!StringUtil::fromString(syncIntervalStr, syncInterval)) {
            BS_LOG(ERROR, "invalid %s [%s]", SYNC_INTERVAL.c_str(), syncIntervalStr.c_str());
            return false;
        }
    }
    if (param.metricProvider) {
        _skipQpsMetric = DECLARE_METRIC(param.metricProvider, "basic/skipQps", kmonitor::QPS, "count");
    }
    _syncThread = LoopThread::createLoopThread(std::bind(&SlowUpdateProcessor::syncConfig, this), syncInterval,
                                               "SlowUpdateProcessor");
    if (!_syncThread) {
        BS_LOG(ERROR, "start sync config thread failed");
        return false;
    }

    BS_LOG(INFO, "SlowUpdateProcessor inited, config path [%s], sync interval [%ld]us", _configPath.c_str(),
           syncInterval);
    return true;
}

SlowUpdateProcessor* SlowUpdateProcessor::clone()
{
    assert(false);
    return nullptr;
}

Executor* SlowUpdateProcessor::allocate()
{
    auto ret = new Executor(*this);
    return ret;
}

void SlowUpdateProcessor::syncConfig()
{
    if (_configPath.empty()) {
        return;
    }
    string content;
    auto ec = FileSystem::readFile(_configPath, content);
    Config newConfig;
    if (ec == EC_OK) {
        try {
            FromJsonString(newConfig, content);
            if (newConfig.action == ActionType::UNKNOWN) {
                BS_LOG(ERROR,
                       "invalid SlowUpdateConfig [%s], this action not supported, slow update "
                       "goes to default setting",
                       content.c_str());
                newConfig.reset();
            }
        } catch (const autil::legacy::ExceptionBase& e) {
            BS_LOG(ERROR,
                   "deserialize SlowUpdateConfig [%s] failed, slow update goes to default "
                   "setting, errorMsg [%s]",
                   content.c_str(), e.what());
            newConfig.reset();
        }
    } else if (ec == EC_NOENT) {
        BS_LOG(INFO, "config [%s] not exist, slow update config goes to default setting", _configPath.c_str());
    } else {
        BS_LOG(ERROR, "read config [%s] failed, error [%d], slow update config goes to default setting",
               _configPath.c_str(), ec);
    }

    if (newConfig != _config) {
        ScopedWriteLock l(_rwLock);
        _config = newConfig;
        BS_LOG(INFO, "slow update config updated to [%s]", content.c_str());
    }
}

bool SlowUpdateProcessor::process(const ExtendDocumentPtr& document)
{
    Executor executor(*this);
    return executor.process(document);
}

void SlowUpdateProcessor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    Executor executor(*this);
    for (size_t i = 0; i < docs.size(); i++) {
        if (!executor.process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void SlowUpdateProcessor::destroy() { delete this; }

bool Executor::process(const ExtendDocumentPtr& document)
{
    const ProcessedDocumentPtr& processedDocPtr = document->getProcessedDocument();
    const RawDocumentPtr& rawDocCumentPtr = document->getRawDocument();

    auto docTs = rawDocCumentPtr->getDocTimestamp();
    // if doc ts is greater than end ts, dont skip it
    if (docTs >= _config.timestamp) {
        return true;
    }
    // if begin ts is less than begin ts, dont skip it
    if (docTs <= _config.beginTs) {
        return true;
    }

    if (!_config.filterKey.empty()) {
        // if has condition, filter by condition
        for (size_t i = 0; i < _config.filterKey.size(); i++) {
            const auto& value = rawDocCumentPtr->getField(_config.filterKey[i]);
            if (_config.filterValue[i] != value) {
                return true;
            }
        }
    }
    if (_config.action == ActionType::SKIP_REALTIME) {
        ProcessedDocument::DocClusterMetaVec metas = processedDocPtr->getDocClusterMetaVec();
        for (size_t i = 0; i < metas.size(); i++) {
            metas[i].buildType |= ProcessedDocument::SWIFT_FILTER_BIT_REALTIME;
        }
        processedDocPtr->setDocClusterMetaVec(metas);
    } else if (_config.action == ActionType::SKIP_ALL) {
        rawDocCumentPtr->setDocOperateType(SKIP_DOC);
    } else {
        assert(false);
        BS_LOG(WARN, "unkwown action");
        return true;
    }
    INCREASE_QPS(_metric);
    return true;
}

void Executor::batchProcess(const vector<ExtendDocumentPtr>& docs)
{
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

Executor* Executor::clone() { return new Executor(*this); }

}} // namespace build_service::processor
