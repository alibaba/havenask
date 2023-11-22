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
#pragma once

#include <mutex>
#include <ostream>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "build_service/common_define.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/io/Output.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/reader/RawDocumentReader.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/metrics/Metric.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace build_service { namespace task_base {

class ExtractDocTask : public Task
{
public:
    ExtractDocTask();
    ~ExtractDocTask();

public:
    static const std::string TASK_NAME;

public:
    bool init(TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override;
    indexlib::util::CounterMapPtr getCounterMap() override;
    std::string getTaskStatus() override;
    bool hasFatalError() override { return false; }

private:
    using DocFields = std::unordered_map<std::string, std::string>;
    using AppendFileds = std::vector<std::pair<std::string /*fieldName*/, std::string /*fieldValue*/>>;

    ExtractDocTask(const ExtractDocTask&);
    ExtractDocTask& operator=(const ExtractDocTask&);

private:
    virtual bool getServiceConfig(config::ResourceReader& resourceReader, config::BuildServiceConfig& serviceConfig);
    template <typename T>
    bool getConfig(config::ResourceReader& resourceReader, const std::string& path, T& config) const;

    std::string getCheckpointFileName();
    bool readCheckpoint(const std::string& generationRoot, config::TaskTarget& checkpoint);
    bool writeCheckpoint(const std::string& generationRoot, const config::TaskTarget& checkpoint);
    bool isTargetEqual(const config::TaskTarget& lhs, const config::TaskTarget& rhs);

private:
    void prepareMetrics();
    bool prepareOutput(KeyValueMap& outputParam, const config::TaskConfig& taskConfig);
    bool prepareReader(const std::string& generationRoot, KeyValueMap& taskParam, const config::TaskTarget& target);
    void waitAllDocCommit();

private:
    struct Checkpoint {
        Checkpoint() : docCount(0), offset(0) {}
        std::string toString()
        {
            std::stringstream ss;
            ss << docCount << ";" << offset;
            return ss.str();
        }

        bool fromString(const std::string& checkpointStr);
        int64_t docCount;
        int64_t offset;
    };

private:
    std::mutex _mutex;
    TaskInitParam _taskInitParam;
    Checkpoint _checkpoint;
    proto::PartitionId _pid;
    config::TaskTarget _currentFinishedTarget;
    reader::RawDocumentReaderPtr _rawDocReader;
    io::OutputPtr _output;
    AppendFileds _appendFields;

    int64_t _docCountLimit;      // -1 means no limit
    size_t _commitInterval;      // unit second
    size_t _lastCommitTimestamp; // unit second

    indexlib::util::MetricProviderPtr _metricProvider;
    indexlib::util::MetricPtr _extractQpsMetric;
    indexlib::util::MetricPtr _commitLatencyMetric;
    indexlib::util::MetricPtr _writeLatencyMetric;
    indexlib::util::MetricPtr _extractDocCountMetric;

private:
    friend class ExtractDocTaskTest;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ExtractDocTask);

///////////////////////////////////////////////////////
template <typename T>
bool ExtractDocTask::getConfig(config::ResourceReader& resourceReader, const std::string& path, T& config) const
{
    if (path.find("://") == std::string::npos) {
        bool isExist = false;
        if (resourceReader.getConfigWithJsonPath(fslib::util::FileUtil::joinFilePath(config::CLUSTER_CONFIG_PATH, path),
                                                 "", config, isExist) &&
            isExist) {
            return true;
        }
        return false;
    } else {
        std::string content;
        if (!fslib::util::FileUtil::readFile(path, content)) {
            BS_LOG(ERROR, "read config file[%s] failed", path.c_str());
            return false;
        }
        try {
            FromJsonString(config, content);
        } catch (const autil::legacy::ExceptionBase& e) {
            BS_LOG(ERROR, "jsonize config failed, original str : [%s]", content.c_str());
            return false;
        }
        return true;
    }
    return false;
}

}} // namespace build_service::task_base
