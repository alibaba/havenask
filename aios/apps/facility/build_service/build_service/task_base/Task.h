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
#ifndef ISEARCH_BS_TASK_H
#define ISEARCH_BS_TASK_H

#include "beeper/common/common_type.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/io/InputCreator.h"
#include "build_service/io/OutputCreator.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace build_service { namespace task_base {

using InputCreatorMap = std::unordered_map<std::string, io::InputCreatorPtr>;
using OutputCreatorMap = std::unordered_map<std::string, io::OutputCreatorPtr>;

class Task : public proto::ErrorCollector
{
public:
    Task();
    virtual ~Task();

private:
    Task(const Task&);
    Task& operator=(const Task&);

public:
    struct InstanceInfo {
        InstanceInfo() : partitionCount(0), partitionId(0), instanceCount(0), instanceId(0) {}
        uint32_t partitionCount;
        uint32_t partitionId;
        uint32_t instanceCount;
        uint32_t instanceId;
    };

    struct BuildId {
        std::string appName;
        std::string dataTable;
        generationid_t generationId;
    };

    struct TaskInitParam {
        TaskInitParam() : resourceReader(NULL), metricProvider(NULL) {}
        InstanceInfo instanceInfo;
        BuildId buildId;
        proto::PartitionId pid;
        std::string clusterName;
        std::string appZkRoot;
        std::shared_ptr<config::ResourceReader> resourceReader;
        indexlib::util::MetricProviderPtr metricProvider;
        InputCreatorMap inputCreators;
        OutputCreatorMap outputCreators;
        std::string epochId;
        proto::LongAddress address;
    };

public:
    virtual bool init(TaskInitParam& initParam) = 0;
    virtual bool handleTarget(const config::TaskTarget& target) = 0;
    virtual bool isDone(const config::TaskTarget& target) = 0;
    virtual indexlib::util::CounterMapPtr getCounterMap() = 0;
    virtual std::string getTaskStatus() = 0;
    virtual proto::ProgressStatus getTaskProgress() const;

    virtual bool hasFatalError() = 0;
    virtual void handleFatalError() { handleFatalError("fatal error exit"); }
    void initBeeperTags(const Task::TaskInitParam& param);

protected:
    virtual void handleFatalError(const std::string& fatalErrorMsg);

protected:
    beeper::EventTagsPtr _beeperTags;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(Task);
////////////////////////////////////////////////
}} // namespace build_service::task_base

#endif // ISEARCH_BS_TASK_H
