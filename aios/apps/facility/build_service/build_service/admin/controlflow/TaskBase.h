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
#ifndef ISEARCH_BS_CONTROL_FLOW_TASKBASE_H
#define ISEARCH_BS_CONTROL_FLOW_TASKBASE_H

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/admin/WorkerTable.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/TaskOptimizer.h"
#include "build_service/common_define.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"
#include "kmonitor_adapter/Monitor.h"

namespace build_service { namespace admin {

class TaskBase : public autil::legacy::Jsonizable
{
public:
    class TaskMeta : public autil::legacy::Jsonizable
    {
    public:
        TaskMeta() : _beginTime(0), _endTime(0) {}

        TaskMeta(const std::string& id, const std::string& type, const KeyValueMap& pmap, int64_t beginTime,
                 int64_t endTime)
            : _taskId(id)
            , _taskType(type)
            , _propertyMap(pmap)
            , _beginTime(beginTime)
            , _endTime(endTime)
        {
        }

        const std::string& getTaskId() const { return _taskId; }
        const std::string& getTaskType() const { return _taskType; }
        const KeyValueMap& getPropertyMap() const { return _propertyMap; }
        int64_t getBeginTime() const { return _beginTime; }
        int64_t getEndTime() const { return _endTime; }

        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    private:
        std::string _taskId;
        std::string _taskType;
        KeyValueMap _propertyMap;
        int64_t _beginTime;
        int64_t _endTime;
    };
    BS_TYPEDEF_PTR(TaskMeta);

public:
    enum TaskStatus {
        ts_unknown = 0,
        ts_init = 1,
        ts_running = 2,
        ts_finishing = 3,
        ts_finish = 4,
        ts_stopping = 5,
        ts_stopped = 6,
        ts_error = 7,
        ts_suspended = 8,
        ts_suspending = 9
    };

public:
    TaskBase() : _beginTime(0), _endTime(0) {}

    TaskBase(const std::string& id, const std::string& type, const TaskResourceManagerPtr& resMgr);
    virtual ~TaskBase();

public:
    const std::string& getIdentifier() const { return _taskId; }
    virtual std::string getNodesIdentifier() const { return _taskId; }
    const std::string& getTaskType() const { return _taskType; }

    virtual TaskBase* clone() = 0;
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) = 0;
    virtual bool updateConfig() = 0;
    virtual void accept(const TaskOptimizerPtr& optimizer, TaskOptimizer::OptimizeStep step);
    virtual void doAccept(const TaskOptimizerPtr& optimizer, TaskOptimizer::OptimizeStep step);

    bool init(const KeyValueMap& kvMap);
    void syncTaskProperty(proto::WorkerNodes& workerNodes);
    bool start(const KeyValueMap& kvMap);
    bool finish(const KeyValueMap& kvMap);
    bool stop();
    bool suspend();
    bool resume();

    int64_t getEndTime() const { return _endTime; }
    virtual bool executeCmd(const std::string& cmdName, const KeyValueMap& params) { return true; }
    virtual bool isFinished(proto::WorkerNodes& workerNodes) = 0;
    virtual bool waitSuspended(proto::WorkerNodes& workerNodes);
    virtual bool doInit(const KeyValueMap& kvMap) = 0;
    virtual bool doSuspend();
    virtual bool doResume();
    virtual bool doFinish(const KeyValueMap& kvMap) = 0;
    virtual bool doStart(const KeyValueMap& kvMap);
    virtual void doSyncTaskProperty(proto::WorkerNodes& workerNodes) = 0;
    bool isTaskFinishing() const;
    bool isTaskFinish() const;
    bool isTaskStopping() const;
    bool isTaskStopped() const;
    bool isTaskRunning() const;
    bool isTaskError() const;
    bool isTaskSuspended() const;
    bool isTaskSuspending() const;

    const std::string& getProperty(const std::string& key) const;
    bool setProperty(const std::string& key, const std::string& value);

    virtual void setTaskStatus(TaskStatus stat);
    TaskStatus getTaskStatus() const;

    TaskMetaPtr createTaskMeta() const;
    void setPropertyMap(const KeyValueMap& propMap) { _propertyMap = propMap; }
    std::string getTaskLabelString(KeyValueMap* outputDetailInfo = nullptr) const;
    void setCycleTime(int64_t beginTime, int64_t endTime);

public:
    static std::string taskStatus2Str(TaskStatus stat);
    static TaskStatus str2TaskStatus(const std::string& str);
    static const std::string TASK_STATUS_PROPERTY_KEY;
    static const std::string LAST_TASK_STATUS_PROPERTY_KEY;

protected:
    virtual bool CheckKeyAuthority(const std::string& key);
    bool setPropertyByAuthority(const std::string& key, const std::string& value, bool luaUser);
    void setLastTaskStatus(TaskStatus stat);
    TaskStatus getLastTaskStatus() const;
    virtual void supplementLableInfo(KeyValueMap& info) const {}
    std::string getDateTsString(int64_t tsInSecond) const;
    void filterLabelInfo(KeyValueMap& info, bool isSummary) const;

protected:
    std::string _taskId;
    std::string _taskType;
    KeyValueMap _propertyMap;
    TaskResourceManagerPtr _resourceManager;
    int64_t _beginTime;
    int64_t _endTime;
    kmonitor::MetricsTags _kmonTags;

protected:
    bool getValueFromKVMap(const KeyValueMap& kvMap, const std::string& key, std::string& value);

private:
    friend class TaskWrapper;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskBase);
}} // namespace build_service::admin

#endif //__ISEARCH_BS_CONTROL_FLOW_TASKBASE_H
