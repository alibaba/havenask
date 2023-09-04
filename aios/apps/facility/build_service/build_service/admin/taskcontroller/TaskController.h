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
#ifndef ISEARCH_BS_TASKCONTROLLER_H
#define ISEARCH_BS_TASKCONTROLLER_H

#include "autil/legacy/jsonizable.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class TaskController : public autil::legacy::Jsonizable
{
public:
    struct Node {
        Node() {}
        config::TaskTarget taskTarget;
        std::string statusDescription;
        std::string roleName;
        uint32_t nodeId = -1;
        uint32_t instanceIdx = 0;
        uint32_t sourceNodeId = -1;
        uint32_t backupId = -1;
        bool reachedTarget = false;
    };
    typedef std::vector<Node> Nodes;

public:
    TaskController(const std::string& taskId, const std::string& taskName, const TaskResourceManagerPtr& resMgr)
        : _taskId(taskId)
        , _taskName(taskName)
        , _resourceManager(resMgr)
        , _beeperTags(new beeper::EventTags)
    {
    }
    TaskController(const TaskController& other)
        : _taskId(other._taskId)
        , _taskName(other._taskName)
        , _resourceManager(other._resourceManager)
        , _beeperTags(other._beeperTags)
    {
    }
    virtual ~TaskController() = default;

private:
    TaskController& operator=(const TaskController&);

public:
    static bool getTaskConfig(const std::string& taskFileConfigPath, const std::string& taskName,
                              config::TaskConfig& taskConfig);
    virtual TaskController* clone() = 0;
    virtual bool init(const std::string& clusterName, const std::string& taskFileConfigPath,
                      const std::string& initParam) = 0;
    virtual const std::string& getTaskName() const = 0;
    virtual bool start(const KeyValueMap& kvMap);
    virtual bool finish(const KeyValueMap& kvMap) { return true; }
    virtual bool operate(Nodes& nodes) = 0;
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) = 0;
    virtual bool updateConfig() = 0;
    virtual bool operator==(const TaskController&) const = 0;
    virtual bool operator!=(const TaskController&) const = 0;
    virtual std::string getTaskInfo() { return ""; }
    virtual void supplementLableInfo(KeyValueMap& info) const {}
    virtual std::string getUsingConfigPath() const = 0;
    virtual void setBeeperTags(const beeper::EventTagsPtr beeperTags);
    virtual void notifyStopped() {}
    virtual bool isSupportBackup() const { return false; }
    const std::map<std::string, std::string>& GetPropertyMap() const { return _propertyMap; }

    void SetProperty(const std::string& key, const std::string& value) { _propertyMap[key] = value; }

    bool GetProperty(const std::string& key, std::string& value) const
    {
        auto it = _propertyMap.find(key);
        if (it == _propertyMap.end()) {
            return false;
        }
        value = it->second;
        return true;
    }

protected:
    std::string _taskId;
    std::string _taskName;
    TaskResourceManagerPtr _resourceManager;
    beeper::EventTagsPtr _beeperTags;
    std::map<std::string, std::string> _propertyMap;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskController);

}} // namespace build_service::admin

#endif // ISEARCH_BS_TASKCONTROLLER_H
