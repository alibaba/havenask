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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

extern "C" {
#include "lua.h"
}

#include <map>
#include <set>

#include "autil/Lock.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"

BS_DECLARE_REFERENCE_CLASS(admin, FlowContainer);
BS_DECLARE_REFERENCE_CLASS(admin, TaskResourceManager);
BS_DECLARE_REFERENCE_CLASS(admin, TaskContainer);
BS_DECLARE_REFERENCE_CLASS(admin, TaskFactory);
BS_DECLARE_REFERENCE_CLASS(admin, TaskBase);
BS_DECLARE_REFERENCE_CLASS(admin, TaskFlow);

namespace build_service { namespace admin {

class TaskFlow : public autil::legacy::Jsonizable
{
public:
    // key: taskId, value: key-value map
    typedef std::map<std::string, KeyValueMap> TaskParameterMap;

    enum TF_STATUS {
        tf_unknown = 0,
        tf_init = 1,
        tf_running = 2,   // lua stepRun running
        tf_finishing = 4, // lua stepRun finish
        tf_eof = tf_finishing,
        tf_finish = 8,      // all task finish
        tf_stopping = 16,   // flow be stopping
        tf_stopped = 32,    // flow be stopped
        tf_suspending = 64, // flow be suspending
        tf_suspended = 128, // flow be suspended
        tf_error = 256,
        tf_fatal = 512,
    };

public:
    TaskFlow()
        : _lState(NULL)
        , _status(tf_unknown)
        , _lastStatus(tf_unknown)
        , _flowContainer(NULL)
        , _isPublicFlow(false)
        , _hasFatalError(false)
    {
    }

    TaskFlow(const std::string& rootPath, const TaskFactoryPtr& factory,
             const TaskContainerPtr& taskContainer = TaskContainerPtr(),
             const TaskResourceManagerPtr& taskResMgr = TaskResourceManagerPtr());
    virtual ~TaskFlow();

public:
    bool init(const std::string& fileName, const std::string& flowId = "",
              const KeyValueMap& flowParam = KeyValueMap());

    bool initSimpleFlow(const std::string& taskId, const std::string& kernalType, const KeyValueMap& kvMap,
                        const std::string& flowId = "");

    void makeActive(bool force = false);

    void stepRun();

    void setTaskParameter(const TaskParameterMap& taskMap);
    // set default param for all task
    void setGlobalTaskParam(const std::string& key, const std::string& value);

    bool suspendFlow();
    bool resumeFlow();
    bool stopFlow();

    bool isFlowRunning() const { return _status == tf_running; }
    bool isFlowStopping() const { return _status == tf_stopping; }
    bool isFlowStopped() const { return _status == tf_stopped; }
    bool isFlowFinishing() const { return _status == tf_finishing; }
    bool isFlowFinish() const { return _status == tf_finish; }
    bool isFlowSuspending() const { return _status == tf_suspending; }
    bool isFlowSuspended() const { return _status == tf_suspended; }
    bool hasRuntimeError() const { return _status == tf_error; }
    bool hasFatalError() const { return _hasFatalError; }

    bool isSimpleFlow() const;

    TF_STATUS getStatus() const { return _status; }
    const std::string& getStatusStr()
    {
        _statusStr = status2Str(getStatus());
        return _statusStr;
    }

    TaskBasePtr createTask(const std::string& id, const std::string& kernalType, const KeyValueMap& kvMap);

    bool openApi(const std::string& cmd, const std::string& path, const KeyValueMap& params);

    TaskBasePtr getTask(const std::string& id) const;
    TaskBasePtr getTaskInFlow(const std::string& id, const std::string& flowId) const;
    TaskFlowPtr getFlow(const std::string& flowId) const;

    // wait status = xxx|xxx
    void addUpstreamFlowId(const std::string& flowId, const std::string& waitStatus = "finish|stop",
                           const std::string& flowMissingAction = "stop");

    void addFriendFlowId(const std::string& flowId);

    const std::string& getFlowId() const { return _flowId; }
    // const std::vector<std::string>& getTags() const { return _tags; }
    bool hasTag(const std::string& tag) { return _tags.find(tag) != _tags.end(); }
    void addTag(const std::string& tag) { _tags.insert(tag); }
    std::string getTags() const;
    void clearTasks();
    const std::string& getGraphId() const { return _graphId; }
    void setGraphId(const std::string& gid) { _graphId = gid; }

public:
    static std::string getTaskId(const std::string& flowId, const std::string& taskId);
    static void getOriginalFlowIdAndTaskId(const std::string& taskId, std::string& flowId, std::string& id);
    static TF_STATUS str2Status(const std::string& status);
    static std::string status2Str(TF_STATUS stat);
    static std::string waitStatus2Str(uint32_t waitMask);
    static uint32_t str2WaitMask(const std::string& status);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    // for lua script
    void setError(const std::string& errorMsg, bool isFatal);
    std::string getError() const;

    const std::string& getProperty(const std::string& key) const;
    const std::string& getFlowProperty(const std::string& flowId, const std::string& key);
    const std::string& getFlowStatus(const std::string& flowId);
    void setProperty(const std::string& key, const std::string& value);

    void setFlowContainer(FlowContainer* container) { _flowContainer = container; }
    std::vector<TaskBasePtr> getTasks() const;

    const std::string& getScriptInfo() const { return _scriptInfo; }
    std::vector<std::string> getUpstreamItemInfos() const;
    std::vector<std::string> getUpstreamFlowIds() const;
    std::vector<std::string> getFriendFlowIds() const;

    std::string getFlowLableString(bool fillTask = true) const;
    const KeyValueMap& getFlowParameterMap() const { return _flowParamMap; }
    bool isUpstreamFlowReady();

    void fillInnerTaskInfo(std::string& taskDotString, std::map<std::string, KeyValueMap>& taskDetailInfoMap);

private:
    bool initByString(const std::string& str);
    void registerTaskClass(lua_State* state, const std::string& input);
    bool initFlowByString(lua_State* state, const std::string& input);
    bool getFlowString(const std::string& input, std::string& flowStr);
    bool prepareLuaParam(lua_State* state);
    bool isAllTaskInactive() const;
    bool isAllTaskInSuspended() const;

    bool checkUpstreamFlow(bool& needAutoStop);
    bool isFriendFlow(const std::string& flowId) const;

    KeyValueMap getKVMapForTask(const std::string& taskId, const KeyValueMap& kvParams);
    void removeTask(const std::string& taskId);

    void addTaskId(const std::string& taskId);

    std::string getLuaScriptForSimpleFlow(const std::string& taskId, const std::string& taskType);

    bool getLuaScriptFromScriptInfo(const std::string& scriptInfo, std::string& content);

    void setErrorMsg(const std::string& str);

    void setPublic(bool isPublic) { _isPublicFlow = isPublic; }

    std::string getFlowInfoLableStr(bool hasBlockUpstream) const;
    std::string getInFlowTaskNodeStr() const;

private:
    typedef std::map<std::string, TaskBasePtr> TaskMap;

    struct UpstreamItem : public autil::legacy::Jsonizable {
    public:
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    public:
        std::string flowId;
        uint32_t waitMask;
        uint32_t actionForMissing = 0; // 0: auto_stop, 1: ignore, 2: pass before
    };

    struct StepErrorInfo {
    public:
        void reset()
        {
            errorMsg = "";
            errorType = 0;
        }
        int errorType = 0; // 0: no error; 1: error; 2: fatal error
        std::string errorMsg;
    };

private:
    std::string _rootPath;
    lua_State* _lState;
    TaskFactoryPtr _taskFactory;
    TaskContainerPtr _taskContainer;
    TaskResourceManagerPtr _taskResMgr;
    std::string _flowId;
    std::string _scriptInfo;
    std::set<std::string> _tags;
    std::string _graphId;
    volatile TF_STATUS _status;
    volatile TF_STATUS _lastStatus;
    std::string _statusStr;
    std::set<std::string> _taskIdSet;
    TaskParameterMap _taskKVMap;
    KeyValueMap _globalParams;
    std::vector<UpstreamItem> _upstreamFlowInfos;
    std::vector<std::string> _friendFlows;
    std::string _errorMsg;
    StepErrorInfo _stepErrorInfo;
    KeyValueMap _propertyMap;
    KeyValueMap _flowParamMap;

    FlowContainer* _flowContainer;
    std::string _logPrefix;
    std::string _filePathIdentifier;
    bool _isPublicFlow;
    bool _hasFatalError;

    mutable autil::RecursiveThreadMutex _lock;
    mutable autil::RecursiveThreadMutex _actionLock;

private:
    friend class TaskFlowWrapper;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskFlow);

}} // namespace build_service::admin
