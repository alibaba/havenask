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
#include "build_service/admin/controlflow/TaskFactory.h"

#include <assert.h>
#include <cstddef>
#include <map>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/proto/WorkerNode.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TaskFactory);

class DummyTask : public TaskBase
{
public:
    DummyTask(const string& id, const string& type, const TaskResourceManagerPtr& resMgr) : TaskBase(id, type, resMgr)
    {
    }

public:
    bool doInit(const KeyValueMap& kvMap) override
    {
        _kvMap = kvMap;
        auto iter = kvMap.begin();
        for (; iter != kvMap.end(); iter++) {
            setProperty(iter->first, iter->second);
        }
        return true;
    }

    bool doStart(const KeyValueMap& kvMap) override
    {
        KeyValueMap::const_iterator iter = _kvMap.find("auto_finish");
        if (iter != _kvMap.end() && iter->second == "true") {
            setTaskStatus(TaskBase::ts_finish);
        } else {
            setTaskStatus(TaskBase::ts_running);
        }
        return true;
    }

    bool doFinish(const KeyValueMap& kvMap) override
    {
        auto iter = kvMap.begin();
        for (; iter != kvMap.end(); iter++) {
            _kvMap[iter->first] = iter->second;
            setProperty(iter->first, iter->second);
        }
        return true;
    }

    void doSyncTaskProperty(proto::WorkerNodes& workerNodes) override {}

    TaskBase* clone() override
    {
        assert(false);
        return NULL;
    };

    void Jsonize(Jsonizable::JsonWrapper& json) override { json.Jsonize("status", _kvMap); }

    bool doSuspend() override
    {
        assert(false);
        return false;
    }
    bool doResume() override
    {
        assert(false);
        return false;
    }

    bool updateConfig() override { return true; }

    bool isFinished(proto::WorkerNodes& workerNodes) override
    {
        KeyValueMap::const_iterator iter = _kvMap.find("auto_finish");
        if (iter != _kvMap.end() && iter->second == "true") {
            return true;
        }
        return false;
    }

private:
    KeyValueMap _kvMap;
};

TaskFactory::TaskFactory() {}

TaskFactory::~TaskFactory() {}

TaskBasePtr TaskFactory::createTask(const string& id, const string& kernalType, const KeyValueMap& kvMap,
                                    const TaskResourceManagerPtr& resMgr)
{
    TaskBasePtr task = createTaskObject(id, kernalType, resMgr);
    if (!task) {
        BS_LOG(ERROR, "createTaskObject return null, id [%s], kernalType [%s]", id.c_str(), kernalType.c_str());
        return TaskBasePtr();
    }

    if (!task->init(kvMap)) {
        BS_LOG(ERROR, "init task fail, id [%s], kernalType [%s], kvParams [%s]", id.c_str(), kernalType.c_str(),
               ToJsonString(kvMap).c_str());
        return TaskBasePtr();
    }

    task->setTaskStatus(TaskBase::ts_init);
    return task;
}

TaskBasePtr TaskFactory::createTaskObject(const string& id, const string& kernalType,
                                          const TaskResourceManagerPtr& resMgr)
{
    return TaskBasePtr(new DummyTask(id, kernalType, resMgr));
}

}} // namespace build_service::admin
