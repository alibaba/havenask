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
#include "build_service/task_base/Task.h"

#include "beeper/beeper.h"
#include "build_service/proto/ProtoUtil.h"

using namespace std;
using namespace build_service::proto;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, Task);

Task::Task() { setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME); }

Task::~Task() {}

void Task::handleFatalError(const string& message)
{
    int64_t ADMIN_HEARTBEAT_INTERVAL = 1 * 1000 * 1000;
    int64_t WORKER_EXEC_HEARTBEAT_INTERVAL = 1 * 1000 * 1000;
    usleep(ADMIN_HEARTBEAT_INTERVAL * 2 + WORKER_EXEC_HEARTBEAT_INTERVAL * 2);
    BS_LOG_FLUSH();
    cerr << message << endl;
    // wait current collected by admin, maybe not accurate
    cout << "i will exit" << endl;

    string msg = message + ". i will exit";
    BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, msg);
    BEEPER_CLOSE();
    _exit(-1);
}

void Task::initBeeperTags(const Task::TaskInitParam& param)
{
    _beeperTags.reset(new beeper::EventTags);
    ProtoUtil::buildIdToBeeperTags(param.pid.buildid(), *_beeperTags);
    _beeperTags->AddTag("taskId", param.pid.taskid());
    _beeperTags->AddTag("clusters", param.clusterName);
}

proto::ProgressStatus Task::getTaskProgress() const
{
    // return default progress status
    static const proto::ProgressStatus progressStatus;
    return progressStatus;
}
}} // namespace build_service::task_base
