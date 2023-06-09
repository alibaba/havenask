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
#include "build_service/common/CounterSynchronizer.h"

#include "autil/StringUtil.h"
#include "build_service/common/PathDefine.h"
#include "build_service/proto/ProtoUtil.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
BS_NAMESPACE_USE(util);
BS_NAMESPACE_USE(proto);

namespace build_service { namespace common {
BS_LOG_SETUP(common, CounterSynchronizer);

string CounterSynchronizer::getCounterZkRoot(const string& appZkRoot, const proto::BuildId& buildId)
{
    string zkRoot = appZkRoot;
    string generationZkRoot = PathDefine::getGenerationZkRoot(zkRoot, buildId);
    return fslib::util::FileUtil::joinFilePath(generationZkRoot, COUNTER_DIR_NAME);
}

bool CounterSynchronizer::getCounterZkFilePath(const string& appZkRoot, const proto::PartitionId& pid,
                                               string& counterFilePath, bool ignoreBackupId)
{
    string counterZkRoot = CounterSynchronizer::getCounterZkRoot(appZkRoot, pid.buildid());
    string counterFileName;
    if (!ProtoUtil::partitionIdToCounterFileName(pid, counterFileName, ignoreBackupId)) {
        return false;
    }
    counterFilePath = fslib::util::FileUtil::joinFilePath(counterZkRoot, counterFileName);
    return true;
}

string CounterSynchronizer::getCounterRedisKey(const string& serviceName, const proto::BuildId& buildId)
{
    vector<string> strVec;
    strVec.push_back(serviceName);
    strVec.push_back(StringUtil::toString(buildId.appname()));
    strVec.push_back(StringUtil::toString(buildId.datatable()));
    strVec.push_back(StringUtil::toString(buildId.generationid()));
    return StringUtil::toString(strVec, ".");
}

bool CounterSynchronizer::getCounterRedisHashField(const proto::PartitionId& pid, std::string& fieldName)
{
    if (pid.role() == ROLE_UNKNOWN) {
        return false;
    }
    vector<string> strVec;
    strVec.push_back(ProtoUtil::toRoleString(pid.role()));

    switch (pid.role()) {
    case ROLE_PROCESSOR:
    case ROLE_BUILDER:
    case ROLE_JOB: {
        strVec.push_back(ProtoUtil::toStepString(pid));
        break;
    }
    case ROLE_MERGER:
        strVec.push_back(pid.mergeconfigname());
        break;
    case ROLE_TASK:
        strVec.push_back(pid.taskid());
        break;
    default:
        break;
    }

    strVec.push_back(StringUtil::toString(pid.range().from()));
    strVec.push_back(StringUtil::toString(pid.range().to()));

    if (pid.role() == ROLE_BUILDER || pid.role() == ROLE_MERGER || pid.role() == ROLE_JOB || pid.role() == ROLE_TASK) {
        if (pid.clusternames_size() != 1) {
            return false;
        }
        strVec.push_back(pid.clusternames(0));
    }

    fieldName = StringUtil::toString(strVec, ".");
    return true;
}

bool CounterSynchronizer::beginSync(int64_t syncInterval)
{
    if (_syncThread) {
        BS_LOG(ERROR, "beginSync can only be called once");
        return false;
    }

    _syncThread = autil::LoopThread::createLoopThread(std::bind(&CounterSynchronizer::sync, this),
                                                      syncInterval * 1000 * 1000, "BsCounter");

    if (!_syncThread) {
        BS_LOG(ERROR, "create serialize counter thread failed.");
        return false;
    }
    return true;
}

void CounterSynchronizer::stopSync()
{
    if (_syncThread) {
        _syncThread.reset();
        sync();
    }
}

}} // namespace build_service::common
