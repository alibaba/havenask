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
#include "swift/admin/WorkerManagerLocal.h"

#include <algorithm>
#include <cstddef>
#include <fstream>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "hippo/DriverCommon.h"
#include "swift/admin/AppPlanMaker.h"
#include "swift/admin/SimpleMasterSchedulerLocal.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, WorkerManagerLocal);

using namespace std;
using namespace autil;
using namespace hippo;
using namespace swift::config;
using namespace swift::common;
using namespace swift::protocol;

WorkerManagerLocal::WorkerManagerLocal(const string &workDir, const string &logConfFile, int32_t maxRestartCount)
    : _workDir(workDir), _logConfFile(logConfFile), _maxRestartCount(maxRestartCount) {}

WorkerManagerLocal::~WorkerManagerLocal() {}

bool WorkerManagerLocal::init(const string &hippoRoot, const string &applicationId, LeaderChecker *leaderChecker) {
    _scheduler = new SimpleMasterSchedulerLocal(_workDir, _logConfFile, _maxRestartCount);
    if (!_scheduler) {
        AUTIL_LOG(ERROR, "WorkerManagerLocal create fail");
        return false;
    }
    if (!_scheduler->init(hippoRoot, leaderChecker, applicationId)) {
        AUTIL_LOG(ERROR, "WorkerManagerLocal init fail");
        return false;
    }
    return true;
}

void WorkerManagerLocal::getReadyRoleCount(const protocol::RoleType roleType,
                                           const string &configPath,
                                           const string &roleVersion,
                                           uint32_t &total,
                                           uint32_t &ready) {
    vector<string> allRoleName;
    AppPlanMaker *appPlanMaker = getAppPlanMaker(configPath, roleVersion);
    if (!appPlanMaker) {
        return;
    }
    allRoleName = appPlanMaker->getRoleName(roleType);
    total = allRoleName.size();
    ready = total;
}

void WorkerManagerLocal::releaseSlotsPref(const vector<string> &roleNames) {
    hippo::ReleasePreference pref;
    vector<hippo::SlotId> toReleaseSlots;
    for (size_t roleIdx = 0; roleIdx < roleNames.size(); ++roleIdx) {
        hippo::SlotId sid;
        sid.slaveAddress = roleNames[roleIdx];
        toReleaseSlots.push_back(sid);
    }
    _scheduler->releaseSlots(toReleaseSlots, pref);
    string fileName = _workDir + "/changeSlotHistory";
    fstream fp(fileName, ios::out);
    if (fp.is_open()) {
        for (int32_t i = 0; i < roleNames.size(); ++i) {
            fp << roleNames[i];
            fp << endl;
        }
        fp.close();
    } else {
        AUTIL_LOG(INFO, "open changeSlotHistory failed");
    }
    AUTIL_LOG(INFO, "local mode release roles[%s]", StringUtil::toString(roleNames, ",").c_str());
}

} // namespace admin
} // namespace swift
