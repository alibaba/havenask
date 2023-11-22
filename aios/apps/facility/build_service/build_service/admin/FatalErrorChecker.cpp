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
#include "build_service/admin/FatalErrorChecker.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, FatalErrorChecker);

FatalErrorChecker::FatalErrorChecker() : _fatalErrorRole(ROLE_NONE)
{
    _fatalErrorStartTime = TimeUtility::currentTimeInSeconds();
}

FatalErrorChecker::FatalErrorChecker(const FatalErrorChecker& other)
    : _fatalErrorRole(other._fatalErrorRole)
    , _fatalErrorMsg(other._fatalErrorMsg)
    , _fatalErrorStartTime(other._fatalErrorStartTime)
{
}

FatalErrorChecker::~FatalErrorChecker() {}

void FatalErrorChecker::operator=(const FatalErrorChecker& other)
{
    _fatalErrorRole = other._fatalErrorRole;
    _fatalErrorMsg = other._fatalErrorMsg;
    _fatalErrorStartTime = other._fatalErrorStartTime;
}

void FatalErrorChecker::updateFatalError(FatalErrorRoleType type, const string& errorMsg)
{
    autil::ScopedLock lock(_mutex);
    _fatalErrorMsg = errorMsg;
    if (_fatalErrorRole != type) {
        _fatalErrorRole = type;
        _fatalErrorStartTime = TimeUtility::currentTimeInSeconds();
        BS_LOG(WARN, "update fatal error, type [%s], start time [%ld] msg [%s]", fatalErrorRoleToString(type).c_str(),
               _fatalErrorStartTime, errorMsg.c_str());
    }
}
void FatalErrorChecker::checkFatalErrorInRoles(const proto::GenerationInfo* generationInfo)
{
    if (generationInfo->has_processorinfo() && generationInfo->processorinfo().fatalerrors().size() > 0) {
        string errorMsg = generationInfo->processorinfo().fatalerrors(0).rolename() + ":" +
                          generationInfo->processorinfo().fatalerrors(0).errormsg();
        updateFatalError(ROLE_PROCESSOR, errorMsg);
        return;
    }

    if (generationInfo->has_buildinfo()) {
        for (auto& clusterInfo : generationInfo->buildinfo().clusterinfos()) {
            if (clusterInfo.fatalerrors().size() > 0) {
                string fatalErrorMsg =
                    clusterInfo.fatalerrors(0).rolename() + ":" + clusterInfo.fatalerrors(0).errormsg();
                FatalErrorRoleType type = ROLE_NONE;
                if (clusterInfo.has_builderinfo()) {
                    type = ROLE_BUILDER;
                } else {
                    assert(clusterInfo.has_mergerinfo());
                    type = ROLE_MERGER;
                }
                updateFatalError(type, fatalErrorMsg);
                return;
            }
        }
    }

    if (generationInfo->has_jobinfo() && generationInfo->jobinfo().fatalerrors().size() > 0) {
        string fatalErrorMsg = generationInfo->jobinfo().fatalerrors(0).rolename() + ":" +
                               generationInfo->jobinfo().fatalerrors(0).errormsg();
        updateFatalError(ROLE_JOB, fatalErrorMsg);
        return;
    }
    if (checkFatalErrorInTaskFlows(generationInfo)) {
        return;
    }
    updateFatalError(ROLE_NONE, "");
    return;
}

bool FatalErrorChecker::checkFatalErrorInTaskFlows(const proto::GenerationInfo* generationInfo)
{
    for (auto& flowMeta : generationInfo->activeflowmetas()) {
        if (flowMeta.hasfatalerror()) {
            string fatalErrorMsg =
                string("active flow [") + flowMeta.flowid() + "] has fatal error : " + flowMeta.errormsg();
            updateFatalError(ROLE_TASKFLOW, fatalErrorMsg);
            return true;
        }
    }

    for (auto& flowMeta : generationInfo->stoppedflowmetas()) {
        if (flowMeta.hasfatalerror()) {
            string fatalErrorMsg =
                string("stopped flow [") + flowMeta.flowid() + "] has fatal error : " + flowMeta.errormsg();
            updateFatalError(ROLE_TASKFLOW, fatalErrorMsg);
            return true;
        }
    }
    return false;
}

void FatalErrorChecker::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    string fatalErrorRole = fatalErrorRoleToString(_fatalErrorRole);
    json.Jsonize("fatal_error_role", fatalErrorRole, fatalErrorRole);
    _fatalErrorRole = stringToFatalErrorRole(fatalErrorRole);
    json.Jsonize("fatal_error_msg", _fatalErrorMsg, _fatalErrorMsg);
}

string FatalErrorChecker::fatalErrorRoleToString(FatalErrorRoleType type)
{
    if (type == ROLE_NONE) {
        return "ROLE_NONE";
    } else if (type == ROLE_PROCESSOR) {
        return "ROLE_PROCESSOR";
    } else if (type == ROLE_BUILDER) {
        return "ROLE_BUILDER";
    } else if (type == ROLE_MERGER) {
        return "ROLE_MERGER";
    } else if (type == ROLE_JOB) {
        return "ROLE_JOB";
    } else if (type == ROLE_TASKFLOW) {
        return "ROLE_TASKFLOW";
    }
    assert(false);
    return "";
}

FatalErrorChecker::FatalErrorRoleType FatalErrorChecker::stringToFatalErrorRole(const string& strType)
{
    if (strType == "ROLE_NONE") {
        return ROLE_NONE;
    } else if (strType == "ROLE_PROCESSOR") {
        return ROLE_PROCESSOR;
    } else if (strType == "ROLE_BUILDER") {
        return ROLE_BUILDER;
    } else if (strType == "ROLE_MERGER") {
        return ROLE_MERGER;
    } else if (strType == "ROLE_JOB") {
        return ROLE_JOB;
    } else if (strType == "ROLE_TASKFLOW") {
        return ROLE_TASKFLOW;
    }
    assert(false);
    return ROLE_NONE;
}

void FatalErrorChecker::fillGenerationFatalErrors(proto::GenerationInfo* generationInfo)
{
    autil::ScopedLock lock(_mutex);
    if (_fatalErrorRole != ROLE_NONE) {
        generationInfo->set_hasfatalerror(true);
        generationInfo->set_generationfatalerrormsg(_fatalErrorMsg);
    } else {
        generationInfo->set_hasfatalerror(false);
    }
}

int64_t FatalErrorChecker::getFatalErrorDuration()
{
    if (_fatalErrorRole == ROLE_NONE) {
        return 0;
    }
    return TimeUtility::currentTimeInSeconds() - _fatalErrorStartTime;
}

std::string FatalErrorChecker::getFatalErrorRoleName() const
{
    autil::ScopedLock lock(_mutex);
    auto pos = _fatalErrorMsg.find(":");
    if (pos == string::npos) {
        return std::string("");
    }
    return _fatalErrorMsg.substr(0, pos);
}

std::string FatalErrorChecker::toString() const
{
    autil::ScopedLock lock(_mutex);
    if (_fatalErrorRole == ROLE_NONE) {
        return "";
    }
    return autil::legacy::ToJsonString(*this, true);
}

}} // namespace build_service::admin
