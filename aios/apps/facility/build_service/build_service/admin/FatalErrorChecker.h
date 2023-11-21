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

#include <assert.h>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class FatalErrorChecker : public autil::legacy::Jsonizable
{
public:
    FatalErrorChecker();
    FatalErrorChecker(const FatalErrorChecker& other);
    ~FatalErrorChecker();
    void operator=(const FatalErrorChecker& other);

public:
    enum FatalErrorRoleType {
        ROLE_NONE,
        ROLE_PROCESSOR,
        ROLE_BUILDER,
        ROLE_MERGER,
        ROLE_JOB,
        ROLE_TASKFLOW,
    };

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void checkFatalErrorInRoles(const proto::GenerationInfo* generationInfo);
    void fillGenerationFatalErrors(proto::GenerationInfo* generationInfo);
    int64_t getFatalErrorDuration();
    FatalErrorRoleType getFatalErrorRole() { return _fatalErrorRole; }
    std::string getFatalErrorRoleName() const;

    const char* getFatalErrorRoleTypeString() const
    {
        switch (_fatalErrorRole) {
        case ROLE_NONE:
            return "none";
        case ROLE_PROCESSOR:
            return "processor";
        case ROLE_BUILDER:
            return "builder";
        case ROLE_MERGER:
            return "merger";
        case ROLE_JOB:
            return "job";
        case ROLE_TASKFLOW:
            return "taskflow";
        default:
            assert(false);
            return "unknown";
        }
    }

    std::string toString() const;

private:
    void updateFatalError(FatalErrorRoleType type, const std::string& errorMsg);
    std::string fatalErrorRoleToString(FatalErrorRoleType type);
    FatalErrorRoleType stringToFatalErrorRole(const std::string& strType);
    bool checkFatalErrorInTaskFlows(const proto::GenerationInfo* generationInfo);

private:
    FatalErrorRoleType _fatalErrorRole;
    std::string _fatalErrorMsg;
    int64_t _fatalErrorStartTime;
    mutable autil::ThreadMutex _mutex;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FatalErrorChecker);

}} // namespace build_service::admin
