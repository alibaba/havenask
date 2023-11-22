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
#include "build_service/proto/WorkerNode.h"

#include <hippo/proto/Common.pb.h>
#include <iosfwd>

#include "autil/UrlEncode.h"
#include "autil/legacy/json.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/ProtoUtil.h"

using namespace std;

namespace build_service { namespace proto {

bool operator==(const hippo::proto::SlotId& l, const hippo::proto::SlotId& r)
{
    return COMPARE_PB_FIELD1(slaveaddress) && COMPARE_PB_FIELD1(id) && COMPARE_PB_FIELD1(declaretime);
}

bool operator!=(const hippo::proto::SlotId& l, const hippo::proto::SlotId& r) { return !(l == r); }

bool operator==(const hippo::proto::AssignedSlot& l, const hippo::proto::AssignedSlot& r)
{
    if (COMPARE_PB_FIELD1(id) && COMPARE_PB_FIELD1(processstatus_size)) {
        if (l.processstatus_size() > 0) {
            return COMPARE_PB_FIELD1(processstatus(0).pid);
        }
        return true;
    }
    return false;
}

bool operator!=(const hippo::proto::AssignedSlot& l, const hippo::proto::AssignedSlot& r) { return !(l == r); }

template <>
std::string WorkerNode<ROLE_TASK>::getCurrentStatusJsonStr() const
{
    TaskCurrent current = getCurrentStatus();
    if (current.has_statusdescription()) {
        auto str = current.statusdescription();
        try {
            auto any = autil::legacy::json::ParseJson(str);
        } catch (const autil::legacy::ExceptionBase& e) {
            // parse to pb object for generalTask
            proto::OperationCurrent opCurrent;
            if (proto::ProtoUtil::parseBase64EncodedPbStr(str, &opCurrent)) {
                current.set_statusdescription(proto::ProtoJsonizer::toJsonString(opCurrent));
            }
        }
    }
    return proto::ProtoJsonizer::toJsonString(current);
}

template <>
std::string WorkerNode<ROLE_TASK>::getTargetStatusJsonStr() const
{
    TaskTarget target = getTargetStatus();
    if (target.has_targetdescription()) {
        auto str = target.targetdescription();
        config::TaskTarget taskTarget;
        try {
            autil::legacy::FromJsonString(taskTarget, str);
        } catch (const autil::legacy::ExceptionBase& e) {
            return proto::ProtoJsonizer::toJsonString(target);
        }
        std::string content;
        if (taskTarget.getTargetDescription("general_task_operation_target", content)) {
            try {
                auto any = autil::legacy::json::ParseJson(content);
            } catch (const autil::legacy::ExceptionBase& e) {
                // parse to pb object for generalTask
                proto::OperationTarget opTarget;
                if (proto::ProtoUtil::parseBase64EncodedPbStr(content, &opTarget)) {
                    taskTarget.updateTargetDescription("general_task_operation_target",
                                                       proto::ProtoJsonizer::toJsonString(opTarget));
                    target.set_targetdescription(autil::legacy::ToJsonString(taskTarget));
                }
            }
        }
    }
    return proto::ProtoJsonizer::toJsonString(target);
}

template <>
std::string WorkerNode<ROLE_PROCESSOR>::getCurrentStatusJsonStr() const
{
    ProcessorCurrent current = getCurrentStatus();
    if (current.has_stoplocator()) {
        auto locator = current.stoplocator();
        if (locator.has_userdata()) {
            locator.set_userdata(autil::UrlEncode::encode(locator.userdata()));
            *current.mutable_stoplocator() = locator;
        }
    }
    if (current.has_currentlocator()) {
        auto locator = current.currentlocator();
        if (locator.has_userdata()) {
            locator.set_userdata(autil::UrlEncode::encode(locator.userdata()));
            *current.mutable_currentlocator() = locator;
        }
    }
    return proto::ProtoJsonizer::toJsonString(current);
}

template <>
std::string WorkerNode<ROLE_PROCESSOR>::getTargetStatusJsonStr() const
{
    ProcessorTarget target = getTargetStatus();
    if (target.has_startlocator()) {
        auto locator = target.startlocator();
        if (locator.has_userdata()) {
            locator.set_userdata(autil::UrlEncode::encode(locator.userdata()));
            *target.mutable_startlocator() = locator;
        }
    }
    return proto::ProtoJsonizer::toJsonString(target);
}

template <>
std::string WorkerNode<ROLE_JOB>::getCurrentStatusJsonStr() const
{
    JobCurrent current = getCurrentStatus();
    if (current.has_currentlocator()) {
        auto locator = current.currentlocator();
        if (locator.has_userdata()) {
            locator.set_userdata(autil::UrlEncode::encode(locator.userdata()));
            *current.mutable_currentlocator() = locator;
        }
    }
    return proto::ProtoJsonizer::toJsonString(current);
}

}} // namespace build_service::proto
