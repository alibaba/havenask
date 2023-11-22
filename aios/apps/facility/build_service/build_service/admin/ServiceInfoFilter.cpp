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
#include "build_service/admin/ServiceInfoFilter.h"

#include <google/protobuf/descriptor.h>
#include <map>
#include <ostream>
#include <typeinfo>
#include <utility>

#include "alog/Logger.h"
#include "autil/Lock.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/PathDefine.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/util/FileUtil.h"
#include "worker_framework/WorkerState.h"
#include "worker_framework/ZkState.h"

using namespace std;
using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::util;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;
using namespace worker_framework;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ServiceInfoFilter);

ServiceInfoFilter::~ServiceInfoFilter() {}

bool ServiceInfoFilter::init(cm_basic::ZkWrapper* zkWrapper, const std::string& zkRoot)
{
    _zkWrapper = zkWrapper;
    _zkRoot = zkRoot;
    ZkState zkStatus(_zkWrapper, fslib::util::FileUtil::joinFilePath(_zkRoot, PathDefine::ZK_SERVICE_INFO_TEMPLATE));

    WorkerState::ErrorCode ec = zkStatus.read(_templateStr);
    if (WorkerState::EC_FAIL == ec) {
        stringstream ss;
        ss << "read service_info_template failed, ec[" << int(ec) << "].";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (_templateStr.empty()) {
        return true;
    }

    if (!update(_templateStr)) {
        stringstream ss;
        ss << "update service_info_template failed.";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

void ServiceInfoFilter::filter(const GenerationInfo& gsInfo, GenerationInfo& filteredGsInfo) const
{
    ScopedReadLock slock(_lock);
    filteredGsInfo.CopyFrom(gsInfo);
    if (_templateStr.empty()) {
        return;
    }

    if (doFilter(&filteredGsInfo, _templateMap)) {
        return;
    }
    filteredGsInfo.CopyFrom(gsInfo);
    BS_LOG(ERROR, "template for gs info error, filter gs info failed");
    return;
}

bool ServiceInfoFilter::doFilter(google::protobuf::Message* msg, const JsonMap& jsonMap) const
{
    const auto* desc = msg->GetDescriptor();
    const auto* ref = msg->GetReflection();

    size_t fieldCount = desc->field_count();
    for (size_t i = 0; i < fieldCount; i++) {
        const auto* field = desc->field(i);
        const auto& it = jsonMap.find(field->name());
        if (it == jsonMap.end()) {
            ref->ClearField(msg, field);
            continue;
        }
        auto any = it->second;
        if (any.GetType() == typeid(string)) {
            string value = AnyCast<string>(any);
            if (value == "ALL") {
                continue;
            }
        }
        if (field->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
            if (!field->is_repeated()) {
                if (any.GetType() != typeid(JsonMap)) {
                    return false;
                }
                JsonMap subMap = AnyCast<JsonMap>(any);
                google::protobuf::Message* subMessage = ref->MutableMessage(msg, field);
                if (!doFilter(subMessage, subMap)) {
                    return false;
                }
            } else {
                if (any.GetType() != typeid(JsonArray)) {
                    return false;
                }
                JsonArray subArray = AnyCast<JsonArray>(any);
                if (subArray.size() != 1) {
                    return false;
                }
                Any subAny = subArray[0];
                if (subAny.GetType() != typeid(JsonMap)) {
                    return false;
                }
                JsonMap subMap = AnyCast<JsonMap>(subAny);
                size_t fieldSize = ref->FieldSize(*msg, field);
                for (size_t j = 0; j < fieldSize; j++) {
                    auto* subMessage = ref->MutableRepeatedMessage(msg, field, j);
                    if (!doFilter(subMessage, subMap)) {
                        return false;
                    }
                }
            }
        }
    }
    return true;
}

bool ServiceInfoFilter::update(const std::string& templateStr)
{
    autil::legacy::json::JsonMap jsonMap;
    if (!templateStr.empty()) {
        try {
            FromJsonString(jsonMap, templateStr);
        } catch (const ExceptionBase& e) {
            string errorMsg = "jsonize " + templateStr + " failed, exception: " + string(e.what());
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }

    ZkState zkStatus(_zkWrapper, fslib::util::FileUtil::joinFilePath(_zkRoot, PathDefine::ZK_SERVICE_INFO_TEMPLATE));
    if (WorkerState::EC_FAIL == zkStatus.write(templateStr)) {
        return false;
    }
    {
        ScopedWriteLock sLock(_lock);
        _templateStr = templateStr;
        _templateMap = jsonMap;
    }
    return true;
}

}} // namespace build_service::admin
