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
#include "build_service_tasks/channel/ChannelBase.h"

#include <iosfwd>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/Log.h"
#include "worker_framework/PathUtil.h"

using namespace build_service::util;
using namespace autil;
using namespace std;

namespace build_service { namespace task_base {

BS_LOG_SETUP(task_base, ChannelBase);

ChannelBase::ChannelBase() {}

ChannelBase::~ChannelBase() {}

bool ChannelBase::init()
{
    _zkWrapper = make_unique<cm_basic::ZkWrapper>(worker_framework::PathUtil::getHostFromZkPath(_hostPath));
    if (!_zkWrapper->open()) {
        return false;
    }
    _leaderClient = make_unique<arpc::LeaderClient>(_zkWrapper.get());
    _leaderClient->registerSpecParseCallback(ChannelBase::hostParse);
    return true;
}

RPCChannelPtr ChannelBase::getRpcChannelWithRetry()
{
    return _leaderClient->getRpcChannel(worker_framework::PathUtil::getPathFromZkPath(_hostPath),
                                        /*required Spec = */ {});
}

arpc::LeaderClient::Spec ChannelBase::hostParse(const std::string& content)
{
    LeaderInfoContent leaderInfo;
    if (!parseFromJsonString(content, leaderInfo)) {
        BS_LOG(ERROR, "parse leaderInfo failed, value:[%s] failed", content.c_str());
        return {};
    }
    if (leaderInfo.rpcAddress.empty()) {
        BS_LOG(ERROR, "parse AdminSpec from json:[%s] failed, address is empty", content.c_str());
        return {};
    }
    auto strs = autil::StringUtil::split(leaderInfo.rpcAddress, ":");
    std::string ip;
    std::string port;
    // TODO(tianxiao) check size == 2
    if (strs.size() >= 1) {
        ip = strs[0];
    }
    if (strs.size() >= 2) {
        port = strs[1];
    }
    return arpc::LeaderClient::Spec(ip, port, "");
}

bool ChannelBase::parseFromJsonString(const std::string& jsonStr, LeaderInfoContent& leaderInfoContent)
{
    if (jsonStr.empty()) {
        BS_LOG(ERROR, "FromJsonString failed, jsonStr is empty");
        return false;
    }
    try {
        autil::legacy::FromJsonString(leaderInfoContent, jsonStr);
    } catch (autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "FromJsonString [%s] catch exception: [%s]", jsonStr.c_str(), e.ToString().c_str());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "exception happen when call FromJsonString [%s] ", jsonStr.c_str());
        return false;
    }
    return true;
}

}} // namespace build_service::task_base
