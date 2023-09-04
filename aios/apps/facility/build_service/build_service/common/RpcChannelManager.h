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

#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "aios/network/arpc/arpc/ANetRPCChannel.h"
#include "aios/network/arpc/arpc/ANetRPCChannelManager.h"
#include "build_service/util/Log.h"
#include "worker_framework/ZkState.h"

namespace build_service::common {

class RpcChannelManager
{
private:
    static constexpr uint32_t DEFAULT_ZK_TIMEOUT = 10; // 10 seconds
public:
    RpcChannelManager() = default;
    virtual ~RpcChannelManager();

public:
    bool Init(const std::string& zkRoot, bool isJsonFormat = true);
    std::shared_ptr<::google::protobuf::RpcChannel> getRpcChannel();

private:
    static std::string convertToSpec(const std::string& addr);
    static bool ChannelBroken(const std::shared_ptr<::google::protobuf::RpcChannel>& channel)
    {
        auto ch = static_cast<arpc::ANetRPCChannel*>(channel.get());
        return ch->ChannelBroken();
    }

private:
    virtual std::shared_ptr<::google::protobuf::RpcChannel> createRpcChannel(const std::string& spec);
    std::string getAdminTcpAddress();

    static std::string getHostFromZkPath(const std::string& zkPath);
    static std::string getPathFromZkPath(const std::string& zkPath);

private:
    std::string _zkPath;

    std::unique_ptr<cm_basic::ZkWrapper> _zkWrapper;
    std::unique_ptr<arpc::ANetRPCChannelManager> _rpcChannelManager;

    std::shared_ptr<::google::protobuf::RpcChannel> _channel;
    bool _isJsonFormat = true;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::common
