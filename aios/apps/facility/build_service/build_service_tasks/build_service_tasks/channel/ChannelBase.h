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
#ifndef ISEARCH_BS_CHANNELBASE_H
#define ISEARCH_BS_CHANNELBASE_H

#include <memory>
#include <string>

#include "aios/network/arpc/arpc/ANetRPCChannel.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/util/Log.h"
#include "indexlib/misc/common.h"
#include "leader_client/LeaderClient.h"

namespace build_service { namespace task_base {

DEFINE_SHARED_PTR(RPCChannel);

struct LeaderInfoContent : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("address", rpcAddress, rpcAddress);
        json.Jsonize("port", rpcPort, rpcPort);
        json.Jsonize("httpAddress", httpAddress, httpAddress);
        json.Jsonize("httpPort", httpPort, httpPort);
    }
    std::string rpcAddress;
    std::string httpAddress;
    uint32_t rpcPort {0};
    uint32_t httpPort {0};
};

class ChannelBase
{
public:
    ChannelBase();
    virtual ~ChannelBase();

public:
    static bool parseFromJsonString(const std::string& jsonStr, LeaderInfoContent& leaderInfoContent);

public:
    bool init();
    RPCChannelPtr getRpcChannelWithRetry();
    const std::string getHostAddress() const { return _hostAddr; }

public:
    static constexpr uint32_t DEFAULT_ZK_TIMEOUT_S = 10; // in seconds, 10s
    static constexpr uint32_t RPC_TIMEOUT_MS = 5 * 1000; // 5s

private:
    static arpc::LeaderClient::Spec hostParse(const std::string& content);

protected:
    std::string _hostPath;
    std::string _hostAddr;

    std::unique_ptr<cm_basic::ZkWrapper> _zkWrapper;
    std::unique_ptr<arpc::LeaderClient> _leaderClient;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::task_base

#endif // ISEARCH_BS_MADROXCHANNEL_H
