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

#include <stddef.h>
#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "build_service/admin/Heartbeat.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"
#include "leader_client/LeaderClient.h"

namespace build_service { namespace admin {

class RpcHeartbeat : public Heartbeat
{
public:
    RpcHeartbeat(const std::string& zkRoot, cm_basic::ZkWrapper* zkWrapper);
    ~RpcHeartbeat();

public:
    static arpc::LeaderClient::Identifier convertStringToIdentifier(const std::string& identifier);

private:
    RpcHeartbeat(const RpcHeartbeat&);
    RpcHeartbeat& operator=(const RpcHeartbeat&);

protected:
    void syncStatus() override;

private:
    void callback(arpc::OwnControllerClosure<proto::TargetRequest, proto::CurrentResponse>* closure);
    void call(const proto::PartitionId& pid);
    std::string constructZkPathFromPartitionId(const proto::PartitionId& pid);

private:
    static const size_t HEARTBEAT_TIMEOUT = 5 * 1000; // 5s
private:
    // _leaderClient应该最先析构，_leaderClient析构时有可能调用callback函数
    // callback函数使用了_zkRelativePath，如果_zkRelativePath先析构，会导致core
    std::string _zkRelativePath;
    arpc::LeaderClient _leaderClient;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RpcHeartbeat);

}} // namespace build_service::admin
