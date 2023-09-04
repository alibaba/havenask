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

#include <google/protobuf/stubs/callback.h>
#include <string>

#include "autil/Log.h"
#include "swift/admin/AdminLogClosure.h" // IWYU pragma: keep
#include "swift/admin/SwiftAdminServer.h"
#include "swift/admin/SysController.h"
#include "swift/auth/RequestAuthenticator.h"
#include "swift/common/Common.h"
#include "swift/monitor/AdminMetricsReporter.h"
#include "swift/protocol/Heartbeat.pb.h"

namespace swift {
namespace config {
class AdminConfig;
} // namespace config
} // namespace swift

namespace google {
namespace protobuf {
class RpcController;
} // namespace protobuf
} // namespace google

namespace swift {
namespace heartbeat {
class HeartbeatServiceImpl : public ::swift::protocol::HeartbeatService, public swift::admin::SwiftAdminServer {
public:
    HeartbeatServiceImpl(config::AdminConfig *config,
                         admin::SysController *sysController,
                         monitor::AdminMetricsReporter *reporter);
    ~HeartbeatServiceImpl() override;

public:
    RPC_METHOD_DEFINE(reportBrokerStatus, BrokerStatusRequest, BrokerStatusResponse, 1, false, checkSysAuthorizer);

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(HeartbeatServiceImpl);

} // namespace heartbeat
} // namespace swift
