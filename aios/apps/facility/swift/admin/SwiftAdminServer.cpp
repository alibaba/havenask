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
#include "swift/admin/SwiftAdminServer.h"

#include <google/protobuf/stubs/callback.h>
#include <string>

#include "swift/config/AdminConfig.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace monitor {
class AdminMetricsReporter;
} // namespace monitor

namespace admin {
class SysController;

AUTIL_LOG_SETUP(swift, SwiftAdminServer);

SwiftAdminServer::SwiftAdminServer(config::AdminConfig *config,
                                   admin::SysController *sysController,
                                   monitor::AdminMetricsReporter *reporter)
    : _adminConfig(config), _sysController(sysController), _metricsReporter(reporter), _requestProcessTimeRatio(600) {}

SwiftAdminServer::~SwiftAdminServer() {}

bool SwiftAdminServer::init() {
    _requestProcessTimeRatio = _adminConfig->getRequestProcessTimeRatio();
    return true;
}

void SwiftAdminServer::setLoggerLevel(::google::protobuf::RpcController *controller,
                                      const ::swift::protocol::SetLoggerLevelRequest *request,
                                      ::swift::protocol::SetLoggerLevelResponse *response,
                                      ::google::protobuf::Closure *done) {
    (void)controller;
    (void)response;
    AUTIL_LOG(INFO, "set logger level, request:%s", request->ShortDebugString().c_str());
    alog::Logger *logger = alog::Logger::getLogger(request->logger().c_str());
    logger->setLevel(request->level());
    done->Run();
}
} // namespace admin
} // namespace swift
