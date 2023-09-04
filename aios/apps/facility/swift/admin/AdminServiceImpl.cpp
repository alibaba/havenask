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
#include "swift/admin/AdminServiceImpl.h"

#include <iosfwd>

#include "autil/TimeUtility.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace config {
class AdminConfig;
} // namespace config
} // namespace swift

using namespace swift::common;
using namespace swift::protocol;
using namespace swift::monitor;
using namespace swift::config;

using namespace std;
using namespace autil;
namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, AdminServiceImpl);

AdminServiceImpl::AdminServiceImpl(config::AdminConfig *config,
                                   admin::SysController *sysController,
                                   monitor::AdminMetricsReporter *reporter)
    : SwiftAdminServer(config, sysController, reporter) {}

AdminServiceImpl::~AdminServiceImpl() {}

} // end namespace admin
} // end namespace swift
