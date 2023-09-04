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
#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace protocol {
class HeartbeatInfo;
} // namespace protocol
} // namespace swift

namespace swift {
namespace heartbeat {

class ZkHeartbeatWrapper : public cm_basic::ZkWrapper {
public:
    ZkHeartbeatWrapper(const std::string &host = "", unsigned int timeout = 10);
    ~ZkHeartbeatWrapper();

private:
    ZkHeartbeatWrapper(const ZkHeartbeatWrapper &);
    ZkHeartbeatWrapper &operator=(const ZkHeartbeatWrapper &);

public:
    using cm_basic::ZkWrapper::touch;
    bool touch(const std::string &path, const protocol::HeartbeatInfo &msg, bool permanent = false);
    bool getHeartBeatInfo(const std::string &path, protocol::HeartbeatInfo &msg, bool watch = false);
    bool remove(const std::string &path);

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ZkHeartbeatWrapper);

} // namespace heartbeat
} // namespace swift
