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
#include "swift/heartbeat/ZkHeartbeatWrapper.h"

#include <iosfwd>

#include "autil/TimeUtility.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "zookeeper/zookeeper.h"

namespace swift {
namespace heartbeat {
using namespace swift::protocol;
using namespace cm_basic;
using namespace std;
using namespace autil;
AUTIL_LOG_SETUP(swift, ZkHeartbeatWrapper);

ZkHeartbeatWrapper::ZkHeartbeatWrapper(const string &host, unsigned int timeout) : ZkWrapper(host, timeout) {}

ZkHeartbeatWrapper::~ZkHeartbeatWrapper() {}

bool ZkHeartbeatWrapper::touch(const string &path, const HeartbeatInfo &msg, bool permanent) {
    string value;
    if (!msg.SerializeToString(&value)) {
        return false;
    }
    bool isExist = false;
    if (!ZkWrapper::check(path, isExist, false)) {
        return false;
    }
    if (!isExist) {
        return ZkWrapper::createNode(path, value, permanent);
    }
    return ZkWrapper::set(path, value);
}

bool ZkHeartbeatWrapper::getHeartBeatInfo(const string &path, HeartbeatInfo &msg, bool watch) {
    string value;
    ZOO_ERRORS error = ZkWrapper::getData(path, value, watch);
    if (ZOK != error) {
        return false;
    }
    if (msg.ParseFromString(value)) {
        return true;
    } else {
        return false;
    }
}

bool ZkHeartbeatWrapper::remove(const string &path) { return ZkWrapper::remove(path); }

} // namespace heartbeat
} // namespace swift
