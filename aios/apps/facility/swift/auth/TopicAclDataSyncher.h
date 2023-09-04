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

#include <functional>
#include <map>
#include <stdint.h>
#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/HashAlgorithm.h" // IWYU pragma: keep
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "autil/result/Result.h"
#include "swift/common/Common.h"

namespace swift {
namespace protocol {
class TopicAclData;
} // namespace protocol

namespace auth {
struct TopicAclDataHeader {
    TopicAclDataHeader() {
        version = 0;
        compress = 0;
        len = 0;
    }
    uint32_t version  : 16;
    uint32_t compress : 3;
    uint32_t len = 0;
    uint32_t topicCount = 0;
    uint32_t checksum = 0;
    static uint32_t calcChecksum(const std::string &content) {
        return autil::HashAlgorithm::hashString(content.c_str(), content.size(), 0);
    }
};

class TopicAclDataSyncher {
public:
    typedef std::function<void(const std::map<std::string, swift::protocol::TopicAclData> &)> CallbackFuncType;
    TopicAclDataSyncher(const CallbackFuncType &callback);
    ~TopicAclDataSyncher();
    TopicAclDataSyncher(const TopicAclDataSyncher &) = delete;
    TopicAclDataSyncher &operator=(const TopicAclDataSyncher &) = delete;

public:
    bool init(const std::string &zkRoot, bool needBackSync, int64_t maxSyncIntervalUs = 10 * 1000 * 1000);
    void stop();

    autil::Result<bool> serialized(const std::map<std::string, protocol::TopicAclData> &accessDataMap);
    autil::Result<bool> deserialized(std::map<std::string, protocol::TopicAclData> &accessDataMap);

private:
    bool doSync();
    bool registerSyncCallback();
    void unregisterSyncCallback();
    void dataChange(cm_basic::ZkWrapper *zk, const std::string &path, cm_basic::ZkWrapper::ZkStatus status);

private:
    std::string _zkPath;
    cm_basic::ZkWrapper *_zkWrapper = nullptr;
    autil::LoopThreadPtr _loopThread;
    autil::ThreadMutex _mutex;
    CallbackFuncType _callbackFunc;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(TopicAclDataSyncher);

} // namespace auth
} // namespace swift
