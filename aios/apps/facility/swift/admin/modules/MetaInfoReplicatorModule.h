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

#include <atomic>
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_map>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "swift/admin/modules/BaseModule.h"

namespace swift {
namespace network {
class SwiftRpcChannelManager;
typedef std::shared_ptr<SwiftRpcChannelManager> SwiftRpcChannelManagerPtr;
class SwiftAdminAdapter;
typedef std::shared_ptr<SwiftAdminAdapter> SwiftAdminAdapterPtr;
} // namespace network

namespace util {
class ZkDataAccessor;
}
namespace admin {

class MetaInfoReplicatorModule : public BaseModule {
public:
    MetaInfoReplicatorModule();
    ~MetaInfoReplicatorModule();
    MetaInfoReplicatorModule(const MetaInfoReplicatorModule &) = delete;
    MetaInfoReplicatorModule &operator=(const MetaInfoReplicatorModule &) = delete;

public:
    bool doInit() override;
    bool doLoad() override;
    bool doUnload() override;
    bool waitStop();

private:
    void workLoop();
    // virtual for test
    virtual bool checkReplicator();
    bool syncTopicAclData();
    bool syncTopicMeta();
    bool syncTopicSchemas();
    bool syncWriterVersion();

private:
    autil::ThreadMutex _lock;
    bool _stopped;
    std::atomic<int64_t> _lastDoneTimestamp;
    autil::LoopThreadPtr _loopThread;
    std::unique_ptr<util::ZkDataAccessor> _selfZkAccessor;
    std::unique_ptr<util::ZkDataAccessor> _mirrorZkAccessor;
    std::string _selfZkRoot;
    std::string _mirrorZkRoot;
    std::string _selfZkPath;
    std::string _mirrorZkPath;

    std::string _topicMeta;
    std::string _topicAclData;
    std::unordered_map<std::string, std::string> _topicSchemas;
    std::unordered_map<std::string, std::string> _writerVersion;

    network::SwiftRpcChannelManagerPtr _channelManager;
    network::SwiftAdminAdapterPtr _adminAdapter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace admin
} // namespace swift
