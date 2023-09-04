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

#include <memory>
#include <string>

#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "swift/admin/AdminZkDataAccessor.h"
#include "swift/admin/modules/BaseModule.h"
#include "swift/admin/modules/NoUseTopicInfo.h"

namespace swift {
namespace protocol {
class LastDeletedNoUseTopicResponse;
class MissTopicRequest;
class MissTopicResponse;
} // namespace protocol

namespace network {
class SwiftRpcChannelManager;

typedef std::shared_ptr<SwiftRpcChannelManager> SwiftRpcChannelManagerPtr;
class SwiftAdminAdapter;

typedef std::shared_ptr<SwiftAdminAdapter> SwiftAdminAdapterPtr;
} // namespace network

namespace admin {
class TopicInStatusManager;
class TopicTable;

class NoUseTopicModule : public BaseModule {
public:
    NoUseTopicModule();
    ~NoUseTopicModule();
    NoUseTopicModule(const NoUseTopicModule &) = delete;
    NoUseTopicModule &operator=(const NoUseTopicModule &) = delete;

public:
    bool doInit() override;
    bool doLoad() override;
    bool doUnload() override;

public:
    void insertNotExistTopics(const std::string &topic);
    void getLastNoUseTopicsMeta(protocol::LastDeletedNoUseTopicResponse *response);
    bool loadLastDeletedNoUseTopics();
    void reportMissTopic(const protocol::MissTopicRequest *request, protocol::MissTopicResponse *response);

private:
    bool initAdminClient();
    void deleteNoUseTopic();
    void recoverMissTopics();

private:
    autil::LoopThreadPtr _deleteThread;
    autil::LoopThreadPtr _recoverThread;
    network::SwiftRpcChannelManagerPtr _channelManager;
    network::SwiftAdminAdapterPtr _adminAdapter;
    AdminZkDataAccessorPtr _zkDataAccessor;
    TopicInStatusManager *_topicInStatusManager;
    TopicTable *_topicTable;
    NoUseTopicInfo _noUseTopicInfo;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace admin
} // namespace swift
