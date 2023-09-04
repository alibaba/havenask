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
#include <unordered_set>

#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "swift/admin/AdminZkDataAccessor.h"
#include "swift/admin/modules/BaseModule.h"
#include "swift/common/Common.h"

namespace swift {
namespace protocol {
class TopicCreationRequest;
} // namespace protocol

namespace admin {
class CleanAtDeleteManager;

class CleanDataModule : public BaseModule {
public:
    CleanDataModule();
    ~CleanDataModule();
    CleanDataModule(const CleanDataModule &) = delete;
    CleanDataModule &operator=(const CleanDataModule &) = delete;

public:
    bool doInit() override;
    bool doLoad() override;
    bool doUnload() override;

public:
    bool isCleaningTopic(const std::string &topic);
    bool needCleanDataAtOnce(const std::string &topic);
    void pushCleanAtDeleteTopic(const std::string &topic, const protocol::TopicCreationRequest &meta);
    void removeCleanAtDeleteTopicData(const std::unordered_set<std::string> &loadedTopicSet);

private:
    autil::LoopThreadPtr _loopThread;
    std::shared_ptr<CleanAtDeleteManager> _cleanAtDeleteManager;
    AdminZkDataAccessorPtr _zkDataAccessor;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(CleanDataModule);

} // namespace admin
} // namespace swift
