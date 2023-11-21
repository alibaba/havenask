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
#include <stdint.h>
#include <string>
#include <vector>

#include "build_service/common/BrokerTopicAccessor.h"
#include "build_service/common/BrokerTopicKeeper.h"
#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "swift/network/SwiftAdminAdapter.h"

namespace build_service { namespace admin {

class BatchBrokerTopicAccessor : public common::BrokerTopicAccessor
{
public:
    static const size_t DEFAULT_MAX_BATCH_SIZE;

public:
    BatchBrokerTopicAccessor(proto::BuildId buildId, size_t maxBatchSize = DEFAULT_MAX_BATCH_SIZE);
    ~BatchBrokerTopicAccessor();

private:
    BatchBrokerTopicAccessor(const BatchBrokerTopicAccessor&);
    BatchBrokerTopicAccessor& operator=(const BatchBrokerTopicAccessor&);

public:
    bool prepareBrokerTopics() override;
    bool clearUselessBrokerTopics(bool clearAll) override;

private:
    bool canRetry();
    void updateRetryStatus(bool success);
    void markCreateTopicSuccess(const std::string& topicName);
    void reportCreateTopicFail(const std::string& topicName);

    void handleBatchCreateExistError(const std::string& errorMsg,
                                     const std::vector<common::TopicCreationInfo>& topicInfos);

    void makeBatchDeleteRequest(bool clearAll, common::BatchDeleteTopicInfoMap& batchDeleteInfoMap) const;

    common::BrokerTopicKeeperPtr findBrokerTopicKeeper(const std::string& topicName) const;

    void reportBatchDeleteTopic(const std::vector<std::string>& toDeleteTopicNames, bool success);

    virtual swift::network::SwiftAdminAdapterPtr createSwiftAdminAdapter(const std::string& zkPath);

    bool BatchCreateTopic(const swift::network::SwiftAdminAdapterPtr& adapter,
                          const std::vector<common::TopicCreationInfo>& topicInfos);

    bool doBatchCreateTopic(const swift::network::SwiftAdminAdapterPtr& adapter,
                            const std::vector<common::TopicCreationInfo>& topicInfos);

    bool BatchDeleteTopic(const std::string& swiftRoot, const std::vector<std::string>& toDeleteTopicNames);

    bool doBatchDeleteTopic(const swift::network::SwiftAdminAdapterPtr& adapter,
                            const std::vector<std::string>& toDeleteTopicNames);

private:
    int64_t _retryInterval;      // second
    int64_t _lastRetryTimestamp; // second
    size_t _maxBatchSize;
    static const int64_t MAX_RETRY_INTERVAL = 60;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BatchBrokerTopicAccessor);

}} // namespace build_service::admin
