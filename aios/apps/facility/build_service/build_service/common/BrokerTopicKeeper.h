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

#include <map>
#include <set>
#include <stdint.h>
#include <string>

#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftClientCreator.h"

namespace build_service { namespace util {
BS_TYPEDEF_PTR(SwiftClientCreator);
}} // namespace build_service::util

namespace build_service { namespace common {

struct BatchDeleteTopicInfo {
    std::set<std::string> toDeleteTopicNames;
    std::set<std::string> uselessBrokerTopicKeys;
};
typedef std::map<std::string, BatchDeleteTopicInfo> BatchDeleteTopicInfoMap;

// todo single cluster--chayu
class BrokerTopicKeeper : public proto::ErrorCollector
{
private:
    enum SwiftTopicStatus { STS_UNKOWN, STS_CREATED, STS_DELETED };

public:
    BrokerTopicKeeper(const util::SwiftClientCreatorPtr& creator = util::SwiftClientCreatorPtr())
        : _topicStatus(STS_UNKOWN)
        , _retryInterval(0)
        , _lastRetryTimestamp(0)
        , _swiftClientCreator(creator)
    {
    }
    virtual ~BrokerTopicKeeper() {}
    BrokerTopicKeeper(const BrokerTopicKeeper& other);

private:
    BrokerTopicKeeper& operator=(const BrokerTopicKeeper&);

public:
    virtual bool init(const proto::BuildId& buildId, const config::ResourceReaderPtr& resourceReader,
                      const std::string& clusterName, const std::string& topicConfigName, const std::string& topicName,
                      bool versionControl);

    void collectBatchTopicCreationInfos(common::TopicCreationInfoMap& infos);

    void collectDeleteAllTopic(BatchDeleteTopicInfoMap& infoMap);

    bool prepareBrokerTopic();
    virtual bool destroyAllBrokerTopic();

    void markCreatedTopic();
    void reportCreateTopicFail();
    void reportDeleteTopic(bool success);

    std::string getSwiftRoot() const;
    std::string getTopicName() const { return _topicName; }

private:
    bool canRetry();
    void updateRetryStatus(bool success);

protected:
    std::string _clusterName;
    SwiftTopicStatus _topicStatus;
    std::string _topicConfigName;
    std::string _topicName;
    int64_t _retryInterval;      // second
    int64_t _lastRetryTimestamp; // second
    bool _versionControl;

    static int64_t MAX_RETRY_INTERVAL;

protected:
    util::SwiftClientCreatorPtr _swiftClientCreator;
    common::SwiftAdminFacadePtr _swiftAdminFacade;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BrokerTopicKeeper);

}} // namespace build_service::common
