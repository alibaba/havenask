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
#ifndef ISEARCH_BS_BROKERTOPICACCESSOR_H
#define ISEARCH_BS_BROKERTOPICACCESSOR_H

#include "build_service/common/BrokerTopicKeeper.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class BrokerTopicAccessor
{
public:
    BrokerTopicAccessor(proto::BuildId buildId);
    virtual ~BrokerTopicAccessor();

private:
    BrokerTopicAccessor(const BrokerTopicAccessor&);
    BrokerTopicAccessor& operator=(const BrokerTopicAccessor&);

public:
    std::pair<std::string, std::string> registBrokerTopic(const std::string& roleId,
                                                          const config::ResourceReaderPtr& resourceReader,
                                                          const std::string& clusterName,
                                                          const std::string& topicConfigName, const std::string& tag);
    std::string getTopicName(const config::ResourceReaderPtr& resourceReader, const std::string& clusterName,
                             const std::string& topicConfigName, const std::string& tag);
    std::string getTopicId(const config::ResourceReaderPtr& resourceReader, const std::string& clusterName,
                           const std::string& topicConfigName, const std::string& tag);

    void deregistBrokerTopic(const std::string& roleId, const std::string& topicName);

    virtual bool prepareBrokerTopics();
    virtual bool clearUselessBrokerTopics(bool clearAll);

protected:
    virtual BrokerTopicKeeperPtr createBrokerTopicKeeper()
    {
        BrokerTopicKeeperPtr brokerTopic(new BrokerTopicKeeper(_swiftClientCreator));
        return brokerTopic;
    }

    void addApplyId(const std::string& roleId, const std::string brokerTopicKey);

protected:
    util::SwiftClientCreatorPtr _swiftClientCreator;
    std::map<std::string, BrokerTopicKeeperPtr> _usingBrokerTopics;
    std::map<std::string, std::vector<std::string>> _brokerTopicApplers;
    proto::BuildId _buildId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BrokerTopicAccessor);

}} // namespace build_service::common

#endif // ISEARCH_BS_BROKERTOPICACCESSOR_H
