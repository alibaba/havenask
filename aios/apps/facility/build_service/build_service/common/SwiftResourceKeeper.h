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
#ifndef ISEARCH_BS_SWIFTRESOURCEKEEPER_H
#define ISEARCH_BS_SWIFTRESOURCEKEEPER_H

#include "build_service/common/BrokerTopicAccessor.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeper.h"
#include "build_service/common/SwiftParam.h"
#include "build_service/common_define.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftClientCreator.h"

namespace build_service { namespace common {

class SwiftResourceKeeper : public ResourceKeeper
{
public:
    SwiftResourceKeeper(const std::string& name, const std::string& type,
                        const ResourceContainerPtr& resourceContainer);
    ~SwiftResourceKeeper();

private:
    SwiftResourceKeeper(const SwiftResourceKeeper&);
    SwiftResourceKeeper& operator=(const SwiftResourceKeeper&);

public:
    bool init(const KeyValueMap& params) override;
    bool initLegacy(const std::string& clusterNmae, const proto::PartitionId& pid,
                    const config::ResourceReaderPtr& configReader,
                    const KeyValueMap& kvMap); // for worker
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool prepareResource(const std::string& applyer, const config::ResourceReaderPtr& configReader) override;
    void deleteApplyer(const std::string& applyer) override;
    void syncResources(const std::string& roleName, const proto::WorkerNodes& workerNodes) override;
    std::string getResourceId() override { return "0"; }
    std::string getSwiftRoot() const { return _swiftRoot; }
    std::string getTopicName() const { return _topicName; }
    std::string getTopicId() const { return _topicId; }
    bool getSwiftConfig(const config::ResourceReaderPtr& configReader, config::SwiftConfig& swiftConfig);

    virtual common::SwiftParam createSwiftReader(const util::SwiftClientCreatorPtr& swiftClientCreator,
                                                 const KeyValueMap& params,
                                                 const config::ResourceReaderPtr& configReader,
                                                 const proto::Range& range);

    virtual swift::client::SwiftWriter* createSwiftWriter(const util::SwiftClientCreatorPtr& swiftClientCreator,
                                                          const std::string& dataTable,
                                                          const proto::DataDescription& dataDescription,
                                                          const KeyValueMap& params,
                                                          const config::ResourceReaderPtr& configReader);

    virtual swift::client::SwiftClient* createSwiftClient(const util::SwiftClientCreatorPtr& swiftClientCreator,
                                                          const config::ResourceReaderPtr& configReader);

private:
    std::string getTopicClusterName(const std::string& clusterName, const KeyValueMap& kvMap);
    std::pair<std::string, std::string> getLegacyTopicName(const std::string& clusterName,
                                                           const proto::PartitionId& pid,
                                                           const config::ResourceReaderPtr& configReader,
                                                           const std::string& configName, const KeyValueMap& kvMap);

    bool init(const std::string& clusterName, const std::string& topicConfigName, const std::string& tag,
              const std::string& resourceName, ResourceContainer* resourceContainer);

private:
    std::string _topicConfigName;
    std::string _clusterName;
    std::string _tag;
    std::string _topicName;
    std::string _swiftRoot;
    std::string _topicId;
    BrokerTopicAccessorPtr _topicAccessor;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftResourceKeeper);

}} // namespace build_service::common

#endif // ISEARCH_BS_SWIFTRESOURCEKEEPER_H
