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
#ifndef ISEARCH_BS_SWIFTADMINFACADE_H
#define ISEARCH_BS_SWIFTADMINFACADE_H

#include "aios/network/arpc/arpc/ANetRPCChannelManager.h"
#include "build_service/common_define.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "build_service/util/SwiftClientCreator.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/AdminRequestResponse.pb.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace swift { namespace network {
class SwiftAdminAdapter;
}} // namespace swift::network

namespace build_service { namespace common {

struct TopicCreationInfo {
    std::string topicName;
    swift::protocol::TopicCreationRequest request;
};

typedef std::map<std::string, std::vector<TopicCreationInfo>> TopicCreationInfoMap;

class SwiftAdminFacade
{
public:
    SwiftAdminFacade(const util::SwiftClientCreatorPtr& creator = util::SwiftClientCreatorPtr());
    virtual ~SwiftAdminFacade();
    SwiftAdminFacade(const SwiftAdminFacade&);

public:
    bool init(const proto::BuildId& buildId, const config::ResourceReaderPtr& resourceReader,
              const std::string& clusterName);

    std::string getLastErrorMsg();

    // todo mv out of here--yijie
    std::string getTopicId(const std::string& swiftTopicConfigName, std::string userDefineTag = "");
    // full-->topicFull: username_servicename_(full)_processed_generationid_clustername
    // schemaId-->schema version > 0 && no ops:
    // username_servicename_(3)_processed_generationid_clustername
    //普通：username_servicename()_processed_generationId_clusterName
    //带tag的：username_servicename_(tag_schemaid)_processed_generationId_clusterName
    // topicConfigName_userTag_schemaid
    // full, inc, shemaid, stage
    // specify schema for realtime build, use partition schema
    std::string getTopicName(const std::string& swiftTopicConfigName, std::string userDefineTag = "");

    static std::string getRealtimeTopicName(const std::string& applicationId, proto::BuildId _buildId,
                                            const std::string& clusterName,
                                            const indexlib::config::IndexPartitionSchemaPtr& specifySchema);
    static std::string getRealtimeTopicName(const std::string& applicationId, proto::BuildId _buildId,
                                            const std::string& clusterName);

public:
    SwiftAdminFacade& operator=(const SwiftAdminFacade&);
    virtual bool deleteTopic(const std::string& topicConfigName, const std::string& topicName);
    virtual bool createTopic(const std::string& topicConfigName, const std::string& topicName, bool needVersionControl);
    virtual bool updateWriterVersion(const std::string& topicConfigName, const std::string& topicName,
                                     uint32_t majorVersion,
                                     const std::vector<std::pair<std::string, uint32_t>>& minorVersions);
    virtual std::string getSwiftRoot(const std::string& topicConfigName);

    virtual void collectBatchTopicCreationInfos(const std::string& topicConfigName, const std::string& topicName,
                                                TopicCreationInfoMap& infos);
    virtual bool updateTopicVersionControl(const std::string& topicConfigName, const std::string& topicName,
                                           bool needVersionControl);

    static std::string getAdminSwiftConfigStr();

private:
    // virtual for test
    std::string getTopicTag(const std::string& swiftTopicConfigName, std::string userDefineTag = "");
    virtual swift::network::SwiftAdminAdapterPtr createSwiftAdminAdapter(const std::string& zkPath);

private:
    // return true is topic created, or already exist
    virtual bool createTopic(const swift::network::SwiftAdminAdapterPtr& adapter, const std::string& topicName,
                             const config::SwiftTopicConfig& config, bool needVersionControl);
    // return true if topic destroyed, or not exist
    virtual bool deleteTopic(const swift::network::SwiftAdminAdapterPtr& adapter, const std::string& topicName);
    virtual bool updateWriterVersion(const swift::network::SwiftAdminAdapterPtr& adapter, const std::string& topicName,
                                     uint32_t majorVersion,
                                     const std::vector<std::pair<std::string, uint32_t>>& minorVersions);

    static uint32_t getSwiftTimeout();

private:
    proto::BuildId _buildId;
    config::BuildServiceConfig _serviceConfig;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
    std::string _clusterName;
    config::SwiftConfig _swiftConfig;
    util::SwiftClientCreatorPtr _swiftClientCreator;
    std::string _lastErrorMsg;
    std::string _adminSwiftConfigStr;
    config::ResourceReaderPtr _resourceReader;

private:
    static uint32_t DEFAULT_SWIFT_TIMEOUT;

private:
    BS_LOG_DECLARE();
};
BS_TYPEDEF_PTR(SwiftAdminFacade);
}} // namespace build_service::common

#endif // ISEARCH_BS_SWIFTADMINFACADE_H
