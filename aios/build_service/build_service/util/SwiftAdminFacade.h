#ifndef ISEARCH_BS_SWIFTADMINFACADE_H
#define ISEARCH_BS_SWIFTADMINFACADE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <arpc/ANetRPCChannelManager.h>
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/SwiftConfig.h"
#include <swift/common/Common.h>
#include <swift/protocol/Control.pb.h>
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/ControlConfig.h"
#include "build_service/util/SwiftClientCreator.h"

SWIFT_BEGIN_NAMESPACE(client);
class SwiftAdminAdapter;
SWIFT_END_NAMESPACE(client);

namespace build_service {
namespace util {

class SwiftAdminFacade
{
public:
    SwiftAdminFacade();
    virtual ~SwiftAdminFacade();
    SwiftAdminFacade(const SwiftAdminFacade &);
public:
    bool init(const proto::BuildId &buildId,
              const std::string &configPath);

    std::string getLastErrorMsg();

public:
    static bool getTopicName(const config::ResourceReader& resourceReader,
                             const std::string& applicationId,
                             const proto::BuildId &buildId,
                             const std::string &clusterName, bool isFullBrokerTopic,
                             int64_t specifiedTopicId,
                             std::string& topicName);
    static bool getTopicId(const config::ResourceReader& resourceReader,
                           const std::string &clusterName,
                           int64_t& topicId);
    static bool getTopicId(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
                           int64_t& topicId);

private:
    static bool getTopicName(const std::string& applicationId,
                             int64_t topicId,
                             const proto::BuildId &buildId,
                             const std::string &clusterName,
                             const config::SwiftConfig &swiftConfig,
                             bool isFullBrokerTopic,
                             std::string& topicName);

public:
    SwiftAdminFacade& operator=(const SwiftAdminFacade &);
    // return true is topic created, or already exist
    // no atomic guarantee, may return false when some created successfully some failed
    virtual bool prepareBrokerTopic(bool isFullBuildTopic);
    
    // return true if topic destroyed, or not exist
    virtual bool destroyBrokerTopic(bool isFullBuildTopic);

    virtual bool deleteTopic(const std::string &clusterName, bool isFullBuildTopic);
    virtual bool createTopic(const std::string &clusterName, bool isFullBuildTopic);
    bool getTopicName(const std::string &clusterName, bool isFullBuildTopic, std::string& topicName);
    virtual std::string getSwiftRoot() { return _serviceConfig.swiftRoot; } 
private:
    // virtual for test
    virtual SWIFT_NS(client)::SwiftAdminAdapterPtr createSwiftAdminAdapter(
        const std::string &zkPath);

private:
    // return true is topic created, or already exist
    virtual bool createTopic(const SWIFT_NS(client)::SwiftAdminAdapterPtr &adapter,
                             const std::string &topicName, const config::SwiftTopicConfig &config);
    // return true if topic destroyed, or not exist
    virtual bool deleteTopic(const SWIFT_NS(client)::SwiftAdminAdapterPtr &adapter,
                             const std::string &topicName);
    uint32_t getSwiftTimeout();
    
private:
    proto::BuildId _buildId;
    config::BuildServiceConfig _serviceConfig;
    std::map<std::string, config::SwiftConfig> _swiftConfigs;
    std::map<std::string, int64_t> _topicIds; 
    SwiftClientCreatorPtr _swiftClientCreator;
    std::string _lastErrorMsg;
    std::string _adminSwiftConfigStr;
    static const std::string SHARED_BROKER_TOPIC_CLUSTER_NAME;
    config::ResourceReaderPtr _resourceReader;    
private:
    static uint32_t DEFAULT_SWIFT_TIMEOUT;
private:
    BS_LOG_DECLARE();
};
BS_TYPEDEF_PTR(SwiftAdminFacade);
}
}

#endif //ISEARCH_BS_SWIFTADMINFACADE_H
