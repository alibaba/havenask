#include "build_service/util/SwiftAdminFacade.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/ProtoUtil.h"
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <swift/common/PathDefine.h>
#include <swift/client/SwiftAdminAdapter.h>
#include <fslib/fs/FileSystem.h>
#include <autil/StringUtil.h>
#include <autil/legacy/jsonizable.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace google::protobuf;
using namespace google::protobuf::io;

SWIFT_USE_NAMESPACE(protocol);
SWIFT_USE_NAMESPACE(client);

using namespace build_service::config;
using namespace build_service::proto;

namespace build_service {
namespace util {

BS_LOG_SETUP(util, SwiftAdminFacade);

uint32_t SwiftAdminFacade::DEFAULT_SWIFT_TIMEOUT = 3000;
const string SwiftAdminFacade::SHARED_BROKER_TOPIC_CLUSTER_NAME = "shared_broker_topic_clusterx";

SwiftAdminFacade::SwiftAdminFacade() {
}

SwiftAdminFacade::~SwiftAdminFacade() {
}

SwiftAdminFacade::SwiftAdminFacade(const SwiftAdminFacade &other)
    : _buildId(other._buildId)
    , _serviceConfig(other._serviceConfig)
    , _swiftConfigs(other._swiftConfigs)
    , _topicIds(other._topicIds)
    , _swiftClientCreator(other._swiftClientCreator)
    , _adminSwiftConfigStr(other._adminSwiftConfigStr)
    , _resourceReader(other._resourceReader)
{
}

SwiftAdminFacade& SwiftAdminFacade::operator=(const SwiftAdminFacade &other)
{
    _buildId = other._buildId;
    _serviceConfig = other._serviceConfig;
    _swiftConfigs = other._swiftConfigs;
    _topicIds = other._topicIds;
    _swiftClientCreator = other._swiftClientCreator;
    _adminSwiftConfigStr = other._adminSwiftConfigStr;
    _resourceReader = other._resourceReader;
    return *this;
}

bool SwiftAdminFacade::init(const BuildId &buildId,
                            const string &configPath)
{
    _buildId = buildId;
    _resourceReader = ResourceReaderManager::getResourceReader(configPath);
    BuildServiceConfig serviceConfig;
    if (!_resourceReader->getConfigWithJsonPath("build_app.json", "", serviceConfig)) {
        string errorMsg = "parse build_app.json failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _serviceConfig = serviceConfig;
    _swiftClientCreator.reset(new SwiftClientCreator());
    stringstream swiftConfigSS;
    swiftConfigSS << swift::client::CLIENT_CONFIG_ADMIN_TIMEOUT <<
        swift::client::CLIENT_CONFIG_KV_SEPERATOR << getSwiftTimeout();
    _adminSwiftConfigStr = swiftConfigSS.str();
    vector<string> allClusters;
    if (!_resourceReader->getAllClusters(buildId.datatable(), allClusters)) {
        string errorMsg = "getAllClusters for ["
            + buildId.datatable() + "] failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    for (size_t i = 0; i < allClusters.size(); ++i) {
        SwiftConfig swiftConfig;
        const string& clusterName = allClusters[i];
        if (!_resourceReader->getClusterConfigWithJsonPath(clusterName,
                                                         "swift_config", swiftConfig))
        {
            string errorMsg = "Invalid swift config in ["
                + clusterName + "].";
            _lastErrorMsg = errorMsg;
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        _swiftConfigs[clusterName] = swiftConfig;
        IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
            _resourceReader->getSchema(clusterName);
        if (!schema) {
            string errorMsg = "fail to get Schema for cluster[" + clusterName + "]";
            _lastErrorMsg = errorMsg;
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        getTopicId(schema, _topicIds[clusterName]);
    }
    return true;
}    

bool SwiftAdminFacade::getTopicName(
        const config::ResourceReader& resourceReader,
        const std::string& applicationId,
        const BuildId &buildId, const string &clusterName,
        bool isFullBrokerTopic, int64_t specifiedTopicId,
        string& topicName)
{
    SwiftConfig swiftConfig;
    if (!resourceReader.getClusterConfigWithJsonPath(clusterName,
                                                     "swift_config", swiftConfig))
    {
        string errorMsg = "Invalid swift config in [" + clusterName + "].";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    
    int64_t topicId;
    if (specifiedTopicId != -1) {
        topicId = specifiedTopicId;
    } else {
        if (!getTopicId(resourceReader, clusterName, topicId)) {
            return false;
        }
    }
    return getTopicName(applicationId, topicId, buildId,
                        clusterName, swiftConfig,
                        isFullBrokerTopic, topicName);
}

bool SwiftAdminFacade::getTopicName(
        const std::string& applicationId,
        int64_t topicId,
        const BuildId &buildId,
        const string &clusterName,
        const SwiftConfig &swiftConfig, 
        bool isFullBrokerTopic,
        string& topicName)
{
    if (topicId == -1) {
        BS_LOG(ERROR, "invalid topicId");
        return false;
    }
    string topicSuffix = StringUtil::toString(topicId);
    if (topicSuffix == string("0")) {
        topicSuffix = "";
    } else {
        topicSuffix = "_" + topicSuffix;
    }
    if (swiftConfig.isFullIncShareBrokerTopic()) {
        topicName = ProtoUtil::getProcessedDocTopicName(
                applicationId + topicSuffix, buildId, clusterName);
        return true;
    }
    string suffix = isFullBrokerTopic ? "_full" : topicSuffix;
    topicName = ProtoUtil::getProcessedDocTopicName(
            applicationId + suffix, buildId, clusterName);
    return true;
}

bool SwiftAdminFacade::getTopicId(const config::ResourceReader& resourceReader,
                                  const std::string &clusterName,
                                  int64_t& topicId) {

    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
        resourceReader.getSchema(clusterName);
    return getTopicId(schema, topicId);
}

bool SwiftAdminFacade::getTopicId(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
                                  int64_t& topicId) {
    if (!schema) {
        return false;
    }
    if (schema->HasModifyOperations()) {
        topicId = 0;
    } else {
        topicId = schema->GetSchemaVersionId();
    }

    return true;    
}

bool SwiftAdminFacade::prepareBrokerTopic(bool isFullBuildTopic)
{
    SwiftAdminAdapterPtr adapter = createSwiftAdminAdapter(_serviceConfig.swiftRoot);
    if (!adapter) {
        string errorMsg = "create swift adapter for ["
                          + _buildId.datatable() + "] failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!_resourceReader) {
        string errorMsg = "resourceReader for ["
                          + _buildId.datatable() + "] is empty";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;        
    }
    
    for (const auto &kv : _swiftConfigs) {
        const string &clusterName = kv.first;
        const SwiftConfig &swiftConfig = kv.second;
        SwiftTopicConfigPtr clusterTopicConf = isFullBuildTopic ?
            swiftConfig.getFullBrokerTopicConfig() :
            swiftConfig.getIncBrokerTopicConfig();
        string topicName;
        if (!getTopicName(_serviceConfig.getApplicationId(),
                          _topicIds[clusterName],
                          _buildId,
                          clusterName, swiftConfig, isFullBuildTopic, topicName))
        {
            string errorMsg = "cluster [" + clusterName + "] get topicName failed";
            _lastErrorMsg = errorMsg;
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }        

        if (!createTopic(adapter, topicName, *clusterTopicConf)) {
            string errorMsg = "create broker topic[" + topicName +
                              "] for cluster [" + clusterName +"] failed.";
            _lastErrorMsg = errorMsg;
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    return true;    
}

SwiftAdminAdapterPtr SwiftAdminFacade::createSwiftAdminAdapter(
    const std::string &zkPath) {
    SwiftClient* swiftClient = _swiftClientCreator->createSwiftClient(zkPath, _adminSwiftConfigStr);
    if (swiftClient) {
        return swiftClient->getAdminAdapter();
    }
    return SwiftAdminAdapterPtr();
}

bool SwiftAdminFacade::destroyBrokerTopic(bool isFullBuildTopic)
{
    SwiftAdminAdapterPtr adapter = createSwiftAdminAdapter(_serviceConfig.swiftRoot);
    if (!adapter) {
        string errorMsg = "create swift adapter  for ["
                          + _buildId.datatable() + "] failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!_resourceReader) {
        string errorMsg = "resourceReader for ["
                          + _buildId.datatable() + "] is empty";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;        
    }

    for (const auto &kv : _swiftConfigs) {
        const string &clusterName = kv.first;
        const SwiftConfig &swiftConfig = kv.second;
        SwiftTopicConfigPtr swiftTopicConfig;
        string topicName;
        if (!getTopicName(_serviceConfig.getApplicationId(),
                          _topicIds[clusterName],
                          _buildId, clusterName, swiftConfig,
                          isFullBuildTopic, topicName))
        {
            return false;
        }
        if (!deleteTopic(adapter, topicName)) {
            return false;
        }
    }
    return true;
}

bool SwiftAdminFacade::deleteTopic(const string &clusterName, bool isFullBuildTopic)
{
    auto it1 = _swiftConfigs.find(clusterName);
    if (it1 == _swiftConfigs.end()) {
        BS_LOG(ERROR, "cluster[%s] is not existed in SwiftAdminFacade", clusterName.c_str());
        return false;
    }
    const SwiftConfig& swiftConfig = it1->second;
    auto it2 = _topicIds.find(clusterName);
    if (it2 == _topicIds.end()) {
        BS_LOG(ERROR, "cluster[%s] has no topic id", clusterName.c_str());
        return false;
    }    

    int64_t topicId = it2->second;
    SwiftAdminAdapterPtr adapter = createSwiftAdminAdapter(_serviceConfig.swiftRoot);
    if (!adapter) {
        string errorMsg = "create swift adapter  for ["
                          + _buildId.datatable() + "] failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    string topicName;
    if (!getTopicName(_serviceConfig.getApplicationId(), topicId,
                      _buildId, clusterName, swiftConfig,
                      isFullBuildTopic, topicName))
    {
        return false;
    }
    if (!deleteTopic(adapter, topicName)) {
        return false;
    }
    return true;
}

bool SwiftAdminFacade::getTopicName(
        const std::string &clusterName, bool isFullBuildTopic,
        std::string& topicName) {
    const SwiftConfig &swiftConfig = _swiftConfigs[clusterName];
    return getTopicName(_serviceConfig.getApplicationId(),
                        _topicIds[clusterName],
                        _buildId,
                        clusterName, swiftConfig, isFullBuildTopic, topicName);
}

bool SwiftAdminFacade::createTopic(const std::string &clusterName, bool isFullBuildTopic) {
    SwiftAdminAdapterPtr adapter = createSwiftAdminAdapter(_serviceConfig.swiftRoot);
    if (!adapter) {
        string errorMsg = "create swift adapter for ["
                          + _buildId.datatable() + "] failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    const SwiftConfig &swiftConfig = _swiftConfigs[clusterName];
    SwiftTopicConfigPtr clusterTopicConf =
        isFullBuildTopic ?
        swiftConfig.getFullBrokerTopicConfig() :
        swiftConfig.getIncBrokerTopicConfig();
    string topicName;
    if (!getTopicName(_serviceConfig.getApplicationId(),
                      _topicIds[clusterName],
                      _buildId,
                      clusterName, swiftConfig, isFullBuildTopic, topicName))
    {
        string errorMsg = "cluster [" + clusterName + "] get topicName failed";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!createTopic(adapter, topicName, *clusterTopicConf)) {
        string errorMsg = "create broker topic[" + topicName +
                          "] for cluster [" + clusterName +"] failed.";
        _lastErrorMsg = errorMsg;
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool SwiftAdminFacade::createTopic(const SwiftAdminAdapterPtr &adapter,
                                   const string &topicName,
                                   const config::SwiftTopicConfig &config)

{
    TopicCreationRequest request = config.get();
    request.set_topicname(topicName);
    BS_LOG(INFO, "create swift topic with request[%s]", request.ShortDebugString().c_str());
    SWIFT_NS(protocol)::ErrorCode ec = adapter->createTopic(request);
    if (SWIFT_NS(protocol)::ERROR_NONE == ec) {
        BS_LOG(INFO, "create swift topic[%s] done", topicName.c_str());
        return true;
    } else if (SWIFT_NS(protocol)::ERROR_ADMIN_TOPIC_HAS_EXISTED == ec) {
        BS_LOG(INFO, "topic[%s] already exists, reuse it", topicName.c_str());
        return true;
    } else {
        BS_LOG(ERROR, "create topic [%s] failed, request [%s]",
               topicName.c_str(), request.ShortDebugString().c_str());
        return false;
    }
}

bool SwiftAdminFacade::deleteTopic(const SwiftAdminAdapterPtr &adapter, const string &topicName) {
    SWIFT_NS(protocol)::ErrorCode ec = adapter->deleteTopic(topicName);
    if (SWIFT_NS(protocol)::ERROR_NONE == ec) {
        BS_LOG(INFO, "delete swift topic[%s] done", topicName.c_str());
        return true;
    } else if (SWIFT_NS(protocol)::ERROR_ADMIN_TOPIC_NOT_EXISTED == ec) {
        BS_LOG(INFO, "swift topic[%s] already deleted", topicName.c_str());
        return true;
    } else {
        BS_LOG(ERROR, "delete topic [%s] failed.", topicName.c_str());
        return false;
    }
}

string SwiftAdminFacade::getLastErrorMsg() {
    string ret = _lastErrorMsg;
    _lastErrorMsg = "";
    return ret;
}

uint32_t SwiftAdminFacade::getSwiftTimeout()
{
    const char* param = getenv(BS_ENV_ADMIN_SWIFT_TIMEOUT.c_str());
    uint32_t timeout = DEFAULT_SWIFT_TIMEOUT;
    if (!param || !StringUtil::fromString(string(param), timeout)) {
        return DEFAULT_SWIFT_TIMEOUT;
    }
    return timeout;
}

}
}
