#pragma once

#include <string>

#include "build_service/common/BrokerTopicKeeper.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"

namespace build_service { namespace common {

class MockBrokerTopicKeeper : public BrokerTopicKeeper
{
public:
    MockBrokerTopicKeeper(const std::string& brokerTopicZkPath) : _brokerTopicZkPath(brokerTopicZkPath) {}
    ~MockBrokerTopicKeeper();
    MockBrokerTopicKeeper(const MockBrokerTopicKeeper& other);
    bool init(const proto::BuildId& buildId, const config::ResourceReaderPtr& resourceReader,
              const std::string& clusterName, const std::string& topicConfigName, const std::string& topicName,
              bool versionControl) override;

private:
    std::string _brokerTopicZkPath;
};

BS_TYPEDEF_PTR(MockBrokerTopicKeeper);

}} // namespace build_service::common
