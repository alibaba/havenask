#ifndef ISEARCH_BS_MOKEBROKERTOPICKEEPER_H
#define ISEARCH_BS_MOKEBROKERTOPICKEEPER_H

#include "build_service/common/BrokerTopicKeeper.h"
#include "build_service/common/test/MockSwiftAdminFacade.h"
#include "build_service/common_define.h"

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

#endif // ISEARCH_BS_MOKEBROKERTOPICKEEPER_H
