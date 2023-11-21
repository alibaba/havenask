#pragma once

#include <string>

#include "build_service/common/BrokerTopicKeeper.h"
#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/common/test/FakeSwiftAdminFacade.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"

namespace build_service { namespace common {

class FakeBrokerTopicKeeper : public BrokerTopicKeeper
{
public:
    FakeBrokerTopicKeeper(const std::string& brokerTopicZkPath) : _brokerTopicZkPath(brokerTopicZkPath) {}
    ~FakeBrokerTopicKeeper();
    FakeBrokerTopicKeeper(const FakeBrokerTopicKeeper& other);
    bool init(const proto::BuildId& buildId, const config::ResourceReaderPtr& resourceReader,
              const std::string& clusterName, const std::string& topicConfigName, const std::string& topicName,
              bool versionControl) override;

public:
    void setDeleteTopicResult(bool success)
    {
        ((FakeSwiftAdminFacade*)_swiftAdminFacade.get())->setDeleteTopicResult(success);
    }

    void setCreateTopicResult(bool success)
    {
        ((FakeSwiftAdminFacade*)_swiftAdminFacade.get())->setCreateTopicResult(success);
    }

private:
    std::string _brokerTopicZkPath;
};

BS_TYPEDEF_PTR(FakeBrokerTopicKeeper);

}} // namespace build_service::common
