#include "build_service/common/test/MockBrokerTopicKeeper.h"

#include <iosfwd>
#include <memory>

#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/common/test/MockSwiftAdminFacade.h"
#include "build_service/config/CounterConfig.h"

using namespace std;
using namespace build_service::config;

namespace build_service { namespace common {

MockBrokerTopicKeeper::~MockBrokerTopicKeeper() {}

MockBrokerTopicKeeper::MockBrokerTopicKeeper(const MockBrokerTopicKeeper& other)
    : BrokerTopicKeeper(other)
    , _brokerTopicZkPath(other._brokerTopicZkPath)
{
}

bool MockBrokerTopicKeeper::init(const proto::BuildId& buildId, const ResourceReaderPtr& resourceReader,
                                 const string& clusterName, const string& topicConfigName, const string& topicName,
                                 bool versionControl)
{
    _swiftAdminFacade.reset(new NiceMockSwiftAdminFacade());
    _swiftAdminFacade->init(buildId, resourceReader, clusterName);
    setBeeperCollector(GENERATION_ERROR_COLLECTOR_NAME);
    initBeeperTag(buildId);
    _clusterName = clusterName;
    _topicConfigName = topicConfigName;
    _topicName = topicName;
    _versionControl = versionControl;
    return true;
    // return initBrokerTopicMap(buildId, clusterName, *(resourceReader.get()));
}

}} // namespace build_service::common
