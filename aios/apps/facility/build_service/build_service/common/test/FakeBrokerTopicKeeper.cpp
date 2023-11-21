#include "build_service/common/test/FakeBrokerTopicKeeper.h"

#include <iosfwd>
#include <memory>

#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/config/CounterConfig.h"

using namespace std;
using namespace build_service::config;

namespace build_service { namespace common {

FakeBrokerTopicKeeper::~FakeBrokerTopicKeeper() {}

FakeBrokerTopicKeeper::FakeBrokerTopicKeeper(const FakeBrokerTopicKeeper& other)
    : BrokerTopicKeeper(other)
    , _brokerTopicZkPath(other._brokerTopicZkPath)
{
    if (_swiftAdminFacade) {
        _swiftAdminFacade.reset(new FakeSwiftAdminFacade(*((FakeSwiftAdminFacade*)other._swiftAdminFacade.get())));
    }
}

bool FakeBrokerTopicKeeper::init(const proto::BuildId& buildId, const ResourceReaderPtr& resourceReader,
                                 const string& clusterName, const string& topicConfigName, const string& topicName,
                                 bool versionControl)
{
    _swiftAdminFacade.reset(new FakeSwiftAdminFacade(_brokerTopicZkPath));
    _versionControl = versionControl;
    _swiftAdminFacade->init(buildId, resourceReader, clusterName);
    setBeeperCollector(GENERATION_ERROR_COLLECTOR_NAME);
    initBeeperTag(buildId);
    _clusterName = clusterName;
    _topicConfigName = topicConfigName;
    _topicName = topicName;
    return true;
    // return initBrokerTopicMap(buildId, clusterName, *(resourceReader.get()));
}

}} // namespace build_service::common
