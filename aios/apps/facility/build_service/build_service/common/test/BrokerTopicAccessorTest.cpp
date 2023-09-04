#include "build_service/common/BrokerTopicAccessor.h"

#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/common/test/FakeBrokerTopicAccessor.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/test/ProtoCreator.h"
#include "build_service/test/unittest.h"

using namespace std;
using namespace testing;
using namespace build_service::proto;
using namespace build_service::config;

namespace build_service { namespace common {

class BrokerTopicAccessorTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

public:
    FakeBrokerTopicAccessorPtr _brokerTopicAccessor;
    ResourceReaderPtr _resourceReader;
    BuildId _buildId;
};

void BrokerTopicAccessorTest::setUp()
{
    _buildId = ProtoCreator::createBuildId("simple2", 1, "app1");
    string fakeZkPath = GET_TEMP_DATA_PATH();
    _brokerTopicAccessor.reset(new FakeBrokerTopicAccessor(_buildId, fakeZkPath));
}

void BrokerTopicAccessorTest::tearDown() {}

TEST_F(BrokerTopicAccessorTest, testRegistAndDeregistTopic)
{
    string configPath = GET_TEST_DATA_PATH() + "admin_test/broker_topic_accessor_test/config";
    _resourceReader.reset(new ResourceReader(configPath));
    SwiftAdminFacade facade;
    string topicName;
    BuildId buildId = _buildId;
    _brokerTopicAccessor->registBrokerTopic("processor", _resourceReader, "cluster1", "topic_full", "");
    _brokerTopicAccessor->registBrokerTopic("builder", _resourceReader, "cluster1", "topic_full", "");
    facade.init(buildId, _resourceReader, "cluster1");
    topicName = facade.getTopicName("topic_full");
    _brokerTopicAccessor->deregistBrokerTopic("processor", topicName);
    _brokerTopicAccessor->registBrokerTopic("processor", _resourceReader, "cluster1", "topic_inc", "");

    _brokerTopicAccessor->registBrokerTopic("processor", _resourceReader, "cluster2", "topic_inc", "");
    _brokerTopicAccessor->registBrokerTopic("processor", _resourceReader, "cluster3", "topic_inc", "");
    _brokerTopicAccessor->registBrokerTopic("processor", _resourceReader, "cluster4", "topic_inc", "");
    ASSERT_TRUE(_brokerTopicAccessor->prepareBrokerTopics());

    ASSERT_TRUE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_full_processed_1_cluster1"));
    ASSERT_TRUE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_processed_1_cluster1"));
    ASSERT_TRUE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_processed_1_cluster2"));
    ASSERT_TRUE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_processed_1_cluster3"));
    ASSERT_TRUE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_1_processed_1_cluster4"));

    facade.init(buildId, _resourceReader, "cluster1");
    topicName = facade.getTopicName("topic_full");
    _brokerTopicAccessor->deregistBrokerTopic("builder", topicName);
    facade.init(buildId, _resourceReader, "cluster4");
    topicName = facade.getTopicName("topic_inc");
    _brokerTopicAccessor->deregistBrokerTopic("processor", topicName);
    ASSERT_TRUE(_brokerTopicAccessor->clearUselessBrokerTopics(false));
    ASSERT_FALSE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_full_processed_1_cluster1"));
    ASSERT_TRUE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_processed_1_cluster1"));
    ASSERT_TRUE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_processed_1_cluster2"));
    ASSERT_TRUE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_processed_1_cluster3"));
    ASSERT_FALSE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_1_processed_1_cluster4"));

    ASSERT_TRUE(_brokerTopicAccessor->clearUselessBrokerTopics(true));
    ASSERT_FALSE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_full_processed_1_cluster1"));
    ASSERT_FALSE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_processed_1_cluster1"));
    ASSERT_FALSE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_processed_1_cluster2"));
    ASSERT_FALSE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_processed_1_cluster3"));
    ASSERT_FALSE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_1_processed_1_cluster4"));
}

TEST_F(BrokerTopicAccessorTest, testProcessedTopicNeedVersionControl)
{
    string configPath = GET_TEST_DATA_PATH() + "admin_test/broker_topic_accessor_test/config_with_safe_write";
    _resourceReader.reset(new ResourceReader(configPath));
    SwiftAdminFacade facade;
    string topicName;
    BuildId buildId = _buildId;
    _brokerTopicAccessor->registBrokerTopic("processor", _resourceReader, "cluster1", "topic_full", "");
    facade.init(buildId, _resourceReader, "cluster1");
    topicName = facade.getTopicName("topic_full");
    ASSERT_TRUE(_brokerTopicAccessor->prepareBrokerTopics());
    ASSERT_TRUE(_brokerTopicAccessor->checkBrokerTopicExist("user_name_service_name_full_processed_1_cluster1", true));
}

}} // namespace build_service::common
