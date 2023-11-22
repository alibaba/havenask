#include "build_service/common/SwiftResourceKeeper.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/EnvUtil.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/BrokerTopicAccessor.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/common/SwiftParam.h"
#include "build_service/common_define.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SwiftConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/test/unittest.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/test/FakeSwiftClientCreator.h"
#include "build_service/util/test/FakeSwiftWriter.h"
#include "swift/client/SwiftClient.h"
#include "swift/client/SwiftReader.h"
#include "swift/client/SwiftWriter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace build_service::common;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;

namespace build_service { namespace common {

class SwiftResourceKeeperTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

private:
    PartitionId _pid;
    string _indexRoot;
    string _configPath;
    ResourceContainerPtr _container;
    KeyValueMap _params;
};

void SwiftResourceKeeperTest::setUp()
{
    _pid.set_role(ROLE_PROCESSOR);
    _pid.set_step(BUILD_STEP_FULL);
    _pid.mutable_range()->set_from(0);
    _pid.mutable_range()->set_to(32767);
    _pid.mutable_buildid()->set_appname("app");
    _pid.mutable_buildid()->set_generationid(1);
    _pid.mutable_buildid()->set_datatable("simple");
    *_pid.add_clusternames() = "cluster4";
    _indexRoot = GET_TEMP_DATA_PATH();
    _configPath = GET_TEST_DATA_PATH() + "/resource_keeper_test";
    _container.reset(new ResourceContainer());
    {
        ConfigReaderAccessorPtr configResource(new ConfigReaderAccessor(_pid.buildid().datatable()));
        _container->addResource(configResource);
        CheckpointAccessorPtr checkpointAccessor(new CheckpointAccessor);
        _container->addResource(checkpointAccessor);
        // BrokerTopicAccessorPtr brokerTopicAccessor(new FakeBrokerTopicAccessor(_buildId, _indexRoot));
        BrokerTopicAccessorPtr brokerTopicAccessor(new BrokerTopicAccessor(_pid.buildid()));
        _container->addResource(brokerTopicAccessor);
    }
    _params["topicConfigName"] = "topic_full";
    _params[config::SWIFT_FILTER_MASK] = "1";
    _params[config::SWIFT_FILTER_RESULT] = "1";
    _params["clusterName"] = "cluster4";
    _params["tag"] = "tag1";
    _params["name"] = "resource1";
    _params[PROCESSED_DOC_SWIFT_ROOT] = "default_root";
}

void SwiftResourceKeeperTest::tearDown() {}

TEST_F(SwiftResourceKeeperTest, testInit)
{
    {
        tearDown();
        setUp();
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        _params["clusterName"] = "";
        ASSERT_FALSE(keeper->init(_params));
        _params["clusterName"] = "cluster4";
        _params["name"] = "";
        ASSERT_FALSE(keeper->init(_params));
        _params["name"] = "resource1";
        ASSERT_TRUE(keeper->init(_params));
        ASSERT_EQ(_params["clusterName"], keeper->_clusterName);
        ASSERT_EQ(_params["topicConfigName"], keeper->_topicConfigName);
        ASSERT_EQ(_params["tag"], keeper->_tag);
        ASSERT_EQ(_params["name"], keeper->_resourceName);
    }
    {
        // legacy
        string clusterName = "cluster4";
        {
            tearDown();
            setUp();
            // test init fail
            ResourceReaderPtr reader1(new ResourceReader(_configPath + "/invalid_config"));
            SwiftResourceKeeperPtr keeper1(new SwiftResourceKeeper("name", "swift", _container));
            ASSERT_FALSE(keeper1->initLegacy(clusterName, _pid, reader1, _params));
            ASSERT_EQ("", keeper1->_topicName);
            ASSERT_EQ("", keeper1->_swiftRoot);
        }
        {
            tearDown();
            setUp();
            // test init success
            ResourceReaderPtr reader2(new ResourceReader(_configPath + "/default_config"));
            SwiftResourceKeeperPtr keeper2(new SwiftResourceKeeper("cluster4", "swift", _container));
            ASSERT_TRUE(keeper2->initLegacy(clusterName, _pid, reader2, _params));
            ASSERT_EQ(clusterName, keeper2->_clusterName);
            ASSERT_EQ(FULL_SWIFT_BROKER_TOPIC_CONFIG, keeper2->_topicConfigName);
            SwiftAdminFacade facade;
            ASSERT_TRUE(facade.init(_pid.buildid(), reader2, "cluster4"));
            ASSERT_EQ(facade.getTopicName(FULL_SWIFT_BROKER_TOPIC_CONFIG), keeper2->_topicName);
            ASSERT_EQ(_params[PROCESSED_DOC_SWIFT_ROOT], keeper2->_swiftRoot);
            ASSERT_EQ(clusterName, keeper2->_resourceName);
        }
        {
            tearDown();
            setUp();
            // test shared cluster
            string workerCluster = "cluster1";
            ResourceReaderPtr reader3(new ResourceReader(_configPath + "/default_config"));
            SwiftResourceKeeperPtr keeper3(new SwiftResourceKeeper(workerCluster, "swift", _container));
            _params[SHARED_TOPIC_CLUSTER_NAME] = "cluster4";
            ASSERT_TRUE(keeper3->initLegacy(workerCluster, _pid, reader3, _params));
            ASSERT_EQ(clusterName, keeper3->_clusterName);
            ASSERT_EQ(FULL_SWIFT_BROKER_TOPIC_CONFIG, keeper3->_topicConfigName);
            SwiftAdminFacade facade;
            ASSERT_TRUE(facade.init(_pid.buildid(), reader3, "cluster4"));
            ASSERT_EQ(facade.getTopicName(FULL_SWIFT_BROKER_TOPIC_CONFIG), keeper3->_topicName);
            ASSERT_EQ(_params[PROCESSED_DOC_SWIFT_ROOT], keeper3->_swiftRoot);
            ASSERT_EQ(workerCluster, keeper3->_resourceName);
        }
    }
}

TEST_F(SwiftResourceKeeperTest, testJsonize)
{
    {
        tearDown();
        setUp();
        // normal
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        ASSERT_TRUE(keeper->init(_params));
        string jsonStr = ToJsonString(*keeper);
        SwiftResourceKeeperPtr keeper2(new SwiftResourceKeeper("name", "swift", _container));
        FromJsonString(*keeper2, jsonStr);
        ASSERT_TRUE(keeper->_topicConfigName == keeper2->_topicConfigName);
        ASSERT_TRUE(keeper->_clusterName == keeper2->_clusterName);
        ASSERT_TRUE(keeper->_tag == keeper2->_tag);
        ASSERT_TRUE(keeper->_resourceName == keeper2->_resourceName);
        ASSERT_TRUE(keeper->_topicName == keeper2->_topicName);
        ASSERT_TRUE(keeper->_swiftRoot == keeper2->_swiftRoot);
    }
    {
        tearDown();
        setUp();
        // legacy
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        ResourceReaderPtr reader(new ResourceReader(_configPath + "/default_config"));
        string clusterName = "cluster4";
        ASSERT_TRUE(keeper->initLegacy(clusterName, _pid, reader, _params));
        string jsonStr = ToJsonString(*keeper);
        SwiftResourceKeeperPtr keeper2(new SwiftResourceKeeper("name", "swift", _container));
        FromJsonString(*keeper2, jsonStr);
        ASSERT_TRUE(keeper->_clusterName == keeper2->_clusterName);
        ASSERT_TRUE(keeper->_topicConfigName == keeper2->_topicConfigName);
        ASSERT_TRUE(keeper->_tag == keeper2->_tag);
        ASSERT_TRUE(keeper->_resourceName == keeper2->_resourceName);
        ASSERT_TRUE(keeper->_topicName == keeper2->_topicName);
        ASSERT_TRUE(keeper->_swiftRoot == keeper2->_swiftRoot);
    }
}

TEST_F(SwiftResourceKeeperTest, testFunc)
{
    {
        tearDown();
        setUp();
        // test getTopicClusterName()
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        KeyValueMap param;
        ASSERT_EQ("cluster1", keeper->getTopicClusterName("cluster1", param));
        param[SHARED_TOPIC_CLUSTER_NAME] = "share";
        ASSERT_EQ("share", keeper->getTopicClusterName("cluster1", param));
    }
    {
        tearDown();
        setUp();
        // test getLegacyTopicName()
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        KeyValueMap param;
        param[PROCESSED_DOC_SWIFT_ROOT] = "swift_root_1";
        param[PROCESSED_DOC_SWIFT_TOPIC_NAME] = "swift_topic_1";
        ResourceReaderPtr reader(new ResourceReader(_configPath + "/default_config"));
        auto topicInfo =
            keeper->getLegacyTopicName(_params["clusterName"], _pid, reader, _params["topicConfigName"], param);
        ASSERT_EQ(param[PROCESSED_DOC_SWIFT_TOPIC_NAME], topicInfo.first);
        ASSERT_EQ(param[PROCESSED_DOC_SWIFT_ROOT], topicInfo.second);
        // test no clusterName
        param[PROCESSED_DOC_SWIFT_TOPIC_NAME] = "";
        topicInfo = keeper->getLegacyTopicName(_params["clusterName"], _pid, reader, _params["topicConfigName"], param);
        ASSERT_EQ("", topicInfo.first);
        // test getTopicName from swfitAdminFacade
        ASSERT_TRUE(keeper->init(_params));
        topicInfo = keeper->getLegacyTopicName(_params["clusterName"], _pid, reader, _params["topicConfigName"], param);
        SwiftAdminFacade facade;
        ASSERT_TRUE(facade.init(_pid.buildid(), reader, "cluster4"));
        ASSERT_EQ(facade.getTopicName(FULL_SWIFT_BROKER_TOPIC_CONFIG), topicInfo.first);
        ASSERT_EQ(param[PROCESSED_DOC_SWIFT_ROOT], topicInfo.second);
    }
    {
        tearDown();
        setUp();
        // test getSwiftConfig
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        ASSERT_TRUE(keeper->init(_params));
        ResourceReaderPtr reader1(new ResourceReader(_configPath + "/invalid_config"));
        SwiftConfig config;
        ASSERT_FALSE(keeper->getSwiftConfig(reader1, config));
        ResourceReaderPtr reader2(new ResourceReader(_configPath + "/default_config"));
        ASSERT_TRUE(keeper->getSwiftConfig(reader2, config));
    }
}

TEST_F(SwiftResourceKeeperTest, testPrepareResource)
{
    {
        tearDown();
        setUp();
        // test no _topicAccessor
        ResourceContainerPtr container;
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", container));
        ASSERT_TRUE(keeper->init(_params));
        ResourceReaderPtr reader(new ResourceReader(_configPath + "/invalid_config"));
        ASSERT_FALSE(keeper->prepareResource("task1", reader));
    }
    {
        tearDown();
        setUp();
        // test get topicName fail
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        ASSERT_TRUE(keeper->init(_params));
        ResourceReaderPtr reader(new ResourceReader(_configPath + "/invalid_config"));
        ASSERT_FALSE(keeper->prepareResource("task1", reader));
    }
    {
        tearDown();
        setUp();
        // test topicName change
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        ASSERT_TRUE(keeper->init(_params));
        ResourceReaderPtr reader(new ResourceReader(_configPath + "/default_config"));
        ASSERT_TRUE(keeper->prepareResource("task1", reader));
        SwiftAdminFacade facade;
        ASSERT_TRUE(facade.init(_pid.buildid(), reader, "cluster4"));
        ASSERT_EQ(facade.getTopicName(FULL_SWIFT_BROKER_TOPIC_CONFIG, _params["tag"]), keeper->_topicName);
        string configSwiftRoot = "zfs://root/";
        ASSERT_EQ(configSwiftRoot, keeper->_swiftRoot);
        keeper->_topicName = "invalid_topic";
        ASSERT_FALSE(keeper->prepareResource("task2", reader));
    }
}

TEST_F(SwiftResourceKeeperTest, testOnWorker)
{
    {
        tearDown();
        setUp();
        // test createSwiftClient
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        ASSERT_TRUE(keeper->init(_params));
        ResourceReaderPtr reader1(new ResourceReader(_configPath + "/invalid_config"));
        FakeSwiftClientCreatorPtr creator(new FakeSwiftClientCreator(true, true));
        swift::client::SwiftClient* client =
            keeper->createSwiftClient(creator, reader1); // delete when ~SwiftClientCreator
        ASSERT_EQ(nullptr, client);
        ResourceReaderPtr reader2(new ResourceReader(_configPath + "/default_config"));
        client = keeper->createSwiftClient(creator, reader2);
        ASSERT_NE(nullptr, client);
    }
    {
        tearDown();
        setUp();
        // test createSwiftReader
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        ASSERT_TRUE(keeper->init(_params));
        ResourceReaderPtr reader1(new ResourceReader(_configPath + "/invalid_config"));
        // test invalid config
        FakeSwiftClientCreatorPtr creator1(new FakeSwiftClientCreator(true, true));
        swift::client::SwiftReader* swiftReader =
            keeper->createSwiftReader(creator1, _params, reader1, _pid.range()).reader;
        ASSERT_EQ(nullptr, swiftReader);
        // test creatReader failed
        FakeSwiftClientCreatorPtr creator2(new FakeSwiftClientCreator(false, true));
        ResourceReaderPtr reader2(new ResourceReader(_configPath + "/default_config"));
        swiftReader = keeper->createSwiftReader(creator2, _params, reader2, _pid.range()).reader;
        ASSERT_EQ(nullptr, swiftReader);
        // test normal
        FakeSwiftClientCreatorPtr creator3(new FakeSwiftClientCreator(true, true));
        swiftReader = keeper->createSwiftReader(creator3, _params, reader2, _pid.range()).reader;
        ASSERT_NE(nullptr, swiftReader);
        delete swiftReader;
    }
    {
        tearDown();
        setUp();
        // test createSwiftWriter
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        ASSERT_TRUE(keeper->init(_params));
        ResourceReaderPtr reader1(new ResourceReader(_configPath + "/invalid_config"));
        // test invalid config
        FakeSwiftClientCreatorPtr creator1(new FakeSwiftClientCreator(true, true));
        DataDescription dataDescription;
        dataDescription[READ_SRC] = "swift";
        dataDescription[READ_SRC_TYPE] = "swift";
        dataDescription["swift_root"] = "zfs://root";
        dataDescription["topic_name"] = "raw_doc";
        swift::client::SwiftWriter* swiftWriter =
            keeper->createSwiftWriter(creator1, _pid.buildid().datatable(), dataDescription, _params, reader1);
        ASSERT_EQ(nullptr, swiftWriter);
        // test createWriter failed
        FakeSwiftClientCreatorPtr creator2(new FakeSwiftClientCreator(true, false));
        ResourceReaderPtr reader2(new ResourceReader(_configPath + "/default_config"));
        swiftWriter =
            keeper->createSwiftWriter(creator2, _pid.buildid().datatable(), dataDescription, _params, reader2);
        ASSERT_EQ(nullptr, swiftWriter);
        // test normal
        FakeSwiftClientCreatorPtr creator3(new FakeSwiftClientCreator(true, true));
        ResourceReaderPtr reader3(new ResourceReader(_configPath + "/default_config"));
        swiftWriter =
            keeper->createSwiftWriter(creator3, _pid.buildid().datatable(), dataDescription, _params, reader3);
        ASSERT_NE(nullptr, swiftWriter);
        delete (swiftWriter);
    }
}

TEST_F(SwiftResourceKeeperTest, testWriterVersion)
{
    SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
    ASSERT_TRUE(keeper->init(_params));
    ResourceReaderPtr reader1(new ResourceReader(_configPath + "/invalid_config"));
    // test invalid config
    FakeSwiftClientCreatorPtr creator1(new FakeSwiftClientCreator(true, true));
    DataDescription dataDescription;
    dataDescription[READ_SRC] = "swift";
    dataDescription[READ_SRC_TYPE] = "swift";
    dataDescription["swift_root"] = "zfs://root";
    dataDescription["topic_name"] = "raw_doc";
    // test writer version
    FakeSwiftClientCreatorPtr creator(new FakeSwiftClientCreator(true, true));
    ResourceReaderPtr reader(new ResourceReader(_configPath + "/default_config"));
    _params[SWIFT_MAJOR_VERSION] = "12";
    _params[SWIFT_MINOR_VERSION] = "123";
    _params[SWIFT_PROCESSOR_WRITER_NAME] = "processor_0_65535";
    auto swiftWriter = keeper->createSwiftWriter(creator, _pid.buildid().datatable(), dataDescription, _params, reader);
    ASSERT_NE(nullptr, swiftWriter);
    auto fakeWriter = dynamic_cast<FakeSwiftWriter*>(swiftWriter);

    ASSERT_EQ("topicName=;maxBufferSize=86973087744;functionChain=hashId2partId;mode=async|safe;writerName=processor_0_"
              "65535;majorVersion=12;"
              "minorVersion=123",
              fakeWriter->getWriterConfig());
    delete (swiftWriter);
}

TEST_F(SwiftResourceKeeperTest, testNPCMode)
{
    {
        KeyValueMap params;
        params["topicConfigName"] = "topic_raw";
        params["clusterName"] = "cluster1";
        params[config::SWIFT_TOPIC_NAME] = "fake-swift-topic";

        /* add some distraction keys for testing */
        params[config::SWIFT_FILTER_MASK] = "1";
        params[config::SWIFT_FILTER_RESULT] = "1";
        params["tag"] = "tag1";
        params["name"] = "resource1";
        params[PROCESSED_DOC_SWIFT_ROOT] = "default_root";

        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        ASSERT_TRUE(keeper->init(params));
        ASSERT_EQ(params["clusterName"], keeper->_clusterName);
        ASSERT_EQ(params["topicConfigName"], keeper->_topicConfigName);
        ASSERT_EQ(string("fake-swift-topic"), keeper->_topicName);
    }
    {
        KeyValueMap params;
        params["topicConfigName"] = "topic_raw";
        params["clusterName"] = "cluster1";
        params[config::SWIFT_TOPIC_NAME] = "fake-swift-topic";
        params[config::REALTIME_MODE] = config::REALTIME_SERVICE_NPC_MODE;

        /* add some distraction keys for testing */
        params[config::SWIFT_FILTER_MASK] = "1";
        params[config::SWIFT_FILTER_RESULT] = "1";
        params["tag"] = "tag1";
        params["name"] = "resource1";
        params[PROCESSED_DOC_SWIFT_ROOT] = "default_root";
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        ASSERT_TRUE(keeper->init(params));

        ResourceReaderPtr reader(new ResourceReader(_configPath + "/npc_config"));
        config::SwiftConfig swiftConfig;
        ASSERT_TRUE(keeper->getSwiftConfig(reader, swiftConfig));
        auto readerConfig = keeper->getSwiftReaderConfigForNPCMode(swiftConfig, _pid.range());
        EXPECT_EQ(string("topicName=fake-swift-topic;uint8FilterMask=2;uint8MaskResult=2;from=0;to=32767;"),
                  readerConfig);
    }
    {
        autil::EnvGuard guard("raw_topic_swift_reader_config", "oneRequestReadCount=200000;readBufferSize=300000");
        KeyValueMap params;
        params["topicConfigName"] = "topic_raw";
        params["clusterName"] = "cluster1";
        params[config::SWIFT_TOPIC_NAME] = "fake-swift-topic";
        params[config::REALTIME_MODE] = config::REALTIME_SERVICE_NPC_MODE;
        /* add some distraction keys for testing */
        params[config::SWIFT_FILTER_MASK] = "1";
        params[config::SWIFT_FILTER_RESULT] = "1";
        params["tag"] = "tag1";
        params["name"] = "resource1";
        params[PROCESSED_DOC_SWIFT_ROOT] = "default_root";
        SwiftResourceKeeperPtr keeper(new SwiftResourceKeeper("name", "swift", _container));
        ASSERT_TRUE(keeper->init(params));

        ResourceReaderPtr configReader(new ResourceReader(_configPath + "/npc_config"));
        config::SwiftConfig swiftConfig;
        ASSERT_TRUE(keeper->getSwiftConfig(configReader, swiftConfig));
        auto readerConfigStr = keeper->getSwiftReaderConfigForNPCMode(swiftConfig, _pid.range());
        EXPECT_EQ(string("topicName=fake-swift-topic;uint8FilterMask=2;uint8MaskResult=2;oneRequestReadCount=200000;"
                         "readBufferSize=300000;from=0;to=32767;"),
                  readerConfigStr);

        FakeSwiftClientCreatorPtr creator(new FakeSwiftClientCreator(true, true));
        SwiftParam swiftParam = keeper->createSwiftReader(creator, params, configReader, _pid.range());
        std::unique_ptr<swift::client::SwiftReader> swiftReader(swiftParam.reader);
        ASSERT_NE(nullptr, swiftParam.swiftClient);
        ASSERT_NE(nullptr, swiftReader);
        EXPECT_EQ(string("fake-swift-topic"), swiftParam.topicName);
        EXPECT_EQ(false, swiftParam.isMultiTopic);
        EXPECT_EQ(false, swiftParam.disableSwiftMaskFilter);

        const auto& maskPairs = swiftParam.maskFilterPairs;
        ASSERT_EQ(1, maskPairs.size());
        ASSERT_EQ(2, maskPairs[0].first);
        ASSERT_EQ(2, maskPairs[0].second);
        EXPECT_EQ(0, swiftParam.from);
        EXPECT_EQ(32767, swiftParam.to);
    }
}

}} // namespace build_service::common
