#include "autil/StringUtil.h"
#include "build_service/common/test/MockSwiftAdminFacade.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/test/unittest.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/testlib/MockSwiftAdminAdapter.h"

using namespace std;
using namespace std::tr1;
using namespace std::placeholders;
using namespace autil;
using namespace testing;

using namespace swift::protocol;
using namespace swift::client;
using namespace swift::testlib;

using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::proto;

namespace build_service { namespace common {

#define NEW_SWIFT_ADMIN_ADAPTER()                                                                                      \
    std::shared_ptr<NiceMockSwiftAdminAdapter> swiftAdapter(new NiceMockSwiftAdminAdapter());                          \
    EXPECT_CALL(*_swiftAdminFacade, createSwiftAdminAdapter(_)).WillOnce(Return(swiftAdapter));

class SwiftAdminFacadeTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

protected:
    static bool collectCreateTopic(const TopicCreationRequest* request, vector<TopicCreationRequest>* allRequest,
                                   const string& createFailedTopicName = "");

    static bool collectDeleteTopic(const TopicDeletionRequest* request, vector<TopicDeletionRequest>* allRequest);

protected:
    proto::BuildId _buildId;
    string _configPath;
};

void SwiftAdminFacadeTest::setUp()
{
    _buildId.set_generationid(10);
    _buildId.set_datatable("simple");

    _configPath = GET_TEST_DATA_PATH() + "/swift_admin_facade_test/config";
    setenv(BS_ENV_ADMIN_SWIFT_TIMEOUT.c_str(), "5000", 1);
}

void SwiftAdminFacadeTest::tearDown() {}

ACTION_P(SetErrorCode, ec) { return ec; }

ACTION_P(collectTopic, allRequest) { allRequest->push_back(arg0); }

TEST_F(SwiftAdminFacadeTest, testGetTopicName)
{
    string configPath = GET_TEST_DATA_PATH() + "admin_test/config";
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(configPath);
    BuildId buildId;
    buildId.set_generationid(100);
    SwiftAdminFacade facade;
    ASSERT_TRUE(facade.init(buildId, resourceReader, "cluster1"));
    string topicName = facade.getTopicName(BS_TOPIC_INC, "");
    EXPECT_EQ("user_name_service_name_processed_100_cluster1", topicName);

    string newConfigPath = GET_TEST_DATA_PATH() + "admin_test/config_change_schema";
    ResourceReaderPtr resourceReader2 = ResourceReaderManager::getResourceReader(newConfigPath);
    SwiftAdminFacade facade2;
    ASSERT_TRUE(facade2.init(buildId, resourceReader2, "cluster1"));
    topicName = facade2.getTopicName(BS_TOPIC_INC, "");
    EXPECT_EQ("user_name_service_name_1_processed_100_cluster1", topicName);

    // test realtime topic name
    auto schema = resourceReader2->getSchema("cluster1");
    schema->SetSchemaVersionId(10);
    topicName = SwiftAdminFacade::getRealtimeTopicName("user_name_service_name", buildId, "cluster1", schema);
    EXPECT_EQ("user_name_service_name_10_processed_100_cluster1", topicName);
}

TEST_F(SwiftAdminFacadeTest, testCreateTopic)
{
    std::shared_ptr<NiceMockSwiftAdminAdapter> swiftAdapter(new NiceMockSwiftAdminAdapter());
    EXPECT_CALL(*swiftAdapter, createTopic(_))
        .WillOnce(SetErrorCode(swift::protocol::ERROR_NONE))
        .WillOnce(SetErrorCode(swift::protocol::ERROR_WORKER_DEAD))
        .WillOnce(Return(ERROR_UNKNOWN));

    SwiftAdminFacade obj;
    EXPECT_TRUE(obj.createTopic(swiftAdapter, "topic", config::SwiftTopicConfig(), false));
    EXPECT_FALSE(obj.createTopic(swiftAdapter, "topic", config::SwiftTopicConfig(), false));
    EXPECT_FALSE(obj.createTopic(swiftAdapter, "topic", config::SwiftTopicConfig(), false));
}

TEST_F(SwiftAdminFacadeTest, testDeleteTopic)
{
    std::shared_ptr<NiceMockSwiftAdminAdapter> swiftAdapter(new NiceMockSwiftAdminAdapter());
    EXPECT_CALL(*swiftAdapter, deleteTopic(_))
        .WillOnce(SetErrorCode(swift::protocol::ERROR_NONE))
        .WillOnce(SetErrorCode(swift::protocol::ERROR_WORKER_DEAD))
        .WillOnce(Return(ERROR_UNKNOWN));

    SwiftAdminFacade obj;
    EXPECT_TRUE(obj.deleteTopic(swiftAdapter, "topic"));
    EXPECT_FALSE(obj.deleteTopic(swiftAdapter, "topic"));
    EXPECT_FALSE(obj.deleteTopic(swiftAdapter, "topic"));
}

TEST_F(SwiftAdminFacadeTest, testUpdateWriterVersion)
{
    std::shared_ptr<NiceMockSwiftAdminAdapter> swiftAdapter(new NiceMockSwiftAdminAdapter());
    std::string topicName = "simple";
    uint32_t majorVersion = 123;
    std::vector<std::pair<std::string, uint32_t>> minorVersions = {{"role1", 1}, {"role2", 0}};
    TopicWriterVersionInfo writerVersionInfo;
    writerVersionInfo.set_topicname(topicName);
    writerVersionInfo.set_majorversion(majorVersion);
    auto versions = writerVersionInfo.mutable_writerversions();
    for (auto [name, version] : minorVersions) {
        WriterVersion writerVersion;
        writerVersion.set_name(name);
        writerVersion.set_version(version);
        *(versions->Add()) = writerVersion;
    }
    auto compareFunc = [writerVersionInfo](const auto& result) {
        ASSERT_EQ(writerVersionInfo.majorversion(), result.majorversion());
        ASSERT_EQ(writerVersionInfo.topicname(), result.topicname());
        ASSERT_EQ(writerVersionInfo.writerversions().size(), result.writerversions().size());
    };
    EXPECT_CALL(*swiftAdapter, updateWriterVersion(_))
        .WillOnce(
            DoAll(Invoke(std::bind(compareFunc, std::placeholders::_1)), SetErrorCode(swift::protocol::ERROR_NONE)))
        .WillOnce(SetErrorCode(swift::protocol::ERROR_WORKER_DEAD))
        .WillOnce(Return(ERROR_UNKNOWN));

    SwiftAdminFacade obj;
    EXPECT_TRUE(obj.updateWriterVersion(swiftAdapter, topicName, majorVersion, minorVersions));
    EXPECT_FALSE(obj.updateWriterVersion(swiftAdapter, topicName, majorVersion, minorVersions));
    EXPECT_FALSE(obj.updateWriterVersion(swiftAdapter, topicName, majorVersion, minorVersions));
}

TEST_F(SwiftAdminFacadeTest, testModifyTopicVersionControl)
{
    std::shared_ptr<NiceMockSwiftAdminFacade> facade(new NiceMockSwiftAdminFacade);

    std::shared_ptr<NiceMockSwiftAdminAdapter> swiftAdapter(new NiceMockSwiftAdminAdapter());
    ON_CALL(*facade, createSwiftAdminAdapter(_)).WillByDefault(Return(swiftAdapter));
    std::string topicName = "simple";
    auto compareFunc = [topicName](const auto& request, bool expectedEnable) {
        ASSERT_EQ(topicName, request.topicname());
        ASSERT_TRUE(request.has_versioncontrol());
        ASSERT_EQ(expectedEnable, request.versioncontrol());
    };
    EXPECT_CALL(*swiftAdapter, modifyTopic(_, _))
        .WillOnce(DoAll(Invoke(std::bind(compareFunc, std::placeholders::_1, true)),
                        SetErrorCode(swift::protocol::ERROR_WORKER_DEAD)))
        .WillOnce(DoAll(Invoke(std::bind(compareFunc, std::placeholders::_1, true)),
                        SetErrorCode(swift::protocol::ERROR_NONE)))
        .WillOnce(DoAll(Invoke(std::bind(compareFunc, std::placeholders::_1, false)),
                        SetErrorCode(swift::protocol::ERROR_NONE)));

    EXPECT_FALSE(facade->updateTopicVersionControl("", topicName, true));
    EXPECT_TRUE(facade->updateTopicVersionControl("", topicName, true));
    EXPECT_TRUE(facade->updateTopicVersionControl("", topicName, false));
}

}} // namespace build_service::common
