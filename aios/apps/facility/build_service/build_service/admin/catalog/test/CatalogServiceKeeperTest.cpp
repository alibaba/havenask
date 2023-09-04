#include "build_service/admin/catalog/CatalogServiceKeeper.h"

#include "build_service/admin/GeneralGenerationTask.h"
#include "build_service/admin/GenerationKeeper.h"
#include "build_service/admin/catalog/ProtoOperator.h"
#include "build_service/admin/catalog/test/FakeServiceKeeper.h"
#include "build_service/test/unittest.h"
#include "catalog/proto/CatalogService.pb.h"
#include "google/protobuf/message.h"
#include "google/protobuf/util/message_differencer.h"

using namespace std;
using namespace testing;

namespace catalog::proto {
bool operator==(const Build::BuildCurrent& lhs, const Build::BuildCurrent& rhs)
{
    google::protobuf::util::MessageDifferencer md;
    return md.Compare(lhs, rhs);
}
} // namespace catalog::proto

namespace {
using google::protobuf::Message;
using google::protobuf::util::MessageDifferencer;
bool compareProto(const google::protobuf::Message& expected, const google::protobuf::Message& actual,
                  bool strict = true, std::string* message = nullptr)
{
    MessageDifferencer md;
    if (!strict) {
        md.set_message_field_comparison(MessageDifferencer::EQUIVALENT);
    }
    if (message != nullptr) {
        md.ReportDifferencesToString(message);
    }
    return md.Compare(expected, actual);
}

} // namespace

namespace build_service::admin {

class MockCatalogServiceKeeper : public CatalogServiceKeeper
{
public:
    MockCatalogServiceKeeper() : CatalogServiceKeeper("", "") {}
    MOCK_METHOD(bool, listBuild, (catalog::proto::ListBuildResponse*), (override));
    MOCK_METHOD(void, updateBuildCurrent, (const catalog::proto::BuildId&, const catalog::proto::Build::BuildCurrent&),
                (override));
};

class CatalogServiceKeeperTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() override
    {
        _catalogBuildId.set_generation_id(_generationId);
        _catalogBuildId.set_partition_name("part1");
        _catalogBuildId.set_table_name("tb1");
        _catalogBuildId.set_database_name("db1");
        _catalogBuildId.set_catalog_name("ct1");
        _buildId = CatalogServiceKeeper::transferBuildId(_catalogBuildId);
        gTestStartBuildRequest.Clear();
        gTestStartBuildResponse.Clear();
        gTestStopBuildRequest.Clear();
        gTestStopBuildResponse.Clear();
        gTestUpdateConfigRequest.Clear();
        gTestUpdateConfigResponse.Clear();
    }

private:
    catalog::proto::ListBuildResponse createListBuildResponse()
    {
        catalog::proto::ListBuildResponse listBuildResponse;
        *listBuildResponse.add_build_ids() = _catalogBuildId;
        auto target = listBuildResponse.add_build_targets();
        target->set_type(catalog::proto::BuildTarget::BATCH_BUILD);
        target->set_build_state(catalog::proto::BuildState::RUNNING);
        target->set_config_path(_configPath);
        return listBuildResponse;
    }

private:
    uint32_t _generationId = 1688489752;
    string _configPath = "dfs://ea120dfssearch1--cn-shanghai/mainse/config/default/1674149352";
    proto::BuildId _buildId;
    catalog::proto::BuildId _catalogBuildId;
};

TEST_F(CatalogServiceKeeperTest, testStartBuild)
{
    auto listBuildResponse = createListBuildResponse();
    catalog::proto::Build::BuildCurrent current;
    current.set_build_state(catalog::proto::BuildState::RUNNING);
    current.set_config_path(_configPath);

    MockCatalogServiceKeeper keeper;
    EXPECT_CALL(keeper, listBuild(_)).Times(2).WillRepeatedly(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
    EXPECT_CALL(keeper, updateBuildCurrent(_catalogBuildId, current));
    keeper.catalogLoop();
    keeper.catalogLoop(); // second loop would not trigger action
    ASSERT_EQ(_configPath, gTestStartBuildRequest.configpath());
    ASSERT_EQ(_buildId, gTestStartBuildRequest.buildid());
    ASSERT_FALSE(gTestStartBuildRequest.has_datadescriptionkvs());
}

TEST_F(CatalogServiceKeeperTest, testStartBuildFail)
{
    auto listBuildResponse = createListBuildResponse();
    std::string errorMessage = "start build failed";
    catalog::proto::Build::BuildCurrent current;
    current.set_build_state(catalog::proto::BuildState::STOPPED);
    current.set_last_error(errorMessage);

    MockCatalogServiceKeeper keeper;
    EXPECT_CALL(keeper, listBuild(_)).WillOnce(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
    EXPECT_CALL(keeper, updateBuildCurrent(_catalogBuildId, current));
    *gTestStartBuildResponse.add_errormessage() = errorMessage;
    gTestStartBuildResponse.set_errorcode(proto::ADMIN_INTERNAL_ERROR);
    keeper.catalogLoop();
    ASSERT_EQ(_configPath, gTestStartBuildRequest.configpath());
    ASSERT_EQ(_buildId, gTestStartBuildRequest.buildid());
    ASSERT_FALSE(gTestStartBuildRequest.has_datadescriptionkvs());
}

TEST_F(CatalogServiceKeeperTest, testStopBuild)
{
    {
        SCOPED_TRACE("not running");
        gTestStopBuildRequest.Clear();
        gTestStopBuildResponse.Clear();
        auto listBuildResponse = createListBuildResponse();
        MockCatalogServiceKeeper keeper;
        EXPECT_CALL(keeper, listBuild(_))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
        EXPECT_CALL(keeper, updateBuildCurrent(_, _));
        keeper.catalogLoop();
        listBuildResponse.mutable_build_targets(0)->set_build_state(catalog::proto::BuildState::STOPPED);
        keeper.catalogLoop();
        proto::StopBuildRequest expectedRequest;
        ASSERT_TRUE(compareProto(expectedRequest, gTestStopBuildRequest));
    }
    {
        SCOPED_TRACE("build_state set to stopped");
        gTestStopBuildRequest.Clear();
        gTestStopBuildResponse.Clear();
        auto listBuildResponse = createListBuildResponse();
        MockCatalogServiceKeeper keeper;
        EXPECT_CALL(keeper, listBuild(_)).WillOnce(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
        EXPECT_CALL(keeper, updateBuildCurrent(_, _));
        keeper.catalogLoop();
        listBuildResponse.mutable_build_targets(0)->set_build_state(catalog::proto::BuildState::STOPPED);
        catalog::proto::Build::BuildCurrent current;
        current.set_build_state(catalog::proto::BuildState::STOPPED);
        auto generationKeeper = make_shared<GenerationKeeper>(
            GenerationKeeper::Param {_buildId, "", "", "", nullptr, nullptr, nullptr, nullptr, nullptr});
        keeper._allGenerationKeepers.emplace(_buildId, generationKeeper);
        EXPECT_CALL(keeper, listBuild(_))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
        EXPECT_CALL(keeper, updateBuildCurrent(_catalogBuildId, current));
        keeper.catalogLoop();
        keeper.catalogLoop(); // second loop would not trigger action
        ASSERT_EQ(_buildId, gTestStopBuildRequest.buildid());
        ASSERT_FALSE(gTestStopBuildRequest.has_configpath());
    }
    {
        SCOPED_TRACE("type set to direct_build");
        gTestStopBuildRequest.Clear();
        gTestStopBuildResponse.Clear();
        auto listBuildResponse = createListBuildResponse();
        MockCatalogServiceKeeper keeper;
        EXPECT_CALL(keeper, listBuild(_)).WillOnce(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
        EXPECT_CALL(keeper, updateBuildCurrent(_, _));
        keeper.catalogLoop();
        listBuildResponse.mutable_build_targets(0)->set_type(catalog::proto::BuildTarget::DIRECT_BUILD);
        catalog::proto::Build::BuildCurrent current;
        current.set_build_state(catalog::proto::BuildState::STOPPED);
        auto generationKeeper = make_shared<GenerationKeeper>(
            GenerationKeeper::Param {_buildId, "", "", "", nullptr, nullptr, nullptr, nullptr, nullptr});
        keeper._allGenerationKeepers.emplace(_buildId, generationKeeper);
        EXPECT_CALL(keeper, listBuild(_))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
        EXPECT_CALL(keeper, updateBuildCurrent(_catalogBuildId, current));
        keeper.catalogLoop();
        keeper.catalogLoop(); // second loop would not trigger action
        ASSERT_EQ(_buildId, gTestStopBuildRequest.buildid());
        ASSERT_FALSE(gTestStopBuildRequest.has_configpath());
    }
    {
        SCOPED_TRACE("build removed from catalog");
        gTestStopBuildRequest.Clear();
        gTestStopBuildResponse.Clear();
        auto listBuildResponse = createListBuildResponse();
        MockCatalogServiceKeeper keeper;
        EXPECT_CALL(keeper, listBuild(_)).WillOnce(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
        EXPECT_CALL(keeper, updateBuildCurrent(_, _));
        keeper.catalogLoop();
        listBuildResponse.clear_build_targets();
        listBuildResponse.clear_build_ids();
        auto generationKeeper = make_shared<GenerationKeeper>(
            GenerationKeeper::Param {_buildId, "", "", "", nullptr, nullptr, nullptr, nullptr, nullptr});
        keeper._activeGenerationKeepers.emplace(_buildId, generationKeeper);
        EXPECT_CALL(keeper, listBuild(_))
            .Times(2)
            .WillRepeatedly(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
        keeper.catalogLoop();
        keeper.catalogLoop(); // second loop would not trigger action
        ASSERT_EQ(_buildId, gTestStopBuildRequest.buildid());
        ASSERT_FALSE(gTestStopBuildRequest.has_configpath());
    }
}

TEST_F(CatalogServiceKeeperTest, testStopBuildFail)
{
    auto listBuildResponse = createListBuildResponse();
    MockCatalogServiceKeeper keeper;
    EXPECT_CALL(keeper, listBuild(_)).WillOnce(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
    EXPECT_CALL(keeper, updateBuildCurrent(_, _));
    keeper.catalogLoop();
    listBuildResponse.mutable_build_targets(0)->set_build_state(catalog::proto::BuildState::STOPPED);
    catalog::proto::Build::BuildCurrent current;
    string errorMessage = "stop build failed";
    current.set_last_error(errorMessage);
    auto generationKeeper = make_shared<GenerationKeeper>(
        GenerationKeeper::Param {_buildId, "", "", "", nullptr, nullptr, nullptr, nullptr, nullptr});
    keeper._allGenerationKeepers.emplace(_buildId, generationKeeper);
    EXPECT_CALL(keeper, listBuild(_)).WillOnce(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
    EXPECT_CALL(keeper, updateBuildCurrent(_catalogBuildId, current));
    *gTestStopBuildResponse.add_errormessage() = errorMessage;
    gTestStopBuildResponse.set_errorcode(proto::ADMIN_INTERNAL_ERROR);
    keeper.catalogLoop();
    ASSERT_EQ(_buildId, gTestStopBuildRequest.buildid());
    ASSERT_FALSE(gTestStopBuildRequest.has_configpath());
}

TEST_F(CatalogServiceKeeperTest, testUpdateConfig)
{
    auto listBuildResponse = createListBuildResponse();
    MockCatalogServiceKeeper keeper;
    EXPECT_CALL(keeper, listBuild(_)).WillOnce(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
    EXPECT_CALL(keeper, updateBuildCurrent(_, _));
    keeper.catalogLoop();
    string newConfigPath = "dfs://ea120dfssearch1--cn-shanghai/mainse/config/default/1674149353";
    listBuildResponse.mutable_build_targets(0)->set_config_path(newConfigPath);
    catalog::proto::Build::BuildCurrent current;
    current.set_config_path(newConfigPath);
    auto generationKeeper = make_shared<GenerationKeeper>(
        GenerationKeeper::Param {_buildId, "", "", "", nullptr, nullptr, nullptr, nullptr, nullptr});
    generationKeeper->_generationTask = new GeneralGenerationTask(_buildId, nullptr);
    generationKeeper->_generationTask->setConfigPath(_configPath);
    keeper._allGenerationKeepers.emplace(_buildId, generationKeeper);
    EXPECT_CALL(keeper, listBuild(_)).Times(2).WillRepeatedly(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
    EXPECT_CALL(keeper, updateBuildCurrent(_catalogBuildId, current));
    keeper.catalogLoop();
    keeper.catalogLoop(); // second loop would not trigger action
    ASSERT_EQ(_buildId, gTestUpdateConfigRequest.buildid());
    ASSERT_EQ(newConfigPath, gTestUpdateConfigRequest.configpath());
}

TEST_F(CatalogServiceKeeperTest, testUpdateConfigFail)
{
    auto listBuildResponse = createListBuildResponse();
    MockCatalogServiceKeeper keeper;
    EXPECT_CALL(keeper, listBuild(_)).WillOnce(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
    EXPECT_CALL(keeper, updateBuildCurrent(_, _));
    keeper.catalogLoop();
    string newConfigPath = "dfs://ea120dfssearch1--cn-shanghai/mainse/config/default/1674149353";
    listBuildResponse.mutable_build_targets(0)->set_config_path(newConfigPath);
    string errorMessage = "update config failed";
    catalog::proto::Build::BuildCurrent current;
    current.set_last_error(errorMessage);
    auto generationKeeper = make_shared<GenerationKeeper>(
        GenerationKeeper::Param {_buildId, "", "", "", nullptr, nullptr, nullptr, nullptr, nullptr});
    generationKeeper->_generationTask = new GeneralGenerationTask(_buildId, nullptr);
    generationKeeper->_generationTask->setConfigPath(_configPath);
    keeper._allGenerationKeepers.emplace(_buildId, generationKeeper);
    EXPECT_CALL(keeper, listBuild(_)).WillOnce(DoAll(SetArgPointee<0>(listBuildResponse), Return(true)));
    EXPECT_CALL(keeper, updateBuildCurrent(_catalogBuildId, current));
    *gTestUpdateConfigResponse.add_errormessage() = errorMessage;
    gTestUpdateConfigResponse.set_errorcode(proto::ADMIN_INTERNAL_ERROR);
    keeper.catalogLoop();
    ASSERT_EQ(_buildId, gTestUpdateConfigRequest.buildid());
    ASSERT_EQ(newConfigPath, gTestUpdateConfigRequest.configpath());
}

} // namespace build_service::admin
