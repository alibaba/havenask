#include "build_service/common/RemoteVersionCommitter.h"

#include <memory>
#include <string>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/test/unittest.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/VersionMeta.h"
#include "indexlib/indexlib.h"
#include "unittest/unittest.h"

namespace build_service::common {

class MockRemoteVersionCommitter : public RemoteVersionCommitter
{
public:
    MockRemoteVersionCommitter() : RemoteVersionCommitter() {}
    MOCK_METHOD(Status, CommitVersion, (const proto::CommitVersionRequest& request, proto::InformResponse& response),
                (override));
    MOCK_METHOD(Status, InnerGetCommittedVersions,
                (const proto::GetCommittedVersionsRequest& request, proto::InformResponse& response), (override));
};

class RemoteVersionCommitterTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp() override;
    void tearDown() override {}

private:
    proto::PartitionId _pid;
    std::string _indexRoot;
    std::unique_ptr<config::ResourceReader> _resourceReader;
};

void RemoteVersionCommitterTest::setUp()
{
    _pid.mutable_range()->set_from(0);
    _pid.mutable_range()->set_to(65535);
    _pid.mutable_buildid()->set_generationid(1234);
    _pid.mutable_buildid()->set_appname("orc");
    _pid.mutable_buildid()->set_datatable("orc");
    *_pid.add_clusternames() = "orc";

    std::string srcDataPath = GET_TEST_DATA_PATH() + "general_task_test/config";
    std::string tmpDataPath = GET_TEMP_DATA_PATH() + "general_task_test/config";

    std::string jsonStr;
    fslib::util::FileUtil::readFile(srcDataPath + "/build_app.json.template", jsonStr);
    config::BuildServiceConfig bsConfig;
    autil::legacy::FromJsonString(bsConfig, jsonStr);
    bsConfig._indexRoot = GET_TEMP_DATA_PATH();
    _indexRoot = util::IndexPathConstructor::constructIndexPath(bsConfig._indexRoot, _pid);
    fslib::util::FileUtil::mkDir(_indexRoot, true);
    jsonStr = autil::legacy::ToJsonString(bsConfig);
    fslib::util::FileUtil::atomicCopy(srcDataPath, tmpDataPath);
    fslib::util::FileUtil::writeFile(tmpDataPath + "/build_app.json", jsonStr);
    _resourceReader = std::make_unique<config::ResourceReader>(GET_TEMP_DATA_PATH() + "general_task_test/config/");
}

TEST_F(RemoteVersionCommitterTest, testSimple)
{
    MockRemoteVersionCommitter committer;
    RemoteVersionCommitter::InitParam param;
    param.generationId = _pid.buildid().generationid();
    param.dataTable = _pid.buildid().datatable();
    param.clusterName = _pid.clusternames(0);
    param.rangeFrom = _pid.range().from();
    param.rangeTo = _pid.range().to();
    param.configPath = _resourceReader->getConfigPath();
    ASSERT_TRUE(committer.Init(param).IsOK());

    proto::InformResponse response;
    response.set_errorcode(proto::ADMIN_ERROR_NONE);
    EXPECT_CALL(committer, CommitVersion(_, _)).WillOnce(DoAll(SetArgReferee<1>(response), Return(Status::OK())));
    indexlibv2::framework::VersionMeta versionMeta;
    ASSERT_TRUE(committer.Commit(versionMeta).IsOK());

    std::vector<versionid_t> versions({1, 2, 3, 4, 5}), committedVersions;
    response.set_response(autil::legacy::ToJsonString(versions));
    EXPECT_CALL(committer, InnerGetCommittedVersions(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(response), Return(Status::OK())));
    ASSERT_TRUE(committer.GetCommittedVersions(100, committedVersions).IsOK());
    ASSERT_EQ(versions, committedVersions);

    std::string expectedIdentifier = "orc/generation_1234/partition_0_65535";
    ASSERT_EQ(expectedIdentifier, committer._identifier);
}
} // namespace build_service::common
