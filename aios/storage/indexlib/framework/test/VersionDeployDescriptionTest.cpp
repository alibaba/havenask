#include "indexlib/framework/VersionDeployDescription.h"

#include "indexlib/file_system/DeployIndexMeta.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class VersionDeployDescriptionTest : public TESTBASE
{
public:
    VersionDeployDescriptionTest() = default;
    ~VersionDeployDescriptionTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(VersionDeployDescriptionTest, TestCheckDeployDone)
{
    std::string content;
    VersionDeployDescription versionDeployDescription;
    versionDeployDescription.rawPath = "pangu://testPath/generation_1659894427/partition_3641_7281/";
    versionDeployDescription.remotePath = "pangu://testPath/generation_1659894427/partition_3641_7281/";
    versionDeployDescription.configPath = "configPath";

    // test check deploy done on legacy format
    content = versionDeployDescription.remotePath;
    EXPECT_TRUE(versionDeployDescription.CheckDeployDone(content));
    content = versionDeployDescription.remotePath + "_str";
    EXPECT_FALSE(versionDeployDescription.CheckDeployDone(content));

    content = versionDeployDescription.remotePath + ":" + versionDeployDescription.rawPath;
    EXPECT_TRUE(versionDeployDescription.CheckDeployDone(content));
    content = versionDeployDescription.rawPath + ":" + versionDeployDescription.remotePath + "_str";
    EXPECT_FALSE(versionDeployDescription.CheckDeployDone(content));

    content = versionDeployDescription.rawPath + ":" + versionDeployDescription.remotePath + ":" +
              versionDeployDescription.configPath;
    EXPECT_TRUE(versionDeployDescription.CheckDeployDone(content));
    content = versionDeployDescription.rawPath + ":" + versionDeployDescription.remotePath + ":" +
              versionDeployDescription.configPath + "_str";
    EXPECT_FALSE(versionDeployDescription.CheckDeployDone(content));

    // test check deploy done on current format
    content = ToJsonString(versionDeployDescription);
    EXPECT_TRUE(versionDeployDescription.CheckDeployDone(content));
    content = ToJsonString(versionDeployDescription) + "_str";
    EXPECT_FALSE(versionDeployDescription.CheckDeployDone(content));

    // test check deploy done on future format
    VersionDeployDescription targetDpDesc = versionDeployDescription;
    targetDpDesc.editionId = VersionDeployDescription::CURRENT_EDITION_ID + 1;
    content = ToJsonString(targetDpDesc);
    EXPECT_FALSE(versionDeployDescription.CheckDeployDone(content));
    EXPECT_FALSE(versionDeployDescription.CheckDeployDoneByEdition(VersionDeployDescription::CURRENT_EDITION_ID + 1,
                                                                   targetDpDesc));
    EXPECT_TRUE(
        versionDeployDescription.CheckDeployDoneByEdition(VersionDeployDescription::CURRENT_EDITION_ID, targetDpDesc));
}

TEST_F(VersionDeployDescriptionTest, TestCheckDeployMetaEquality)
{
    VersionDeployDescription lhs, rhs;
    lhs.rawPath = "rawPath";
    lhs.remotePath = "remotePath";
    lhs.configPath = "configPath";
    rhs.rawPath = "rawPath";
    rhs.remotePath = "remotePath";
    rhs.configPath = "configPath";
    EXPECT_TRUE(lhs.CheckDeployMetaEquality(rhs) && rhs.CheckDeployMetaEquality(lhs));
    rhs.configPath = "configPath2";
    rhs.deployTime = 2000000;
    EXPECT_TRUE(lhs.CheckDeployMetaEquality(rhs) && rhs.CheckDeployMetaEquality(lhs));
    rhs.remotePath = "remotePath2";
    EXPECT_TRUE(!lhs.CheckDeployMetaEquality(rhs) && !rhs.CheckDeployMetaEquality(lhs));
    rhs.remotePath = "remotePath";

    auto deployIndexMeta1 = std::make_shared<indexlib::file_system::DeployIndexMeta>();
    deployIndexMeta1->Append({"/dir1/file1", 100, 100});
    deployIndexMeta1->Append({"/dir1/file2", 100, 200});
    deployIndexMeta1->AppendFinal({"/dir1/finalfile2", 100, 300});
    deployIndexMeta1->AppendFinal({"/dir1/finalfile2", 100, 400});

    auto deployIndexMeta2 = std::make_shared<indexlib::file_system::DeployIndexMeta>();
    deployIndexMeta2->Append({"/dir2/file1", 100, 100});
    deployIndexMeta2->Append({"/dir2/file2", 100, 200});
    deployIndexMeta2->AppendFinal({"/dir2/finalfile2", 100, 300});
    deployIndexMeta2->AppendFinal({"/dir2/finalfile2", 100, 400});

    auto deployIndexMeta3 = std::make_shared<indexlib::file_system::DeployIndexMeta>();
    deployIndexMeta3->Append({"/dir1/file1", 100, 100});
    deployIndexMeta3->Append({"/dir1/file2", 100, 200});
    deployIndexMeta3->AppendFinal({"/dir1/finalfile2", 100, 300});
    deployIndexMeta3->AppendFinal({"/dir1/finalfile2", 100, 400});

    lhs.localDeployIndexMetas = {deployIndexMeta1};
    rhs.localDeployIndexMetas = {deployIndexMeta2};
    EXPECT_TRUE(!lhs.CheckDeployMetaEquality(rhs) && !rhs.CheckDeployMetaEquality(lhs));

    rhs.localDeployIndexMetas = {deployIndexMeta3};
    EXPECT_TRUE(lhs.CheckDeployMetaEquality(rhs) && rhs.CheckDeployMetaEquality(lhs));

    deployIndexMeta3->AppendFinal({"/dir1/finalfile2", 100, 400});
    EXPECT_TRUE(!lhs.CheckDeployMetaEquality(rhs) && !rhs.CheckDeployMetaEquality(lhs));

    deployIndexMeta1->AppendFinal({"/dir1/finalfile2", 100, 400});
    EXPECT_TRUE(lhs.CheckDeployMetaEquality(rhs) && rhs.CheckDeployMetaEquality(lhs));
    rhs.localDeployIndexMetas = {deployIndexMeta2, deployIndexMeta3};
    EXPECT_TRUE(!lhs.CheckDeployMetaEquality(rhs) && !rhs.CheckDeployMetaEquality(lhs));

    rhs.localDeployIndexMetas = {deployIndexMeta3};
    EXPECT_TRUE(lhs.CheckDeployMetaEquality(rhs) && rhs.CheckDeployMetaEquality(lhs));
    deployIndexMeta3->sourceRootPath = "sourceRootPath";
    EXPECT_TRUE(!lhs.CheckDeployMetaEquality(rhs) && !rhs.CheckDeployMetaEquality(lhs));

    deployIndexMeta1->sourceRootPath = "sourceRootPath";
    EXPECT_TRUE(lhs.CheckDeployMetaEquality(rhs) && rhs.CheckDeployMetaEquality(lhs));
    rhs.remoteDeployIndexMetas = {deployIndexMeta2};
    EXPECT_TRUE(!lhs.CheckDeployMetaEquality(rhs) && !rhs.CheckDeployMetaEquality(lhs));
}

TEST_F(VersionDeployDescriptionTest, TestDeserialize)
{
    VersionDeployDescription vp1, vp2;
    std::string rawPath = "/rawPath";
    std::string remotePath = "/remotePath";
    std::string configPath = "/configPath";
    vp1.rawPath = rawPath;
    vp1.remotePath = remotePath;
    vp1.configPath = configPath;
    auto deployIndexMeta1 = std::make_shared<indexlib::file_system::DeployIndexMeta>();
    deployIndexMeta1->Append({"/dir1/file1", 100, 100});
    deployIndexMeta1->Append({"/dir1/file2", 100, 200});
    deployIndexMeta1->AppendFinal({"/dir1/finalfile2", 100, 300});
    deployIndexMeta1->AppendFinal({"/dir1/finalfile2", 100, 400});
    vp1.localDeployIndexMetas = {deployIndexMeta1};

    std::string content = ToJsonString(vp1);
    EXPECT_TRUE(vp2.Deserialize(content));
    EXPECT_TRUE(vp2.SupportFeature(VersionDeployDescription::FeatureType::DEPLOY_TIME));
    EXPECT_TRUE(vp2.SupportFeature(VersionDeployDescription::FeatureType::DEPLOY_META_MANIFEST));
    EXPECT_EQ(1, vp2.editionId);
    EXPECT_TRUE(vp1.CheckDeployMetaEquality(vp2) && vp2.CheckDeployMetaEquality(vp1));

    content += "illegal_strings";
    EXPECT_FALSE(vp2.Deserialize(content));

    // test backward compatibility
    EXPECT_TRUE(vp1.CheckDeployDone(remotePath));
    EXPECT_TRUE(vp1.CheckDeployDone(rawPath + ":" + remotePath));
    EXPECT_TRUE(vp1.CheckDeployDone(rawPath + ":" + remotePath + ":" + configPath));
    EXPECT_FALSE(vp1.CheckDeployDone(remotePath + ":" + rawPath));

    // test deserialize from legacy content
    content = vp1.remotePath + ":" + vp1.rawPath + ":" + vp1.configPath;

    EXPECT_TRUE(!VersionDeployDescription::IsJson(content));
    EXPECT_TRUE(vp2.Deserialize(content));
    EXPECT_EQ(0, vp2.editionId);
    EXPECT_FALSE(vp2.SupportFeature(VersionDeployDescription::FeatureType::DEPLOY_TIME));
    EXPECT_FALSE(vp2.SupportFeature(VersionDeployDescription::FeatureType::DEPLOY_META_MANIFEST));
    EXPECT_TRUE(vp2.localDeployIndexMetas.size() == 0);
    EXPECT_TRUE(vp2.remoteDeployIndexMetas.size() == 0);
}

TEST_F(VersionDeployDescriptionTest, TestJsonize)
{
    std::string versionDpDescStr = R"({
        "config_path": "/configPath",
        "deploy_time": 1000,
        "edition_id": 1,
        "lifecycle_table": {
            "segment_dir_map" : {
                "/dir1/": "hot",
                "/dir2/": "warm",
                "/dir3/": "cold"
            }
        },
        "local_deploy_index_metas":
        [
            {
                "deploy_file_metas":
                [
                    {
                        "file_length": 100,
                        "modify_time": 100,
                        "path": "/dir1/file1"
                    }
                ],
                "final_deploy_file_metas":
                [
                    {
                        "file_length": 100,
                        "modify_time": 300,
                        "path": "/dir1/finalfile2"

                     }
               ],
               "lifecycle": "",
               "source_root_path": "",
               "target_root_path": ""
           }
        ],
        "raw_path": "/rawPath",
        "remote_deploy_index_metas": [],
        "remote_path": "/remotePath"
    })";
    VersionDeployDescription vp1;
    FromJsonString(vp1, versionDpDescStr);
    auto& lifecycleTable = vp1.GetLifecycleTable();
    EXPECT_EQ(lifecycleTable->GetLifecycle("/dir1/"), "hot");
    EXPECT_EQ(lifecycleTable->GetLifecycle("/dir2/"), "warm");
    EXPECT_EQ(lifecycleTable->GetLifecycle("/dir3/"), "cold");
    VersionDeployDescription vp2;
    std::string versiondpDescStr2 = ToJsonString(vp1);
    FromJsonString(vp2, versionDpDescStr);
    std::string vp2Str = ToJsonString(vp2);
    ASSERT_EQ(versiondpDescStr2, vp2Str);
    ASSERT_TRUE(vp1.CheckDeployDone(vp2Str));

    vp2.lifecycleTable->AddDirectory("/dir1/", "warm");
    vp2Str = ToJsonString(vp2);
    ASSERT_FALSE(vp1.CheckDeployDone(vp2Str));
}

} // namespace indexlibv2::framework
