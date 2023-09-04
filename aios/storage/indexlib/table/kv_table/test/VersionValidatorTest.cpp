#include "indexlib/framework/VersionValidator.h"

#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/Version.h"
#include "indexlib/table/kv_table/KVSchemaResolver.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlibv2::config;
using namespace indexlibv2::framework;
using namespace indexlib::file_system;

namespace indexlibv2::table {

class VersionValidatorTest : public TESTBASE
{
public:
    VersionValidatorTest() = default;
    ~VersionValidatorTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    shared_ptr<TabletOptions> CreateTabletOptions();
    void PrepareIndex(versionid_t& versionId);
    void GetFileDetails(const shared_ptr<Directory>& dir, vector<pair<shared_ptr<Directory>, string>>& fileDetails);
    void GetFileDetails(const shared_ptr<Directory>& dir, const vector<string>& targetFiles,
                        vector<pair<shared_ptr<Directory>, string>>& fileDetails);
    bool IsEndWith(const string& name, const string& suffix);

private:
    shared_ptr<TabletSchema> _schema;
    shared_ptr<TabletOptions> _options;
    IndexRoot _indexRoot;
};

void VersionValidatorTest::setUp()
{
    _schema.reset(framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "order_kv.json").release());
    ASSERT_TRUE(_schema);
    _options = CreateTabletOptions();
    ASSERT_TRUE(_options);
    string rootDir = GET_TEMP_DATA_PATH() + "/test";
    _indexRoot = IndexRoot(rootDir, rootDir);
}

void VersionValidatorTest::tearDown() {}

shared_ptr<TabletOptions> VersionValidatorTest::CreateTabletOptions()
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 1,
            "level_num" : 1
        }
    }
    } )";
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");
    return tabletOptions;
}

void VersionValidatorTest::PrepareIndex(versionid_t& versionId)
{
    KVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _options).IsOK());
    string docs =
        "cmd=add,biz_order_id=1,out_order_id=111111111111111111111111111111111111111,seller_id=1,ts=1,locator=0:1;"
        "cmd=add,biz_order_id=2,out_order_id=222222222222222222222222222222222222222,seller_id=2,ts=2,locator=0:2;";

    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    TableTestHelper::StepInfo info;
    info.specifyEpochId = "12345";
    info.specifyMaxMergedSegId = -1;
    ASSERT_TRUE(helper.Merge(true /*auto load merge version*/, &info).IsOK());
    const auto& version = helper.GetCurrentVersion();
    ASSERT_EQ(1u, version.GetSegmentCount());
    ASSERT_TRUE(version.HasSegment(0));
    versionId = version.GetVersionId();
    ASSERT_TRUE(VersionValidator::Validate(_indexRoot.GetLocalRoot(), _schema, versionId).IsOK());
}

void VersionValidatorTest::GetFileDetails(const shared_ptr<Directory>& dir,
                                          vector<pair<shared_ptr<Directory>, string>>& fileDetails)
{
    if (!dir) {
        return;
    }
    vector<string> files;
    dir->ListDir("", files);
    for (const auto& f : files) {
        if (!dir->IsDir(f)) {
            fileDetails.emplace_back(dir, f);
        } else {
            auto subDir = dir->GetDirectory(f, true);
            ASSERT_TRUE(subDir);
            GetFileDetails(subDir, fileDetails);
        }
    }
}

void VersionValidatorTest::GetFileDetails(const shared_ptr<Directory>& dir, const vector<string>& targetFiles,
                                          vector<pair<shared_ptr<Directory>, string>>& fileDetails)
{
    if (!dir || targetFiles.empty()) {
        return;
    }
    for (const auto& f : targetFiles) {
        ASSERT_TRUE(dir->IsExist(f));
        if (!dir->IsDir(f)) {
            fileDetails.emplace_back(dir, f);
        } else {
            auto subDir = dir->GetDirectory(f, true);
            ASSERT_TRUE(subDir);
            GetFileDetails(subDir, fileDetails);
        }
    }
}

bool VersionValidatorTest::IsEndWith(const string& name, const string& suffix)
{
    if (suffix.empty() || name.size() < suffix.size()) {
        return false;
    }
    size_t suffixLen = suffix.size();
    size_t namePos = name.size() - 1;
    size_t suffixPos = suffix.size() - 1;
    for (size_t i = 0; i < suffixLen; ++i) {
        if (name[namePos--] != suffix[suffixPos--]) {
            return false;
        }
    }
    return true;
}

TEST_F(VersionValidatorTest, testMissingFile)
{
    versionid_t versionId = -1;
    PrepareIndex(versionId);
    ASSERT_TRUE(versionId != -1);
    auto dir = Directory::GetPhysicalDirectory(_indexRoot.GetLocalRoot());
    ASSERT_TRUE(dir);
    vector<string> targetFiles = {"entry_table.0", "index_format_version", "schema.json", "segment_0_level_1",
                                  "version.536870913"};
    vector<pair<shared_ptr<Directory>, string>> fileDetails;
    GetFileDetails(dir, targetFiles, fileDetails);
    string tmpName = "__aa_bb__";

    set<string> removeOkFiles = {"entry_table.0", "index_format_version", "schema.json", "counter"};
    for (auto [directory, fileName] : fileDetails) {
        ASSERT_TRUE(VersionValidator::Validate(_indexRoot.GetLocalRoot(), _schema, versionId).IsOK());
        directory->Rename(fileName, directory, tmpName);
        bool ok = VersionValidator::Validate(_indexRoot.GetLocalRoot(), _schema, versionId).IsOK();
        if (removeOkFiles.find(fileName) == removeOkFiles.end()) {
            ASSERT_FALSE(ok);
        } else {
            ASSERT_TRUE(ok);
        }
        if (directory->IsExist(fileName)) {
            directory->RemoveFile(tmpName);
        } else {
            directory->Rename(tmpName, directory, fileName);
        }
    }
}

TEST_F(VersionValidatorTest, testBrokenFile)
{
    versionid_t versionId = -1;
    PrepareIndex(versionId);
    ASSERT_TRUE(versionId != -1);
    auto dir = Directory::GetPhysicalDirectory(_indexRoot.GetLocalRoot());
    ASSERT_TRUE(dir);
    vector<string> targetFiles = {"entry_table.0", "index_format_version", "schema.json", "segment_0_level_1",
                                  "version.536870913"};
    vector<pair<shared_ptr<Directory>, string>> fileDetails;
    GetFileDetails(dir, targetFiles, fileDetails);
    string tmpName = "__aa_bb__";
    vector<string> brokenOkSuffix = {"entry_table.0", "index_format_version", "schema.json", "counter",
                                     "index/biz_order_id/value"};
    // skip some file, otherwise would coredump
    set<string> skipFile = {"value.__compress_info__", "value.__compress_meta__"};
    for (auto [directory, fileName] : fileDetails) {
        size_t len = directory->GetFileLength(fileName);
        if (!len || skipFile.find(fileName) != skipFile.end()) {
            continue;
        }
        ASSERT_TRUE(VersionValidator::Validate(_indexRoot.GetLocalRoot(), _schema, versionId).IsOK());
        directory->RemoveFile(tmpName);
        ASSERT_TRUE(FslibWrapper::Copy(directory->GetPhysicalPath("") + "/" + fileName,
                                       directory->GetPhysicalPath("") + "/" + tmpName)
                        .OK());
        ASSERT_TRUE(FslibWrapper::Store(directory->GetPhysicalPath("") + "/" + fileName, string(len, 'a')).OK());
        bool ok = VersionValidator::Validate(_indexRoot.GetLocalRoot(), _schema, versionId).IsOK();
        string fullName = directory->GetLogicalPath() + "/" + fileName;
        bool brokenOk = false;
        for (const auto& suffix : brokenOkSuffix) {
            if (IsEndWith(fullName, suffix)) {
                brokenOk = true;
                break;
            }
        }
        ASSERT_EQ(brokenOk, ok);
        ASSERT_TRUE(FslibWrapper::DeleteFile(directory->GetPhysicalPath("") + "/" + fileName, {}).OK());
        ASSERT_TRUE(FslibWrapper::Rename(directory->GetPhysicalPath("") + "/" + tmpName,
                                         directory->GetPhysicalPath("") + "/" + fileName)
                        .OK());
    }
}

} // namespace indexlibv2::table
