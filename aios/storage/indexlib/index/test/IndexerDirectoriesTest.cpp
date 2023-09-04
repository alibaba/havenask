#include "indexlib/index/IndexerDirectories.h"

#include "indexlib/base/Constant.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace index {

class IndexerDirectoriesTest : public TESTBASE
{
public:
    void setUp() override;
    void tearDown() override;
};

void IndexerDirectoriesTest::setUp() {}
void IndexerDirectoriesTest::tearDown() {}
namespace {
class FakeIndexConfig : public config::IIndexConfig
{
public:
    FakeIndexConfig(std::vector<std::string> paths) : _paths(std::move(paths)) {}

    const std::string& GetIndexType() const override
    {
        static std::string indexTypeV2 = "fake";
        return indexTypeV2;
    }
    const std::string& GetIndexName() const override
    {
        static std::string indexName = "fake";
        return indexName;
    }
    const std::string& GetIndexCommonPath() const override
    {
        static std::string commonPath;
        return commonPath;
    }
    std::vector<std::string> GetIndexPath() const override { return _paths; }
    std::vector<std::shared_ptr<config::FieldConfig>> GetFieldConfigs() const override { return {}; }
    void Check() const override {}
    void Deserialize(const autil::legacy::Any&, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource&) override
    {
    }
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override {}
    Status CheckCompatible(const config::IIndexConfig* other) const override { return Status::OK(); }
    bool IsDisabled() const override { return false; }

private:
    std::vector<std::string> _paths;
};
} // namespace

TEST_F(IndexerDirectoriesTest, testSimple)
{
    auto rootPath = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", rootPath).Value();
    ASSERT_TRUE(fs);
    auto rootDir = indexlib::file_system::IDirectory::Get(fs);

    std::vector<std::string> path {"fake_index"};
    auto indexConfig = std::make_shared<FakeIndexConfig>(path);
    IndexerDirectories indexerDirectories(indexConfig);

    auto getSegmentDirName = [](segmentid_t segId) { return "segment_" + std::to_string(segId) + "_level_0"; };

    for (segmentid_t segId = 0; segId < 5; ++segId) {
        auto segDir =
            rootDir->MakeDirectory(getSegmentDirName(segId), indexlib::file_system::DirectoryOption()).Value();
        [[maybe_unused]] auto indexDir =
            segDir->MakeDirectory("fake_index", indexlib::file_system::DirectoryOption()).Value();
        ASSERT_TRUE(indexerDirectories.AddSegment(segId, segDir).IsOK());
    }

    auto iterator = indexerDirectories.CreateIndexerDirectoryIterator();
    for (segmentid_t expectedSegId = 0; expectedSegId < 5; ++expectedSegId) {
        auto [segId, dirs] = iterator.Next();
        ASSERT_EQ(expectedSegId, segId);
        ASSERT_EQ(1u, dirs.size());
        auto expectedDirName = getSegmentDirName(expectedSegId);
        ASSERT_EQ(expectedDirName + "/fake_index", dirs[0]->GetLogicalPath());
    }
    auto [segId, dirs] = iterator.Next();
    ASSERT_EQ(INVALID_SEGMENTID, segId);
}

}} // namespace indexlibv2::index
