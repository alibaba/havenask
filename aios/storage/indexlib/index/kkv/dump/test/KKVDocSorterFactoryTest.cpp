#include "indexlib/index/kkv/dump/KKVDocSorterFactory.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;

namespace indexlibv2::index {

class KKVDocSorterFactoryTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = indexlib::file_system::Directory::Get(fs);
        _directory = rootDiretory->MakeDirectory("InlineKKVDataDumperTest", indexlib::file_system::DirectoryOption())
                         ->GetIDirectory();
        ASSERT_NE(nullptr, _directory);
    }

    void tearDown() override {}

protected:
    std::shared_ptr<indexlib::file_system::IDirectory> _directory;
};

TEST_F(KKVDocSorterFactoryTest, testDummy)
{
    auto kkvIndexConfig = std::make_shared<indexlibv2::config::KKVIndexConfig>();
    auto [status, kkvDocSorter] = KKVDocSorterFactory::CreateIfNeccessary(kkvIndexConfig, KKVDumpPhrase::BUILD_DUMP);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(nullptr, kkvDocSorter);
}

TEST_F(KKVDocSorterFactoryTest, testTruncSort)
{
    auto kkvIndexConfig = std::make_shared<indexlibv2::config::KKVIndexConfig>();
    auto suffixFieldConfig = std::make_shared<indexlibv2::config::FieldConfig>();
    autil::legacy::FromJsonString(*suffixFieldConfig, R"({"field_name":"friendid","field_type":"INT32"})");
    kkvIndexConfig->SetSuffixFieldConfig(suffixFieldConfig);
    kkvIndexConfig->SetSuffixKeyTruncateLimits(1024);
    auto valueConfig = std::make_shared<indexlibv2::config::ValueConfig>();
    kkvIndexConfig->SetValueConfig(valueConfig);

    auto [status, kkvDocSorter] =
        KKVDocSorterFactory::CreateIfNeccessary(kkvIndexConfig, KKVDumpPhrase::MERGE_BOTTOMLEVEL);
    ASSERT_TRUE(status.IsOK());
    ASSERT_NE(nullptr, kkvDocSorter);
    ASSERT_FALSE(kkvDocSorter->KeepSkeySorted());
}

TEST_F(KKVDocSorterFactoryTest, testSort)
{
    auto kkvIndexConfig = std::make_shared<indexlibv2::config::KKVIndexConfig>();
    auto suffixFieldConfig = std::make_shared<indexlibv2::config::FieldConfig>();
    autil::legacy::FromJsonString(*suffixFieldConfig, R"({"field_name":"friendid","field_type":"INT32"})");
    kkvIndexConfig->SetSuffixFieldConfig(suffixFieldConfig);
    indexlib::config::KKVIndexFieldInfo suffixFieldInfo;
    suffixFieldInfo.enableKeepSortSequence = true;
    kkvIndexConfig->SetSuffixFieldInfo(suffixFieldInfo);
    auto valueConfig = std::make_shared<indexlibv2::config::ValueConfig>();
    kkvIndexConfig->SetValueConfig(valueConfig);

    auto [status, kkvDocSorter] =
        KKVDocSorterFactory::CreateIfNeccessary(kkvIndexConfig, KKVDumpPhrase::MERGE_BOTTOMLEVEL);
    ASSERT_TRUE(status.IsOK());
    ASSERT_NE(nullptr, kkvDocSorter);
    ASSERT_TRUE(kkvDocSorter->KeepSkeySorted());
}

} // namespace indexlibv2::index
