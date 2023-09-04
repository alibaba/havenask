#include "indexlib/index/common/data_structure/AdaptiveAttributeOffsetDumper.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index/attribute/test/AttributeTestUtil.h"
#include "indexlib/index/common/data_structure/AttributeCompressInfo.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class AdaptiveAttributeOffsetDumperTest : public TESTBASE
{
public:
    AdaptiveAttributeOffsetDumperTest() = default;
    ~AdaptiveAttributeOffsetDumperTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
};

void AdaptiveAttributeOffsetDumperTest::setUp()
{
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH()).GetOrThrow();
    _rootDir = indexlib::file_system::Directory::Get(fs);
}

void AdaptiveAttributeOffsetDumperTest::tearDown() {}

TEST_F(AdaptiveAttributeOffsetDumperTest, TestSimpleProcess)
{
    autil::mem_pool::Pool pool;
    AdaptiveAttributeOffsetDumper offsetDumper(&pool);
    auto attrConfig = AttributeTestUtil::CreateAttrConfig<uint32_t>(true, "equal");
    ASSERT_TRUE(attrConfig);
    attrConfig->SetU32OffsetThreshold(100);
    offsetDumper.Init(AttributeCompressInfo::NeedEnableAdaptiveOffset(attrConfig),
                      AttributeCompressInfo::NeedCompressOffset(attrConfig), attrConfig->GetU32OffsetThreshold());
    offsetDumper.PushBack(10);
    ASSERT_FALSE(offsetDumper.IsU64Offset());
    ASSERT_EQ((size_t)1, offsetDumper.Size());
    ASSERT_EQ((uint64_t)10, offsetDumper.GetOffset(0));

    offsetDumper.PushBack(100);
    ASSERT_FALSE(offsetDumper.IsU64Offset());
    ASSERT_EQ((size_t)2, offsetDumper.Size());
    ASSERT_EQ((uint64_t)100, offsetDumper.GetOffset(1));

    offsetDumper.PushBack(101);
    ASSERT_TRUE(offsetDumper.IsU64Offset());
    ASSERT_EQ((size_t)3, offsetDumper.Size());
    ASSERT_EQ((uint64_t)101, offsetDumper.GetOffset(2));

    auto fileWriter = _rootDir->CreateFileWriter("offset_file");

    ASSERT_TRUE(offsetDumper.Dump(fileWriter).IsOK());
    ASSERT_EQ(indexlib::file_system::FSEC_OK, fileWriter->Close());

    size_t fileLen = _rootDir->GetFileLength("offset_file");
    size_t expectLen = sizeof(uint32_t) * 2                         // compress header
                       + sizeof(uint64_t)                           // one slot
                       + sizeof(uint64_t) * 2 + 3 * sizeof(uint8_t) // one u64 delta block
                       + sizeof(uint32_t);                          // magic tail
    ASSERT_EQ(expectLen, fileLen);
}

} // namespace indexlibv2::index
