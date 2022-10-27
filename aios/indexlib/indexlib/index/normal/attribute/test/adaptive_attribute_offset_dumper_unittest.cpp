#include "indexlib/index/normal/attribute/test/adaptive_attribute_offset_dumper_unittest.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/util/simple_pool.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AdaptiveAttributeOffsetDumperTest);

AdaptiveAttributeOffsetDumperTest::AdaptiveAttributeOffsetDumperTest()
{
}

AdaptiveAttributeOffsetDumperTest::~AdaptiveAttributeOffsetDumperTest()
{
}

void AdaptiveAttributeOffsetDumperTest::CaseSetUp()
{
}

void AdaptiveAttributeOffsetDumperTest::CaseTearDown()
{
}

void AdaptiveAttributeOffsetDumperTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");

    util::SimplePool pool;
    AdaptiveAttributeOffsetDumper offsetDumper(&pool);
    AttributeConfigPtr attrConfig = 
        AttributeTestUtil::CreateAttrConfig<uint32_t>(true, true);
    attrConfig->GetFieldConfig()->SetU32OffsetThreshold(100);

    offsetDumper.Init(attrConfig);
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

    DirectoryPtr segDirectory = GET_SEGMENT_DIRECTORY();
    FileWriterPtr fileWriter = 
        segDirectory->CreateFileWriter("offset_file");

    offsetDumper.Dump(fileWriter);
    fileWriter->Close();
    
    size_t fileLen = segDirectory->GetFileLength("offset_file");
    size_t expectLen = sizeof(uint32_t) * 2 // compress header
                       + sizeof(uint64_t)  // one slot
                       + sizeof(uint64_t)* 2 + 3 * sizeof(uint8_t) // one u64 delta block 
                       + sizeof(uint32_t); // magic tail
    ASSERT_EQ(expectLen, fileLen);
}

IE_NAMESPACE_END(index);

