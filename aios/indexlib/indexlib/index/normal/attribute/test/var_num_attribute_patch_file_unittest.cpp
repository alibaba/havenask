#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/index/normal/attribute/test/var_num_attribute_patch_file_unittest.h"
#include "indexlib/common/field_format/attribute/multi_string_attribute_convertor.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/attribute_config.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VarNumAttributePatchFileTest);

VarNumAttributePatchFileTest::VarNumAttributePatchFileTest()
{
}

VarNumAttributePatchFileTest::~VarNumAttributePatchFileTest()
{
}

void VarNumAttributePatchFileTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    FieldConfigPtr fieldConfig(new FieldConfig("var_num_field", ft_uint32, true));
    mAttrConfig.reset(new AttributeConfig());
    mAttrConfig->Init(fieldConfig);
}

void VarNumAttributePatchFileTest::CaseTearDown()
{
}

void VarNumAttributePatchFileTest::TestSimpleProcess()
{
}

void VarNumAttributePatchFileTest::TestReadAndMove()
{
    uint8_t expectData[16];
    uint8_t *ptr = expectData;
    *(uint16_t*)ptr = 2;
    ptr += sizeof(uint16_t);
    *(uint32_t*)(ptr) = 10;
    ptr += sizeof(uint32_t);
    *(uint32_t*)(ptr) = 20;
    ptr += sizeof(uint32_t);

    BufferedFileWriter writer;
    writer.Open(mRootDir + "/offset");    
    writer.Write(expectData, ptr - expectData);
    writer.Close();

    VarNumAttributePatchFile patchFile(0, mAttrConfig);
    patchFile.Open(mRootDir + "/offset");

    size_t bufLen = 64;
    uint8_t buff[bufLen];
    uint8_t *buffPtr = buff;
    patchFile.ReadAndMove(sizeof(uint16_t), buffPtr, bufLen);
    char* base = (char*)buff;
    EXPECT_EQ((uint16_t)2, *(uint16_t*)base);
    EXPECT_EQ((int32_t)2, (int32_t)(buffPtr - buff));
    EXPECT_EQ((size_t)62, bufLen);
}

void VarNumAttributePatchFileTest::TestGetPatchValue()
{
    uint8_t expectData[16];
    size_t encodeLen = VarNumAttributeFormatter::EncodeCount(
            2, (char*)expectData, 16);
    uint32_t* value = (uint32_t*)(expectData + encodeLen);
    value[0] = 10;
    value[1] = 20;

    BufferedFileWriter writer;
    writer.Open(mRootDir + "/offset");    
    writer.Write(expectData, encodeLen + sizeof(uint32_t)*2);
    writer.Close();

    VarNumAttributePatchFile patchFile(0, mAttrConfig);
    patchFile.Open(mRootDir + "/offset");

    ASSERT_EQ(0, patchFile.mCursor);

    size_t bufLen = 64;
    uint8_t buff[bufLen];
    size_t len = patchFile.GetPatchValue<uint32_t>(buff, bufLen);
    ASSERT_EQ((size_t)9, len);

    size_t encodeCountLen = 0;
    uint32_t count = VarNumAttributeFormatter::DecodeCount(
            (const char*)buff, encodeCountLen);
    ASSERT_EQ((uint32_t)2, count);
    ASSERT_EQ((uint32_t)10, *(uint32_t*)(buff + encodeCountLen));
    for (size_t i = 0; i < len; ++i)
    {
        ASSERT_EQ(expectData[i], buff[i]);
    }

    ASSERT_EQ(9, patchFile.mCursor);
}

void VarNumAttributePatchFileTest::TestGetPatchValueForMultiChar()
{
    MultiStringAttributeConvertor convertor;
    uint32_t patchCount = 2;
    string value1 = convertor.Encode("abcefgefgh");
    string value2 = convertor.Encode("");

    common::AttrValueMeta meta1 = convertor.Decode(ConstString(value1));
    common::AttrValueMeta meta2 = convertor.Decode(ConstString(value2));

    BufferedFileWriter writer;
    writer.Open(mRootDir + "/offset");    
    writer.Write(meta1.data.data(), meta1.data.size());
    writer.Write(meta2.data.data(), meta2.data.size());
    writer.Write(&patchCount, sizeof(uint32_t));
    writer.Write(&std::max(meta1.data.size(), meta2.data.size()), sizeof(uint32_t));
    writer.Close();

    VarNumAttributePatchFile patchFile(0, mAttrConfig);
    patchFile.Open(mRootDir + "/offset");

    size_t bufLen = 64;
    uint8_t buff[bufLen];
    size_t len = patchFile.GetPatchValue<MultiChar>(buff, bufLen);

    // count(1) + offsettype(1) + offsets(1*3) + value1(1+3) + value2(1+3) + value3(1+4)
    ASSERT_EQ((size_t)18, len);
    MultiString multiStr(buff);
    ASSERT_EQ(string("abc"), string(multiStr[0].data(), multiStr[0].size()));
    ASSERT_EQ(string("efg"), string(multiStr[1].data(), multiStr[1].size()));
    ASSERT_EQ(string("efgh"), string(multiStr[2].data(), multiStr[2].size()));
    ASSERT_EQ(18, patchFile.mCursor);

    //test data size = 0
    len = patchFile.GetPatchValue<MultiChar>(buff, bufLen);
    ASSERT_EQ((size_t)1, len);
    size_t encodeLen;
    uint32_t recordCount = VarNumAttributeFormatter::DecodeCount(
            (const char*)buff, encodeLen);
    ASSERT_EQ((size_t)0, recordCount);
    ASSERT_EQ(19, patchFile.mCursor);
}

void VarNumAttributePatchFileTest::TestSkipCurDocValue()
{
    //test [2|10,20]
    uint8_t expectData[16];
    size_t encodeLen = VarNumAttributeFormatter::EncodeCount(
            2, (char*)expectData, 16);
    uint32_t* value = (uint32_t*)(expectData + encodeLen);
    value[0] = 10;
    value[1] = 20;
    encodeLen += VarNumAttributeFormatter::EncodeCount(
            0, (char*)(expectData + 9), 5);

    BufferedFileWriter writer;
    writer.Open(mRootDir + "/offset");    
    writer.Write(expectData, encodeLen + sizeof(uint32_t)*2);
    uint32_t count = 2;
    uint32_t maxLength = 9;
    writer.Write(&count, sizeof(uint32_t));
    writer.Write(&maxLength, sizeof(uint32_t));
    writer.Close();

    VarNumAttributePatchFile patchFile(0, mAttrConfig);
    patchFile.Open(mRootDir + "/offset");

    ASSERT_EQ(0, patchFile.mCursor);
    patchFile.SkipCurDocValue<uint32_t>();
    ASSERT_EQ(9, patchFile.mCursor);

    //test data zero
    patchFile.SkipCurDocValue<uint32_t>();
    ASSERT_EQ(10, patchFile.mCursor);

    ASSERT_ANY_THROW(patchFile.SkipCurDocValue<uint32_t>());

    ASSERT_EQ((uint32_t)2, patchFile.GetPatchItemCount());
    ASSERT_EQ((uint32_t)9, patchFile.GetMaxPatchItemLen());
}

void VarNumAttributePatchFileTest::TestSkipCurDocValueForMultiChar()
{
    MultiStringAttributeConvertor convertor;
    string value1 = convertor.Encode("abcefgefgh");
    string value2 = convertor.Encode("");

    common::AttrValueMeta meta1 = convertor.Decode(ConstString(value1));
    common::AttrValueMeta meta2 = convertor.Decode(ConstString(value2));

    BufferedFileWriter writer;
    writer.Open(mRootDir + "/offset");    
    writer.Write(meta1.data.data(), meta1.data.size());
    writer.Write(meta2.data.data(), meta2.data.size());
    uint32_t count = 2;
    uint32_t maxLength = std::max(meta1.data.size(), meta2.data.size());
    writer.Write(&count, sizeof(uint32_t));
    writer.Write(&maxLength, sizeof(uint32_t));
    writer.Close();

    VarNumAttributePatchFile patchFile(0, mAttrConfig);
    patchFile.Open(mRootDir + "/offset");

    // count(1) + offsettype(1) + offsets(1*3) + value1(1+3) + value2(1+3) + value3(1+4)
    ASSERT_EQ(0, patchFile.mCursor);
    patchFile.SkipCurDocValue<MultiChar>();
    ASSERT_EQ(18, patchFile.mCursor);

    //test data size = 0
    patchFile.SkipCurDocValue<MultiChar>();
    ASSERT_EQ(19, patchFile.mCursor);

    ASSERT_ANY_THROW(patchFile.SkipCurDocValue<MultiChar>());

    ASSERT_EQ((uint32_t)2, patchFile.GetPatchItemCount());
    ASSERT_EQ((uint32_t)18, patchFile.GetMaxPatchItemLen());
}

IE_NAMESPACE_END(index);

