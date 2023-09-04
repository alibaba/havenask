#include "indexlib/index/attribute/patch/MultiValueAttributePatchFile.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/MultiStringAttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class MultiValueAttributePatchFileTest : public TESTBASE
{
public:
    MultiValueAttributePatchFileTest() = default;
    ~MultiValueAttributePatchFileTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    template <typename T>
    void InnerTestGetPatchValueForNull(const std::shared_ptr<AttributeConfig>& attrConfig,
                                       const std::shared_ptr<indexlib::file_system::IDirectory>& directory);

private:
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    std::shared_ptr<AttributeConfig> _attrConfig;
};

void MultiValueAttributePatchFileTest::setUp()
{
    _rootPath = GET_TEMP_DATA_PATH() + "/";
    if (!_rootPath.empty()) {
        indexlib::file_system::FslibWrapper::DeleteDirE(_rootPath, indexlib::file_system::DeleteOption::NoFence(true));
    }

    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.needFlush = true;

    auto loadStrategy =
        std::make_shared<indexlib::file_system::MmapLoadStrategy>(/*lock*/ true, false, 4 * 1024 * 1024, 0);
    indexlib::file_system::LoadConfig::FilePatternStringVector pattern;
    pattern.push_back(".*");

    indexlib::file_system::LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetFilePatternString(pattern);
    loadConfig.SetLoadStrategyPtr(loadStrategy);
    loadConfig.SetName("__OFFLINE_ATTRIBUTE__");

    indexlib::file_system::LoadConfigList loadConfigList;
    loadConfigList.PushFront(loadConfig);

    fsOptions.loadConfigList = loadConfigList;
    auto fs = indexlib::file_system::FileSystemCreator::Create("online", _rootPath, fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fs);
    _rootDir = rootDir->GetIDirectory();

    auto fieldConfig = std::make_shared<config::FieldConfig>("var_num_field", ft_uint32, true);
    _attrConfig.reset(new AttributeConfig());
    auto status = _attrConfig->Init(fieldConfig);
    ASSERT_TRUE(status.IsOK());
}

void MultiValueAttributePatchFileTest::tearDown() {}

template <typename T>
void MultiValueAttributePatchFileTest::InnerTestGetPatchValueForNull(
    const std::shared_ptr<AttributeConfig>& attrConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& directory)
{
    uint8_t expectData[16];
    size_t encodeLen = MultiValueAttributeFormatter::EncodeCount(
        MultiValueAttributeFormatter::MULTI_VALUE_NULL_FIELD_VALUE_COUNT, (char*)expectData, 16);

    uint32_t* tailCursor = (uint32_t*)(expectData + encodeLen);
    tailCursor[0] = 0;
    tailCursor[0] = 1;

    auto [status, writer] = directory->CreateFileWriter("offset", indexlib::file_system::WriterOption()).StatusWith();
    ASSERT_TRUE(status.IsOK());
    writer->Write(expectData, encodeLen + 8).GetOrThrow();
    ASSERT_EQ(indexlib::file_system::FSEC_OK, writer->Close());

    MultiValueAttributePatchFile patchFile(0, attrConfig);
    ASSERT_TRUE(patchFile.Open(directory, "offset").IsOK());
    ASSERT_EQ(0, patchFile._cursor);

    size_t bufLen = 64;
    uint8_t buff[bufLen];
    bool isNull = false;
    auto [st, len] = patchFile.GetPatchValue<T>(buff, bufLen, isNull);
    ASSERT_TRUE(st.IsOK());
    ASSERT_EQ((size_t)4, len);
    ASSERT_TRUE(isNull);

    size_t encodeCountLen = 0;
    uint32_t count = MultiValueAttributeFormatter::DecodeCount((const char*)buff, encodeCountLen, isNull);
    ASSERT_EQ((uint32_t)0, count);
    ASSERT_EQ(4, patchFile._cursor);
    ASSERT_TRUE(isNull);
}

TEST_F(MultiValueAttributePatchFileTest, TestReadAndMove)
{
    uint8_t expectData[16];
    uint8_t* ptr = expectData;
    *(uint16_t*)ptr = 2;
    ptr += sizeof(uint16_t);
    *(uint32_t*)(ptr) = 10;
    ptr += sizeof(uint32_t);
    *(uint32_t*)(ptr) = 20;
    ptr += sizeof(uint32_t);

    auto [status, writer] = _rootDir->CreateFileWriter("offset", indexlib::file_system::WriterOption()).StatusWith();
    ASSERT_TRUE(status.IsOK());
    writer->Write((char*)expectData, ptr - expectData).GetOrThrow();
    ASSERT_EQ(indexlib::file_system::FSEC_OK, writer->Close());

    MultiValueAttributePatchFile patchFile(0, _attrConfig);
    ASSERT_TRUE(patchFile.Open(_rootDir, "offset").IsOK());

    size_t bufLen = 64;
    uint8_t buff[bufLen];
    uint8_t* buffPtr = buff;
    ASSERT_TRUE(patchFile.ReadAndMove(sizeof(uint16_t), buffPtr, bufLen).IsOK());
    char* base = (char*)buff;
    EXPECT_EQ((uint16_t)2, *(uint16_t*)base);
    EXPECT_EQ((int32_t)2, (int32_t)(buffPtr - buff));
    EXPECT_EQ((size_t)62, bufLen);
}

TEST_F(MultiValueAttributePatchFileTest, TestGetPatchValueForNull)
{
    InnerTestGetPatchValueForNull<uint32_t>(_attrConfig, _rootDir);
    ASSERT_TRUE(_rootDir->RemoveFile("offset", indexlib::file_system::RemoveOption::MayNonExist()).Status().IsOK());
    InnerTestGetPatchValueForNull<autil::MultiChar>(_attrConfig, _rootDir);
}

TEST_F(MultiValueAttributePatchFileTest, TestGetPatchValue)
{
    uint8_t expectData[16];
    size_t encodeLen = MultiValueAttributeFormatter::EncodeCount(2, (char*)expectData, 16);
    uint32_t* value = (uint32_t*)(expectData + encodeLen);
    value[0] = 10;
    value[1] = 20;

    auto [status, writer] = _rootDir->CreateFileWriter("offset", indexlib::file_system::WriterOption()).StatusWith();
    ASSERT_TRUE(status.IsOK());
    writer->Write((char*)expectData, encodeLen + sizeof(uint32_t) * 2).GetOrThrow();
    ASSERT_EQ(indexlib::file_system::FSEC_OK, writer->Close());

    MultiValueAttributePatchFile patchFile(0, _attrConfig);
    ASSERT_TRUE(patchFile.Open(_rootDir, "offset").IsOK());

    ASSERT_EQ(0, patchFile._cursor);

    size_t bufLen = 64;
    uint8_t buff[bufLen];
    bool isNull = false;
    auto [st, len] = patchFile.GetPatchValue<uint32_t>(buff, bufLen, isNull);
    ASSERT_TRUE(st.IsOK());
    ASSERT_EQ((size_t)9, len);
    ASSERT_FALSE(isNull);

    size_t encodeCountLen = 0;
    uint32_t count = MultiValueAttributeFormatter::DecodeCount((const char*)buff, encodeCountLen, isNull);
    ASSERT_EQ((uint32_t)2, count);
    ASSERT_EQ((uint32_t)10, *(uint32_t*)(buff + encodeCountLen));
    for (size_t i = 0; i < len; ++i) {
        ASSERT_EQ(expectData[i], buff[i]);
    }
    ASSERT_EQ(9, patchFile._cursor);
}

TEST_F(MultiValueAttributePatchFileTest, TestGetPatchValueForMultiChar)
{
    MultiStringAttributeConvertor convertor;
    uint32_t patchCount = 2;
    std::string value1 = convertor.Encode("abcefgefgh");
    std::string value2 = convertor.Encode("");

    AttrValueMeta meta1 = convertor.Decode(autil::StringView(value1));
    AttrValueMeta meta2 = convertor.Decode(autil::StringView(value2));

    auto [status, writer] = _rootDir->CreateFileWriter("offset", indexlib::file_system::WriterOption()).StatusWith();
    ASSERT_TRUE(status.IsOK());
    writer->Write(meta1.data.data(), meta1.data.size()).GetOrThrow();
    writer->Write(meta2.data.data(), meta2.data.size()).GetOrThrow();
    writer->Write(&patchCount, sizeof(uint32_t)).GetOrThrow();
    writer->Write(&std::max(meta1.data.size(), meta2.data.size()), sizeof(uint32_t)).GetOrThrow();
    ASSERT_EQ(indexlib::file_system::FSEC_OK, writer->Close());

    MultiValueAttributePatchFile patchFile(0, _attrConfig);
    ASSERT_TRUE(patchFile.Open(_rootDir, "offset").IsOK());

    size_t bufLen = 64;
    uint8_t buff[bufLen];
    bool isNull = false;
    auto [st, len] = patchFile.GetPatchValue<autil::MultiChar>(buff, bufLen, isNull);
    ASSERT_TRUE(st.IsOK());
    ASSERT_FALSE(isNull);

    // count(1) + offsettype(1) + offsets(1*3) + value1(1+3) + value2(1+3) + value3(1+4)
    ASSERT_EQ((size_t)18, len);
    autil::MultiString multiStr(buff);
    ASSERT_EQ(std::string("abc"), std::string(multiStr[0].data(), multiStr[0].size()));
    ASSERT_EQ(std::string("efg"), std::string(multiStr[1].data(), multiStr[1].size()));
    ASSERT_EQ(std::string("efgh"), std::string(multiStr[2].data(), multiStr[2].size()));
    ASSERT_EQ(18, patchFile._cursor);

    // test data size = 0
    std::tie(st, len) = patchFile.GetPatchValue<autil::MultiChar>(buff, bufLen, isNull);
    ASSERT_TRUE(st.IsOK());
    ASSERT_EQ((size_t)1, len);
    ASSERT_FALSE(isNull);

    size_t encodeLen;
    uint32_t recordCount = MultiValueAttributeFormatter::DecodeCount((const char*)buff, encodeLen, isNull);
    ASSERT_EQ((size_t)0, recordCount);
    ASSERT_EQ(19, patchFile._cursor);
}

TEST_F(MultiValueAttributePatchFileTest, TestSkipCurDocValue)
{
    // test [2|10,20]
    uint8_t expectData[16];
    size_t encodeLen = MultiValueAttributeFormatter::EncodeCount(2, (char*)expectData, 16);
    uint32_t* value = (uint32_t*)(expectData + encodeLen);
    value[0] = 10;
    value[1] = 20;
    encodeLen += MultiValueAttributeFormatter::EncodeCount(0, (char*)(expectData + 9), 5);

    auto [status, writer] = _rootDir->CreateFileWriter("offset", indexlib::file_system::WriterOption()).StatusWith();
    ASSERT_TRUE(status.IsOK());
    writer->Write(expectData, encodeLen + sizeof(uint32_t) * 2).GetOrThrow();
    uint32_t count = 2;
    uint32_t maxLength = 9;
    writer->Write(&count, sizeof(uint32_t)).GetOrThrow();
    writer->Write(&maxLength, sizeof(uint32_t)).GetOrThrow();
    ASSERT_EQ(indexlib::file_system::FSEC_OK, writer->Close());
    MultiValueAttributePatchFile patchFile(0, _attrConfig);
    ASSERT_TRUE(patchFile.Open(_rootDir, "offset").IsOK());

    ASSERT_EQ(0, patchFile._cursor);
    ASSERT_TRUE(patchFile.SkipCurDocValue<uint32_t>().IsOK());
    ASSERT_EQ(9, patchFile._cursor);

    // test data zero
    ASSERT_TRUE(patchFile.SkipCurDocValue<uint32_t>().IsOK());
    ASSERT_EQ(10, patchFile._cursor);

    ASSERT_FALSE(patchFile.SkipCurDocValue<uint32_t>().IsOK());

    ASSERT_EQ((uint32_t)2, patchFile.GetPatchItemCount());
    ASSERT_EQ((uint32_t)9, patchFile.GetMaxPatchItemLen());
}

TEST_F(MultiValueAttributePatchFileTest, TestSkipCurDocValueForMultiChar)
{
    MultiStringAttributeConvertor convertor;
    std::string value1 = convertor.Encode("abcefgefgh");
    std::string value2 = convertor.Encode("");

    AttrValueMeta meta1 = convertor.Decode(autil::StringView(value1));
    AttrValueMeta meta2 = convertor.Decode(autil::StringView(value2));

    auto [status, writer] = _rootDir->CreateFileWriter("offset", indexlib::file_system::WriterOption()).StatusWith();
    ASSERT_TRUE(status.IsOK());
    writer->Write(meta1.data.data(), meta1.data.size()).GetOrThrow();
    writer->Write(meta2.data.data(), meta2.data.size()).GetOrThrow();
    uint32_t count = 2;
    uint32_t maxLength = std::max(meta1.data.size(), meta2.data.size());
    writer->Write(&count, sizeof(uint32_t)).GetOrThrow();
    writer->Write(&maxLength, sizeof(uint32_t)).GetOrThrow();
    ASSERT_EQ(indexlib::file_system::FSEC_OK, writer->Close());

    MultiValueAttributePatchFile patchFile(0, _attrConfig);
    ASSERT_TRUE(patchFile.Open(_rootDir, "offset").IsOK());

    // count(1) + offsettype(1) + offsets(1*3) + value1(1+3) + value2(1+3) + value3(1+4)
    ASSERT_EQ(0, patchFile._cursor);
    ASSERT_TRUE(patchFile.SkipCurDocValue<autil::MultiChar>().IsOK());
    ASSERT_EQ(18, patchFile._cursor);

    // test data size = 0
    ASSERT_TRUE(patchFile.SkipCurDocValue<autil::MultiChar>().IsOK());
    ASSERT_EQ(19, patchFile._cursor);

    ASSERT_FALSE(patchFile.SkipCurDocValue<autil::MultiChar>().IsOK());

    ASSERT_EQ((uint32_t)2, patchFile.GetPatchItemCount());
    ASSERT_EQ((uint32_t)18, patchFile.GetMaxPatchItemLen());
}

} // namespace indexlibv2::index
