#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/patch/MultiValueAttributePatchFile.h"
#include "indexlib/index/attribute/patch/MultiValueAttributeUpdater.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;

namespace indexlibv2::index {

static constexpr size_t NULL_VALUE_COUNT = 100;
static constexpr size_t MULTI_VALUE_ATTRIBUTE_MAX_COUNT = 65535;
static std::string FIELD_NAME_TAG = "price";

template <typename T>
struct TypeParseTraits;

#define REGISTER_PARSE_TYPE(X, Y)                                                                                      \
    template <>                                                                                                        \
    struct TypeParseTraits<X> {                                                                                        \
        static const FieldType type;                                                                                   \
    };                                                                                                                 \
    const FieldType TypeParseTraits<X>::type = Y

REGISTER_PARSE_TYPE(uint8_t, ft_uint8);
REGISTER_PARSE_TYPE(int8_t, ft_int8);
REGISTER_PARSE_TYPE(uint16_t, ft_uint16);
REGISTER_PARSE_TYPE(int16_t, ft_int16);
REGISTER_PARSE_TYPE(uint32_t, ft_uint32);
REGISTER_PARSE_TYPE(int32_t, ft_int32);
REGISTER_PARSE_TYPE(uint64_t, ft_uint64);
REGISTER_PARSE_TYPE(int64_t, ft_int64);
REGISTER_PARSE_TYPE(float, ft_float);
REGISTER_PARSE_TYPE(double, ft_double);
REGISTER_PARSE_TYPE(autil::MultiChar, ft_string);
REGISTER_PARSE_TYPE(autil::MultiString, ft_string);

class MultiValueAttributeUpdaterTest : public TESTBASE
{
public:
    MultiValueAttributeUpdaterTest() = default;
    ~MultiValueAttributeUpdaterTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    std::shared_ptr<AttributeConfig> MakeAttributeConfig(FieldType type, bool multiValue);

    template <typename T>
    void InnerTest(const std::string& operationStr);

    template <typename T>
    void CreateOperationAndExpectResults(const std::string& operationStr,
                                         std::vector<std::pair<docid_t, std::string>>& operations,
                                         std::map<docid_t, std::vector<T>>& expectResults);

    void CreateOperation(const std::string& operationStr, std::vector<std::pair<docid_t, std::string>>& operations);

    template <typename T>
    void Check(std::map<docid_t, std::vector<T>>& expectResults, uint32_t maxValueLen,
               const std::shared_ptr<AttributeConfig>& attrConfig);

    StringView GetDecodedValue(const std::shared_ptr<AttributeConvertor>& convertor, const std::string& value);
    void ClearTestRoot();

    template <typename T>
    void GenPatchData(const std::string& operationStr, bool multiValue, std::shared_ptr<AttributeConfig>& attrConfig);

    void GetMultiValuePatchFile(const std::shared_ptr<AttributeConfig>& attrConfig,
                                std::unique_ptr<MultiValueAttributePatchFile>& patchFile) const;

private:
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    std::unique_ptr<autil::mem_pool::Pool> _pool;
    const segmentid_t _srcSegId = 7;
    const segmentid_t _dstSegId = 6;
};

void MultiValueAttributeUpdaterTest::setUp()
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
    _pool.reset(new autil::mem_pool::Pool(256 * 1024));
}

void MultiValueAttributeUpdaterTest::tearDown() {}
void MultiValueAttributeUpdaterTest::ClearTestRoot()
{
    auto ec = fslib::fs::FileSystem::removeDir(_rootPath);
    ec = fslib::fs::FileSystem::mkDir(_rootPath);
    (void)ec;
    tearDown();
    setUp();
}

template <typename T>
void MultiValueAttributeUpdaterTest::InnerTest(const std::string& operationStr)
{
    auto attrConfig = MakeAttributeConfig(TypeParseTraits<T>::type, /*multiValue*/ true);
    MultiValueAttributeUpdater<T> updater(_dstSegId, attrConfig);

    std::shared_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    assert(convertor);

    uint32_t maxValueLen = 0;
    std::vector<std::pair<docid_t, std::string>> operations;
    std::map<docid_t, std::vector<T>> expectResults;
    CreateOperationAndExpectResults(operationStr, operations, expectResults);

    for (size_t i = 0; i < operations.size(); ++i) {
        std::pair<docid_t, std::string>& op = operations[i];
        if (attrConfig->SupportNull() && op.second == attrConfig->GetFieldConfig()->GetNullFieldLiteralString()) {
            updater.Update(op.first, StringView::empty_instance(), true);
            maxValueLen = std::max(maxValueLen, (uint32_t)4); // null value is 4 byte length (special count)
            continue;
        }
        StringView encodeValue = GetDecodedValue(convertor, op.second);
        maxValueLen = std::max(maxValueLen, (uint32_t)encodeValue.size());
        updater.Update(op.first, encodeValue, /*isNull*/ false);
    }

    ASSERT_TRUE(updater.Dump(_rootDir, _srcSegId).IsOK());
    Check(expectResults, maxValueLen, attrConfig);

    ClearTestRoot();
}

template <typename T>
void MultiValueAttributeUpdaterTest::GenPatchData(const std::string& operationStr, bool multiValue,
                                                  std::shared_ptr<AttributeConfig>& attrConfig)
{
    attrConfig = MakeAttributeConfig(TypeParseTraits<T>::type, multiValue);
    MultiValueAttributeUpdater<T> updater(_dstSegId, attrConfig);

    std::shared_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    assert(convertor);

    std::vector<std::pair<docid_t, std::string>> operations;
    CreateOperation(operationStr, operations);

    for (size_t i = 0; i < operations.size(); ++i) {
        std::pair<docid_t, std::string>& op = operations[i];
        if (attrConfig->SupportNull() && op.second == attrConfig->GetFieldConfig()->GetNullFieldLiteralString()) {
            updater.Update(op.first, StringView::empty_instance(), true);
            continue;
        }
        StringView encodeValue = GetDecodedValue(convertor, op.second);
        updater.Update(op.first, encodeValue, /*isNull*/ false);
    }

    ASSERT_TRUE(updater.Dump(_rootDir, _srcSegId).IsOK());
}

template <typename T>
void MultiValueAttributeUpdaterTest::CreateOperationAndExpectResults(
    const std::string& operationStr, std::vector<std::pair<docid_t, std::string>>& operations,
    std::map<docid_t, std::vector<T>>& expectResults)
{
    StringTokenizer st;
    st.tokenize(operationStr, ";");
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer subSt;
        subSt.tokenize(st[i], ",");
        ASSERT_EQ(2, subSt.getNumTokens());
        docid_t docId = StringUtil::fromString<docid_t>(subSt[0]);
        std::vector<T> values;
        if (subSt[1] == "__NULL__") {
            values.resize(NULL_VALUE_COUNT);
            operations.push_back(std::make_pair(docId, subSt[1]));
            expectResults[docId] = values;
            continue;
        }

        std::string strValue;
        StringTokenizer multiSt;
        multiSt.tokenize(subSt[1], "|");
        for (size_t j = 0; j < multiSt.getNumTokens(); ++j) {
            values.push_back(StringUtil::fromString<T>(multiSt[j]));
            strValue += multiSt[j];
            strValue += MULTI_VALUE_SEPARATOR;
        }
        operations.push_back(std::make_pair(docId, strValue));
        expectResults[docId] = values;
    }
}
void MultiValueAttributeUpdaterTest::CreateOperation(const std::string& operationStr,
                                                     std::vector<std::pair<docid_t, std::string>>& operations)
{
    StringTokenizer st;
    st.tokenize(operationStr, ";");
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer subSt;
        subSt.tokenize(st[i], ",");
        ASSERT_EQ(2, subSt.getNumTokens());
        docid_t docId = autil::StringUtil::fromString<docid_t>(subSt[0]);
        std::vector<std::string> values;
        if (subSt[1] == "__NULL__") {
            values.resize(NULL_VALUE_COUNT);
            operations.push_back(std::make_pair(docId, subSt[1]));
            continue;
        }

        std::string strValue;
        StringTokenizer multiSt;
        multiSt.tokenize(subSt[1], "|");
        for (size_t j = 0; j < multiSt.getNumTokens(); ++j) {
            values.push_back(std::string(multiSt[j]));
            if (j != 0) {
                strValue += MULTI_VALUE_SEPARATOR;
            }
            strValue += multiSt[j];
        }
        operations.push_back(std::make_pair(docId, strValue));
    }
}

StringView MultiValueAttributeUpdaterTest::GetDecodedValue(const std::shared_ptr<AttributeConvertor>& convertor,
                                                           const std::string& value)
{
    StringView encodeValue = convertor->Encode(StringView(value), _pool.get());
    AttrValueMeta meta = convertor->Decode(encodeValue);
    return meta.data;
}

void MultiValueAttributeUpdaterTest::GetMultiValuePatchFile(
    const std::shared_ptr<AttributeConfig>& attrConfig, std::unique_ptr<MultiValueAttributePatchFile>& patchFile) const
{
    patchFile.reset(new MultiValueAttributePatchFile(_dstSegId, attrConfig));

    auto [st, patchDir] = _rootDir->GetDirectory(FIELD_NAME_TAG).StatusWith();
    ASSERT_TRUE(st.IsOK());
    std::stringstream ss;
    ss << _srcSegId << "_" << _dstSegId << "." << PATCH_FILE_NAME;
    ASSERT_TRUE(patchFile->Open(patchDir, ss.str()).IsOK());
}

template <typename T>
void MultiValueAttributeUpdaterTest::Check(std::map<docid_t, std::vector<T>>& expectResults, uint32_t maxValueLen,
                                           const std::shared_ptr<AttributeConfig>& attrConfig)
{
    std::unique_ptr<MultiValueAttributePatchFile> patchFile;
    GetMultiValuePatchFile(attrConfig, patchFile);
    ASSERT_TRUE(patchFile != nullptr);

    ASSERT_EQ(_dstSegId, patchFile->GetSegmentId());
    for (size_t i = 0; i < expectResults.size(); ++i) {
        ASSERT_TRUE(patchFile->HasNext());
        ASSERT_TRUE(patchFile->Next().IsOK());
        docid_t docId = patchFile->GetCurDocId();
        std::vector<T> expectedValue = expectResults[docId];
        size_t bufferLen = sizeof(uint16_t) + sizeof(T) * MULTI_VALUE_ATTRIBUTE_MAX_COUNT;
        uint8_t buffer[bufferLen];
        bool isNull = false;
        patchFile->GetPatchValue<T>(buffer, bufferLen, isNull);
        if (expectedValue.size() == NULL_VALUE_COUNT) {
            ASSERT_TRUE(isNull);
            continue;
        }
        ASSERT_FALSE(isNull);
        size_t encodeCountLen = 0;
        uint32_t recordNum = MultiValueAttributeFormatter::DecodeCount((const char*)buffer, encodeCountLen, isNull);
        T* actualValue = (T*)(buffer + encodeCountLen);
        ASSERT_EQ(expectedValue.size(), recordNum);
        for (size_t j = 0; j < expectedValue.size(); ++j) {
            ASSERT_EQ(expectedValue[j], actualValue[j]);
        }
    }
    ASSERT_FALSE(patchFile->HasNext());
    ASSERT_EQ(maxValueLen, patchFile->GetMaxPatchItemLen());
}

std::shared_ptr<AttributeConfig> MultiValueAttributeUpdaterTest::MakeAttributeConfig(FieldType fieldType,
                                                                                     bool multiValue)
{
    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig(
        new indexlibv2::config::FieldConfig(FIELD_NAME_TAG, fieldType, multiValue));
    fieldConfig->SetEnableNullField(true);
    std::shared_ptr<AttributeConfig> attrConfig(new index::AttributeConfig);
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        return nullptr;
    }
    attrConfig->SetUpdatable(true);
    return attrConfig;
}

TEST_F(MultiValueAttributeUpdaterTest, testInt8)
{
    InnerTest<int8_t>("15,0;17,__NULL__;20,3;5,7;3,1;100,6;1025,9;15,9");
}
TEST_F(MultiValueAttributeUpdaterTest, testUInt8)
{
    InnerTest<uint8_t>("15,0;17,__NULL__;20,3;5,7;3,1;100,6;1025,9;15,9");
}

TEST_F(MultiValueAttributeUpdaterTest, testInt16) { InnerTest<int16_t>("15,0;20,3;5,7;3,1;100,6;1025,9;4,300;15,900"); }
TEST_F(MultiValueAttributeUpdaterTest, testUInt16)
{
    InnerTest<uint16_t>("15,0;20,3;5,7;3,1;100,6;1025,9;4,300;15,900");
}
TEST_F(MultiValueAttributeUpdaterTest, testInt32)
{
    InnerTest<int32_t>("15,0;20,3;5,7;3,1;100,6;1025,9;11,65536;11,__NULL;3,10");
}

TEST_F(MultiValueAttributeUpdaterTest, testUInt32)
{
    InnerTest<uint32_t>("15,0;20,3;5,7;3,1;100,6;1025,9;11,65536;11,__NULL;3,10|12");
}

TEST_F(MultiValueAttributeUpdaterTest, testSingleString)
{
    std::shared_ptr<AttributeConfig> attrConfig;
    GenPatchData<autil::MultiChar>("1,abc;1,1234;10,def;11,__NULL__", /*multiValue*/ false, attrConfig);

    std::unique_ptr<MultiValueAttributePatchFile> patchFile;
    GetMultiValuePatchFile(attrConfig, patchFile);

    ASSERT_TRUE(patchFile != nullptr);
    EXPECT_EQ(_dstSegId, patchFile->GetSegmentId());
    EXPECT_TRUE(patchFile->HasNext());

    ASSERT_TRUE(patchFile->Next().IsOK());
    EXPECT_EQ(1, patchFile->GetCurDocId());

    bool isNull = false;
    size_t bufferLen = sizeof(uint16_t) + sizeof(char) * MULTI_VALUE_ATTRIBUTE_MAX_COUNT;
    uint8_t buffer[bufferLen];
    patchFile->GetPatchValue<char>(buffer, bufferLen, isNull);
    ASSERT_FALSE(isNull);

    size_t encodeCountLen = 0;
    uint32_t recordNum = MultiValueAttributeFormatter::DecodeCount((const char*)buffer, encodeCountLen, isNull);
    char* actualValue = (char*)(buffer + encodeCountLen);
    EXPECT_EQ((uint32_t)4, recordNum);
    EXPECT_EQ('1', actualValue[0]);
    EXPECT_EQ('2', actualValue[1]);
    EXPECT_EQ('3', actualValue[2]);
    EXPECT_EQ('4', actualValue[3]);

    EXPECT_TRUE(patchFile->HasNext());
    ASSERT_TRUE(patchFile->Next().IsOK());

    EXPECT_EQ(10, patchFile->GetCurDocId());
    patchFile->GetPatchValue<char>(buffer, bufferLen, isNull);
    ASSERT_FALSE(isNull);

    recordNum = MultiValueAttributeFormatter::DecodeCount((const char*)buffer, encodeCountLen, isNull);
    actualValue = (char*)(buffer + encodeCountLen);
    EXPECT_EQ((uint32_t)3, recordNum);
    EXPECT_EQ('d', actualValue[0]);
    EXPECT_EQ('e', actualValue[1]);
    EXPECT_EQ('f', actualValue[2]);

    EXPECT_TRUE(patchFile->HasNext());
    ASSERT_TRUE(patchFile->Next().IsOK());

    EXPECT_EQ(11, patchFile->GetCurDocId());
    patchFile->GetPatchValue<char>(buffer, bufferLen, isNull);
    ASSERT_TRUE(isNull);

    recordNum = MultiValueAttributeFormatter::DecodeCount((const char*)buffer, encodeCountLen, isNull);
    EXPECT_EQ((uint32_t)0, recordNum);
    ASSERT_TRUE(isNull);
    ASSERT_EQ(4, encodeCountLen);
    EXPECT_FALSE(patchFile->HasNext());
}

TEST_F(MultiValueAttributeUpdaterTest, testMultiString)
{
    std::shared_ptr<AttributeConfig> attrConfig;
    GenPatchData<autil::MultiChar>("1,abc;1,1234;10,def|hijk;12,__NULL__", /*multiValue*/ true, attrConfig);

    std::unique_ptr<MultiValueAttributePatchFile> patchFile;
    GetMultiValuePatchFile(attrConfig, patchFile);

    ASSERT_TRUE(patchFile != nullptr);
    EXPECT_EQ(_dstSegId, patchFile->GetSegmentId());
    EXPECT_TRUE(patchFile->HasNext());

    ASSERT_TRUE(patchFile->Next().IsOK());
    EXPECT_EQ(1, patchFile->GetCurDocId());

    bool isNull = false;
    size_t bufferLen = sizeof(uint32_t) + sizeof(char) * MULTI_VALUE_ATTRIBUTE_MAX_COUNT;
    uint8_t buffer[bufferLen];
    patchFile->GetPatchValue<autil::MultiChar>(buffer, bufferLen, isNull);
    ASSERT_FALSE(isNull);

    size_t encodeCountLen = 0;
    uint32_t recordNum = MultiValueAttributeFormatter::DecodeCount((const char*)buffer, encodeCountLen, isNull);
    uint8_t* actualValue = buffer + encodeCountLen;
    EXPECT_EQ((uint32_t)1, recordNum);
    // offset length
    EXPECT_EQ((uint8_t)1, *(uint8_t*)(&actualValue[0]));
    // offset[0]
    EXPECT_EQ((uint8_t)0, *(uint8_t*)(&actualValue[1]));
    // item header
    EXPECT_EQ((uint8_t)4, *(uint8_t*)(&actualValue[2]));
    EXPECT_EQ('1', actualValue[3]);
    EXPECT_EQ('2', actualValue[4]);
    EXPECT_EQ('3', actualValue[5]);
    EXPECT_EQ('4', actualValue[6]);

    EXPECT_TRUE(patchFile->HasNext());
    ASSERT_TRUE(patchFile->Next().IsOK());
    EXPECT_EQ(10, patchFile->GetCurDocId());

    patchFile->GetPatchValue<MultiChar>(buffer, bufferLen, isNull);
    ASSERT_FALSE(isNull);

    recordNum = MultiValueAttributeFormatter::DecodeCount((const char*)buffer, encodeCountLen, isNull);
    EXPECT_EQ((uint16_t)2, recordNum);
    actualValue = buffer + encodeCountLen;
    // offset length
    EXPECT_EQ((uint8_t)1, *(uint8_t*)(&actualValue[0]));
    // offset[0]
    EXPECT_EQ((uint8_t)0, *(uint8_t*)(&actualValue[1]));
    // offset[1]
    EXPECT_EQ((uint8_t)4, *(uint8_t*)(&actualValue[2]));

    // item1
    EXPECT_EQ((uint8_t)3, *(uint8_t*)(&actualValue[3]));
    EXPECT_EQ('d', actualValue[4]);
    EXPECT_EQ('e', actualValue[5]);
    EXPECT_EQ('f', actualValue[6]);

    // item2
    EXPECT_EQ((uint8_t)4, *(uint8_t*)(&actualValue[7]));
    EXPECT_EQ('h', actualValue[8]);
    EXPECT_EQ('i', actualValue[9]);
    EXPECT_EQ('j', actualValue[10]);
    EXPECT_EQ('k', actualValue[11]);

    // docid:12, null value patch
    EXPECT_TRUE(patchFile->HasNext());
    ASSERT_TRUE(patchFile->Next().IsOK());
    EXPECT_EQ(12, patchFile->GetCurDocId());
    patchFile->GetPatchValue<MultiChar>(buffer, bufferLen, isNull);
    ASSERT_TRUE(isNull);

    recordNum = MultiValueAttributeFormatter::DecodeCount((const char*)buffer, encodeCountLen, isNull);
    EXPECT_EQ((uint32_t)0, recordNum);
    ASSERT_TRUE(isNull);
    ASSERT_EQ(4, encodeCountLen);
    EXPECT_FALSE(patchFile->HasNext());
}
} // namespace indexlibv2::index
