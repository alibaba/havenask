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
#include "indexlib/index/attribute/patch/SingleValueAttributePatchReader.h"
#include "indexlib/index/attribute/patch/SingleValueAttributeUpdater.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "unittest/unittest.h"

using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;

namespace {
static std::string FIELD_NAME_TAG = "price";
} // namespace

namespace indexlibv2::index {

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

class SingleValueAttributeUpdaterTest : public TESTBASE
{
public:
    SingleValueAttributeUpdaterTest() = default;
    ~SingleValueAttributeUpdaterTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    struct Operation {
        Operation(docid_t docId_, std::string value_, bool isNull_)
        {
            docId = docId_;
            isNull = isNull_;
            value = value_;
        }
        docid_t docId = INVALID_DOCID;
        bool isNull = false;
        std::string value;
    };

private:
    std::shared_ptr<AttributeConfig> MakeAttributeConfig(FieldType type, bool isSupportNull);

    template <typename T>
    void InnerTest(const std::string& operationStr, bool enableNull);

    template <typename T>
    void CreateOperationAndExpectResults(const std::string& operationStr, std::vector<Operation>& operations,
                                         std::map<docid_t, std::pair<T, bool>>& expectResults);

    template <typename T>
    void Check(segmentid_t srcSegId, segmentid_t dstSegId, std::map<docid_t, std::pair<T, bool>>& expectResults,
               bool isPatchCompressed, bool supportNull);

    void ClearTestRoot();

private:
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    std::unique_ptr<autil::mem_pool::Pool> _pool;
};

void SingleValueAttributeUpdaterTest::setUp()
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

void SingleValueAttributeUpdaterTest::tearDown() {}
void SingleValueAttributeUpdaterTest::ClearTestRoot()
{
    auto ec = fslib::fs::FileSystem::removeDir(_rootPath);
    ec = fslib::fs::FileSystem::mkDir(_rootPath);
    (void)ec;
    tearDown();
    setUp();
}

template <typename T>
void SingleValueAttributeUpdaterTest::InnerTest(const std::string& operationStr, bool enableNull)
{
    const segmentid_t srcSegId = 7;
    const segmentid_t dstSegId = 5;
    auto attrConfig = MakeAttributeConfig(TypeParseTraits<T>::type, enableNull);
    SingleValueAttributeUpdater<T> updater(dstSegId, attrConfig);

    std::shared_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    assert(convertor);

    std::vector<Operation> operations;
    std::map<docid_t, std::pair<T, bool>> expectResults;
    CreateOperationAndExpectResults(operationStr, operations, expectResults);

    for (size_t i = 0; i < operations.size(); ++i) {
        std::pair<docid_t, std::string> op = std::make_pair(operations[i].docId, operations[i].value);
        StringView encodeValue = convertor->Encode(StringView(op.second), _pool.get());
        AttrValueMeta meta = convertor->Decode(encodeValue);
        updater.Update(op.first, meta.data, operations[i].isNull);
    }
    ASSERT_TRUE(updater.Dump(_rootDir, srcSegId).IsOK());
    Check(dstSegId, srcSegId, expectResults, attrConfig->GetCompressType().HasPatchCompress(), enableNull);
    ClearTestRoot();
}

template <typename T>
void SingleValueAttributeUpdaterTest::CreateOperationAndExpectResults(
    const std::string& operationStr, std::vector<Operation>& operations,
    std::map<docid_t, std::pair<T, bool>>& expectResults)
{
    StringTokenizer st;
    st.tokenize(operationStr, ";");
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer subSt;
        subSt.tokenize(st[i], ",");

        docid_t docId = StringUtil::fromString<docid_t>(subSt[0]);
        T value = StringUtil::fromString<T>(subSt[1]);
        bool isNull = false;
        if (subSt.getNumTokens() >= 3) {
            isNull = StringUtil::fromString<bool>(subSt[2]);
        }
        Operation op(docId, subSt[1], isNull);
        operations.push_back(op);
        expectResults[docId] = std::make_pair(value, isNull);
    }
}

template <typename T>
void SingleValueAttributeUpdaterTest::Check(segmentid_t dstSegId, segmentid_t srcSegId,
                                            std::map<docid_t, std::pair<T, bool>>& expectResults,
                                            bool isPatchCompressed, bool supportNull)
{
    SinglePatchFile patchFile(dstSegId, isPatchCompressed, supportNull, sizeof(T));
    std::stringstream ss;
    ss << FIELD_NAME_TAG << "/" << srcSegId << "_" << dstSegId << "." << PATCH_FILE_NAME;
    ASSERT_TRUE(patchFile.Open(_rootDir, ss.str()).IsOK());
    ASSERT_EQ(dstSegId, patchFile.GetSegmentId());
    for (size_t i = 0; i < expectResults.size(); ++i) {
        ASSERT_TRUE(patchFile.HasNext());
        ASSERT_TRUE(patchFile.Next<T>().IsOK());
        docid_t docId = patchFile.GetCurDocId();
        T expectedValue = expectResults[docId].first;
        T readValue;
        bool isNull = false;
        patchFile.GetPatchValue<T>(readValue, isNull);
        ASSERT_EQ(expectResults[docId].second, isNull) << docId;
        if (!isNull) {
            ASSERT_EQ(expectedValue, readValue) << docId;
        }
    }
    ASSERT_FALSE(patchFile.HasNext());
}

std::shared_ptr<AttributeConfig> SingleValueAttributeUpdaterTest::MakeAttributeConfig(FieldType fieldType,
                                                                                      bool isSupportNull)
{
    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig(
        new indexlibv2::config::FieldConfig(FIELD_NAME_TAG, fieldType, /*multiField*/ false));
    std::shared_ptr<AttributeConfig> attrConfig(new index::AttributeConfig);
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        assert(false);
        return nullptr;
    }
    fieldConfig->SetEnableNullField(isSupportNull);
    return attrConfig;
}

TEST_F(SingleValueAttributeUpdaterTest, testInt8)
{
    InnerTest<int8_t>("15,0;20,3;5,7;3,1;100,6;1025,9;15,9", false);
    // update with null
    InnerTest<int8_t>("15,0;20,3;5,7;3,1;20,6,true;1025,9;15,9", true);
    InnerTest<int8_t>("15,0;20,3,true;5,7;3,1;20,6;1025,9;15,9", true);
}
TEST_F(SingleValueAttributeUpdaterTest, testUInt8)
{
    InnerTest<uint8_t>("15,0;20,3;5,7;3,1;100,6;1025,9;15,9", false);
    // update with null
    InnerTest<uint8_t>("15,0;20,3;5,7;3,1;20,6,true;1025,9;15,9", true);
    InnerTest<uint8_t>("15,0;20,3,true;5,7;3,1;20,6;1025,9;15,9", true);
}
TEST_F(SingleValueAttributeUpdaterTest, testInt16)
{
    InnerTest<int16_t>("15,0;20,3;5,7;3,1;100,6;1025,9;4,300;15,900", false);
}
TEST_F(SingleValueAttributeUpdaterTest, testUInt16)
{
    InnerTest<uint16_t>("15,0;20,3;5,7;3,1;100,6;1025,9;4,300;15,900", false);
}
TEST_F(SingleValueAttributeUpdaterTest, testInt32)
{
    InnerTest<int32_t>("15,0;20,3;5,7;3,1;100,6;1025,9;11,65536;3,10", false);
    InnerTest<int32_t>("15,0;20,3;5,7;3,1;100,6;20,9,true;11,65536;3,10", true);
    InnerTest<int32_t>("15,0;20,3,true;5,7;3,1;100,6;20,1000;11,65536;3,10", true);
}
TEST_F(SingleValueAttributeUpdaterTest, testUInt32)
{
    InnerTest<uint32_t>("15,0;20,3;5,7;3,1;100,6;1025,9;11,65536;3,10", false);
    InnerTest<uint32_t>("15,0;20,3;5,7;3,1;100,6;20,9,true;11,65536;3,10", true);
    InnerTest<uint32_t>("15,0;20,3,true;5,7;3,1;100,6;20,1000;11,65536;3,10", true);
}
TEST_F(SingleValueAttributeUpdaterTest, testInt64)
{
    InnerTest<int64_t>("15,0;20,3;5,7;3,1;100,6;1025,9;11,65536;3,10", false);
    InnerTest<int64_t>("15,0;20,3;5,7;3,1;100,6;20,9,true;11,65536;3,10", true);
    InnerTest<int64_t>("15,0;20,3,true;5,7;3,1;100,6;20,1000;11,65536;3,10", true);
}
TEST_F(SingleValueAttributeUpdaterTest, testUInt64)
{
    InnerTest<uint64_t>("15,0;20,3;5,7;3,1;100,6;1025,9;11,65536;3,10", false);
    InnerTest<uint64_t>("15,0;20,3;5,7;3,1;100,6;20,9,true;11,65536;3,10", true);
    InnerTest<uint64_t>("15,0;20,3,true;5,7;3,1;100,6;20,1000;11,65536;3,10", true);
}
TEST_F(SingleValueAttributeUpdaterTest, testFloat)
{
    InnerTest<float>("15,0.0;20,3.0;5,7.0;3,1.0;100,6.0;1025,9.0;11,65536.0;3,10.0", false);
    InnerTest<float>("15,0.0;20,3.0;5,7.0;3,1.0;100,6.0;20,9.0,true;11,65536.0;3,10.0", true);
    InnerTest<float>("15,0.0;20,3.0,true;5,7.0;3,1.0;100,6.0;20,1000.0;11,65536.0;3,10.0", true);
}
TEST_F(SingleValueAttributeUpdaterTest, testDouble)
{
    InnerTest<double>("15,0.0;20,3.0;5,7.0;3,1.0;100,6.0;1025,9.0;11,65536.0;3,10.0", false);
    InnerTest<double>("15,0.0;20,3.0;5,7.0;3,1.0;100,6.0;20,9.0,true;11,65536.0;3,10.0", true);
    InnerTest<double>("15,0.0;20,3.0,true;5,7.0;3,1.0;100,6.0;20,1000.0;11,65536.0;3,10.0", true);
}
} // namespace indexlibv2::index
