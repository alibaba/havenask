#include "indexlib/index/attribute/patch/SingleValueAttributePatchReader.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/format/SingleValueAttributePatchFormatter.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::index {

class SingleValueAttributePatchReaderTest : public TESTBASE
{
public:
    SingleValueAttributePatchReaderTest() = default;
    ~SingleValueAttributePatchReaderTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    std::shared_ptr<AttributeConfig> MakeAttributeConfig(bool isSupportNull);
    void MakePatchData(const string& dataStr, bool supportNull);
    template <typename T>
    void CheckIterator(SingleValueAttributePatchReader<T>& reader, std::map<docid_t, T>& expectedPatchData);

    template <typename T>
    void CreatePatchData(const std::shared_ptr<indexlib::file_system::IDirectory>& patchDirectory, uint32_t patchNum,
                         std::map<docid_t, T>& expectedPatchData,
                         std::vector<std::pair<std::string, segmentid_t>>& patchFileVect);
    template <typename T>
    std::string CreateOnePatchData(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                   uint32_t patchId, std::map<docid_t, T>& expectedPatchData);

private:
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;

    static const uint32_t MAX_DOC_COUNT = 100;
};

void SingleValueAttributePatchReaderTest::setUp()
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
}

void SingleValueAttributePatchReaderTest::tearDown() {}

std::shared_ptr<AttributeConfig> SingleValueAttributePatchReaderTest::MakeAttributeConfig(bool isSupportNull)
{
    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig(
        new indexlibv2::config::FieldConfig("field", ft_int32, false));
    std::shared_ptr<AttributeConfig> attrConfig(new index::AttributeConfig);
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        assert(false);
        return nullptr;
    }
    fieldConfig->SetEnableNullField(isSupportNull);
    return attrConfig;
}

void SingleValueAttributePatchReaderTest::MakePatchData(const string& dataStr, bool supportNull)
{
    autil::StringTokenizer st1(dataStr, ";",
                               autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
    assert(_rootDir != nullptr);
    for (size_t i = 0; i < st1.getNumTokens(); ++i) {
        autil::StringTokenizer st2(st1[i], ",",
                                   autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
        std::stringstream ss;
        ss << i << '.' << PATCH_FILE_NAME;
        auto [status, file] = _rootDir->CreateFileWriter(ss.str(), indexlib::file_system::WriterOption()).StatusWith();
        ASSERT_TRUE(status.IsOK());

        SingleValueAttributePatchFormatter formatter;
        formatter.InitForWrite(supportNull, file);

        for (size_t j = 0; j < st2.getNumTokens(); ++j) {
            autil::StringTokenizer st3(st2[j], ":",
                                       autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);
            assert(st3.getNumTokens() >= 2);
            docid_t docId;
            autil::StringUtil::strToInt32(st3[0].c_str(), docId);
            int32_t value;
            autil::StringUtil::strToInt32(st3[1].c_str(), value);
            bool isNull = false;
            if (st3.getNumTokens() == 3) {
                autil::StringUtil::fromString(st3[2].c_str(), isNull);
            }
            if (isNull && supportNull) {
                SingleValueAttributePatchFormatter::EncodedDocId(docId);
            }
            ASSERT_TRUE(formatter.Write(docId, (uint8_t*)&value, sizeof(int32_t)).IsOK());
        }
        ASSERT_TRUE(formatter.Close().IsOK());
    }
}

template <typename T>
void SingleValueAttributePatchReaderTest::CreatePatchData(
    const std::shared_ptr<indexlib::file_system::IDirectory>& patchDirectory, uint32_t patchNum,
    std::map<docid_t, T>& expectedPatchData, std::vector<std::pair<std::string, segmentid_t>>& patchFileVect)
{
    for (uint32_t i = 0; i < patchNum; ++i) {
        auto patchFileName = CreateOnePatchData<T>(patchDirectory, i, expectedPatchData);
        patchFileVect.push_back(std::make_pair(patchFileName, i));
    }
}

template <typename T>
std::string SingleValueAttributePatchReaderTest::CreateOnePatchData(
    const std::shared_ptr<indexlib::file_system::IDirectory>& directory, uint32_t patchId,
    std::map<docid_t, T>& expectedPatchData)
{
    std::map<docid_t, T> patchData;
    for (uint32_t i = 0; i < MAX_DOC_COUNT; ++i) {
        docid_t docId = rand() % MAX_DOC_COUNT;
        T value = (T)(rand() % 10000);
        patchData[docId] = value;
        expectedPatchData[docId] = value;
    }

    std::string patchFileName = autil::StringUtil::toString<uint32_t>(patchId);
    auto [status, file] =
        directory->CreateFileWriter(patchFileName, indexlib::file_system::WriterOption()).StatusWith();
    assert(status.IsOK());

    auto it = patchData.begin();
    for (; it != patchData.end(); ++it) {
        file->Write((void*)(&(it->first)), sizeof(docid_t)).GetOrThrow();
        file->Write((void*)(&(it->second)), sizeof(uint32_t)).GetOrThrow();
    }
    file->Close().GetOrThrow();
    return patchFileName;
}

template <typename T>
void SingleValueAttributePatchReaderTest::CheckIterator(SingleValueAttributePatchReader<T>& reader,
                                                        std::map<docid_t, T>& expectedPatchData)
{
    docid_t docId;
    uint32_t value;

    auto mapIt = expectedPatchData.begin();
    for (; mapIt != expectedPatchData.end(); ++mapIt) {
        bool isNull = false;
        auto [status, ret] = reader.Next(docId, value, isNull);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(ret);
        ASSERT_FALSE(isNull);
        ASSERT_EQ(mapIt->first, docId);
        ASSERT_EQ(mapIt->second, value);
    }
}

#define CHECK_READ_PATCH(reader, docId, value, isNull, RET)                                                            \
    do {                                                                                                               \
        auto [status, ret] = reader.ReadPatch(docId, value, isNull);                                                   \
        ASSERT_TRUE(status.IsOK());                                                                                    \
        ASSERT_TRUE(ret == RET);                                                                                       \
    } while (0);

TEST_F(SingleValueAttributePatchReaderTest, TestCaseForSequentiallyReadPatch)
{
    std::string dataStr("1:20,2:30,3:45;1:48:true;2:40,3:21");
    MakePatchData(dataStr, true);

    SingleValueAttributePatchReader<int32_t> reader(MakeAttributeConfig(true));
    std::string patchFileName = string(".") + PATCH_FILE_NAME;
    ASSERT_TRUE(reader.AddPatchFile(_rootDir, "0" + patchFileName, 0).IsOK());
    ASSERT_TRUE(reader.AddPatchFile(_rootDir, "1" + patchFileName, 1).IsOK());
    ASSERT_TRUE(reader.AddPatchFile(_rootDir, "2" + patchFileName, 2).IsOK());

    int32_t value;
    bool isNull = false;
    CHECK_READ_PATCH(reader, 0, value, isNull, false);
    ASSERT_FALSE(isNull);
    CHECK_READ_PATCH(reader, 1, value, isNull, true);
    ASSERT_TRUE(isNull);
    CHECK_READ_PATCH(reader, 2, value, isNull, true);
    ASSERT_EQ(40, value);
    ASSERT_FALSE(isNull);
    CHECK_READ_PATCH(reader, 3, value, isNull, true);
    ASSERT_EQ(21, value);
    ASSERT_FALSE(isNull);
    CHECK_READ_PATCH(reader, 4, value, isNull, false);
    ASSERT_FALSE(isNull);
}

TEST_F(SingleValueAttributePatchReaderTest, TestCaseForUnSequentiallyReadPatchWithoutLastValidDoc)
{
    std::string dataStr("1:20,2:30,3:45;1:48;2:40");
    MakePatchData(dataStr, false);

    SingleValueAttributePatchReader<int32_t> reader(MakeAttributeConfig(false));
    std::string patchFileName = string(".") + PATCH_FILE_NAME;
    ASSERT_TRUE(reader.AddPatchFile(_rootDir, "0" + patchFileName, 0).IsOK());
    ASSERT_TRUE(reader.AddPatchFile(_rootDir, "1" + patchFileName, 1).IsOK());
    ASSERT_TRUE(reader.AddPatchFile(_rootDir, "2" + patchFileName, 2).IsOK());

    int32_t value;
    bool isNull = false;
    CHECK_READ_PATCH(reader, 0, value, isNull, false);
    ASSERT_FALSE(isNull);
    CHECK_READ_PATCH(reader, 1, value, isNull, true);
    ASSERT_FALSE(isNull);
    ASSERT_EQ(48, value);
    CHECK_READ_PATCH(reader, 2, value, isNull, true);
    ASSERT_FALSE(isNull);
    ASSERT_EQ(40, value);
    CHECK_READ_PATCH(reader, 4, value, isNull, false);
    ASSERT_FALSE(isNull);
}

TEST_F(SingleValueAttributePatchReaderTest, TestCaseForUnSequentiallyReadPatch)
{
    std::string dataStr("1:20,2:30,3:45;1:48;2:40");
    MakePatchData(dataStr, false);

    SingleValueAttributePatchReader<int32_t> reader(MakeAttributeConfig(false));
    std::string patchFileName = string(".") + PATCH_FILE_NAME;
    ASSERT_TRUE(reader.AddPatchFile(_rootDir, "0" + patchFileName, 0).IsOK());
    ASSERT_TRUE(reader.AddPatchFile(_rootDir, "1" + patchFileName, 1).IsOK());
    ASSERT_TRUE(reader.AddPatchFile(_rootDir, "2" + patchFileName, 2).IsOK());

    int32_t value;
    bool isNull = false;
    CHECK_READ_PATCH(reader, 0, value, isNull, false);
    ASSERT_FALSE(isNull);
    CHECK_READ_PATCH(reader, 1, value, isNull, true);
    ASSERT_FALSE(isNull);
    ASSERT_EQ(48, value);
    CHECK_READ_PATCH(reader, 3, value, isNull, true);
    ASSERT_FALSE(isNull);
    ASSERT_EQ(45, value);
    CHECK_READ_PATCH(reader, 4, value, isNull, false);
    ASSERT_FALSE(isNull);
}

TEST_F(SingleValueAttributePatchReaderTest, TestCaseForNext)
{
    std::map<docid_t, uint32_t> expectedPatchData;
    uint32_t patchNum = 3;
    std::vector<std::pair<std::string, segmentid_t>> patchFileVect;
    CreatePatchData<uint32_t>(_rootDir, patchNum, expectedPatchData, patchFileVect);

    std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig(
        new indexlibv2::config::FieldConfig("abc", ft_int32, false));
    std::shared_ptr<AttributeConfig> attrConfig(new index::AttributeConfig);
    auto status = attrConfig->Init(fieldConfig);
    ASSERT_TRUE(status.IsOK());
    SingleValueAttributePatchReader<uint32_t> reader(attrConfig);
    for (auto [patchFileName, segId] : patchFileVect) {
        ASSERT_TRUE(reader.AddPatchFile(_rootDir, patchFileName, segId).IsOK());
    }
    CheckIterator(reader, expectedPatchData);
}

} // namespace indexlibv2::index
