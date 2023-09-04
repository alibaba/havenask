#include "indexlib/index/attribute/patch/MultiValueAttributePatchReader.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/index/test/IndexTestUtil.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::index {

class MultiValueAttributePatchReaderTest : public TESTBASE
{
public:
    MultiValueAttributePatchReaderTest() = default;
    ~MultiValueAttributePatchReaderTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    Status GetPatchFileInfos(const std::string& patchFilePath,
                             std::shared_ptr<indexlib::file_system::IDirectory>& directory, std::string& fileName);
    template <typename T>
    void CreatePatchFile(uint32_t patchId, std::map<docid_t, std::vector<T>>& expectedPatchData,
                         std::string& patchFilePath);
    template <typename T>
    void CreatePatchRandomData(uint32_t patchFileNum, std::map<docid_t, std::vector<T>>& expectedPatchData,
                               std::vector<std::pair<std::string, segmentid_t>>& patchFileVect, bool enableNull);
    template <typename T>
    void CreateOnePatchRandomData(uint32_t patchId, std::map<docid_t, std::vector<T>>& expectedPatchData,
                                  string& patchFilePath, bool enableNull);
    template <typename T>
    void CheckReader(MultiValueAttributePatchReader<T>& reader, std::map<docid_t, std::vector<T>>& expectedPatchData);

private:
    std::string _rootPath;
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    std::shared_ptr<AttributeConfig> _attrConfig;

    static const uint32_t MAX_DOC_COUNT = 10;
    static const uint32_t MAX_VALUE_COUNT = 1000;
};

void MultiValueAttributePatchReaderTest::setUp()
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
    srand(8888);
}

void MultiValueAttributePatchReaderTest::tearDown() {}

template <typename T>
void MultiValueAttributePatchReaderTest::CreatePatchFile(uint32_t patchId,
                                                         std::map<docid_t, std::vector<T>>& expectedPatchData,
                                                         std::string& patchFilePath)
{
    patchFilePath = autil::StringUtil::toString<T>(patchId);
    auto [status, file] = _rootDir->CreateFileWriter(patchFilePath, indexlib::file_system::WriterOption()).StatusWith();
    ASSERT_TRUE(status.IsOK());

    uint32_t patchCount = expectedPatchData.size();
    uint32_t maxPatchLen = 0;
    for (auto it = expectedPatchData.begin(); it != expectedPatchData.end(); ++it) {
        file->Write((void*)(&(it->first)), sizeof(docid_t)).GetOrThrow();
        const std::vector<T>& valueVector = it->second;
        char countBuffer[4];
        if (valueVector.size() == MAX_VALUE_COUNT) // means null
        {
            size_t encodeLen = MultiValueAttributeFormatter::EncodeCount(
                MultiValueAttributeFormatter::MULTI_VALUE_NULL_FIELD_VALUE_COUNT, countBuffer, 4);
            file->Write(countBuffer, encodeLen).GetOrThrow();
            maxPatchLen = std::max(maxPatchLen, (uint32_t)encodeLen);
            continue;
        }

        size_t encodeLen = MultiValueAttributeFormatter::EncodeCount(it->second.size(), countBuffer, 4);
        file->Write(countBuffer, encodeLen).GetOrThrow();
        for (size_t i = 0; i < valueVector.size(); ++i) {
            file->Write((void*)(&(valueVector[i])), sizeof(T)).GetOrThrow();
        }
        uint32_t patchLen = encodeLen + it->second.size() * sizeof(T);
        maxPatchLen = std::max(maxPatchLen, patchLen);
    }

    file->Write(&patchCount, sizeof(uint32_t)).GetOrThrow();
    file->Write(&maxPatchLen, sizeof(uint32_t)).GetOrThrow();
    ASSERT_EQ(indexlib::file_system::FSEC_OK, file->Close());
}

template <typename T>
void MultiValueAttributePatchReaderTest::CreatePatchRandomData(
    uint32_t patchFileNum, std::map<docid_t, std::vector<T>>& expectedPatchData,
    std::vector<std::pair<std::string, segmentid_t>>& patchFileVect, bool enableNull)
{
    IndexTestUtil::ResetDir(_rootPath);
    for (uint32_t i = 0; i < patchFileNum; ++i) {
        std::string patchFilePath;
        CreateOnePatchRandomData<T>(i, expectedPatchData, patchFilePath, enableNull);
        patchFileVect.push_back(make_pair(patchFilePath, i));
    }
}
template <typename T>
void MultiValueAttributePatchReaderTest::CreateOnePatchRandomData(uint32_t patchId,
                                                                  std::map<docid_t, std::vector<T>>& expectedPatchData,
                                                                  std::string& patchFilePath, bool enableNull)
{
    std::map<docid_t, std::vector<T>> patchData;
    for (uint32_t i = 0; i < MAX_DOC_COUNT; ++i) {
        docid_t docId = rand() % MAX_DOC_COUNT;
        uint32_t valueCount = rand() % MAX_VALUE_COUNT;
        if (enableNull && (i % 3) == 0) {
            valueCount = MAX_VALUE_COUNT; // means null
        }
        std::vector<T> valueVector(valueCount);
        for (uint32_t j = 0; j < valueCount; ++j) {
            T value = rand() % 10000;
            valueVector.push_back(value);
        }
        patchData[docId] = valueVector;
        expectedPatchData[docId] = valueVector;
    }
    CreatePatchFile<T>(patchId, expectedPatchData, patchFilePath);
}

template <typename T>
void MultiValueAttributePatchReaderTest::CheckReader(MultiValueAttributePatchReader<T>& reader,
                                                     std::map<docid_t, std::vector<T>>& expectedPatchData)
{
    docid_t docId;
    size_t buffLen = sizeof(uint32_t) + sizeof(T) * 65535;
    uint8_t* buff = new uint8_t[buffLen];

    auto mapIt = expectedPatchData.begin();
    for (; mapIt != expectedPatchData.end(); ++mapIt) {
        bool expectNull = (mapIt->second.size() == MAX_VALUE_COUNT);
        bool isNull = false;
        auto [status, dataLen] = reader.Next(docId, buff, buffLen, isNull);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(expectNull, isNull);

        size_t encodeCountLen = 0;
        uint32_t recordNum = MultiValueAttributeFormatter::DecodeCount((const char*)buff, encodeCountLen, isNull);
        ASSERT_EQ(expectNull, isNull);
        if (isNull) {
            ASSERT_EQ(0, recordNum);
            ASSERT_EQ(recordNum, (dataLen - encodeCountLen) / sizeof(T));
            continue;
        }

        ASSERT_EQ(recordNum, (dataLen - encodeCountLen) / sizeof(T));
        ASSERT_EQ(mapIt->first, docId);
        ASSERT_EQ((uint16_t)mapIt->second.size(), recordNum);
        T* value = (T*)(buff + encodeCountLen);
        for (size_t i = 0; i < recordNum; i++) {
            ASSERT_EQ((mapIt->second)[i], ((T*)value)[i]);
        }
    }
    delete[] buff;
}

Status
MultiValueAttributePatchReaderTest::GetPatchFileInfos(const std::string& patchFilePath,
                                                      std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                                      std::string& fileName)
{
    std::string path = indexlib::util::PathUtil::GetParentDirPath(patchFilePath);
    auto [status, dir] = _rootDir->MakeDirectory(path, indexlib::file_system::DirectoryOption()).StatusWith();
    if (!status.IsOK()) {
        return status;
    }
    directory = dir;
    fileName = indexlib::util::PathUtil::GetFileName(patchFilePath);
    return Status::OK();
}

TEST_F(MultiValueAttributePatchReaderTest, TestCaseSimpleForNextUint32)
{
    std::map<docid_t, std::vector<uint32_t>> expectedPatchData;
    std::vector<uint32_t>& valueVector = expectedPatchData[0];
    valueVector.push_back(1000);
    valueVector.push_back(2000);

    std::vector<uint32_t>& nullValueVector = expectedPatchData[1];
    nullValueVector.resize(MAX_VALUE_COUNT);

    std::string patchFilePath;
    CreatePatchFile<uint32_t>(0, expectedPatchData, patchFilePath);

    std::shared_ptr<config::FieldConfig> fieldConfig(new config::FieldConfig("field", ft_uint32, true));
    _attrConfig.reset(new AttributeConfig);
    ASSERT_TRUE(_attrConfig->Init(fieldConfig).IsOK());

    MultiValueAttributePatchReader<uint32_t> reader(_attrConfig);
    std::shared_ptr<indexlib::file_system::IDirectory> patchDirectory;
    std::string patchFileName;
    ASSERT_TRUE(GetPatchFileInfos(patchFilePath, patchDirectory, patchFileName).IsOK());
    ASSERT_TRUE(reader.AddPatchFile(patchDirectory, patchFileName, 0).IsOK());
    CheckReader<uint32_t>(reader, expectedPatchData);
}

TEST_F(MultiValueAttributePatchReaderTest, TestCaseSimpleForNextUint64)
{
    std::map<docid_t, std::vector<uint64_t>> expectedPatchData;
    std::vector<uint64_t>& valueVector = expectedPatchData[0];
    valueVector.push_back(567878);
    valueVector.push_back(3435345);

    std::vector<uint64_t>& nullValueVector = expectedPatchData[1];
    nullValueVector.resize(MAX_VALUE_COUNT);

    std::string patchFilePath;
    CreatePatchFile<uint64_t>(0, expectedPatchData, patchFilePath);

    std::shared_ptr<config::FieldConfig> fieldConfig(new config::FieldConfig("field", ft_uint64, true));
    _attrConfig.reset(new AttributeConfig);
    ASSERT_TRUE(_attrConfig->Init(fieldConfig).IsOK());

    MultiValueAttributePatchReader<uint64_t> reader(_attrConfig);

    std::shared_ptr<indexlib::file_system::IDirectory> patchDirectory;
    std::string patchFileName;
    ASSERT_TRUE(GetPatchFileInfos(patchFilePath, patchDirectory, patchFileName).IsOK());
    ASSERT_TRUE(reader.AddPatchFile(patchDirectory, patchFileName, 0).IsOK());

    CheckReader<uint64_t>(reader, expectedPatchData);
}

TEST_F(MultiValueAttributePatchReaderTest, TestCaseSimpleForNextFloat)
{
    std::map<docid_t, std::vector<float>> expectedPatchData;
    std::vector<float>& valueVector = expectedPatchData[0];
    valueVector.push_back(10.01);
    valueVector.push_back(20.1);

    std::vector<float>& nullValueVector = expectedPatchData[1];
    nullValueVector.resize(MAX_VALUE_COUNT);

    std::string patchFilePath;
    CreatePatchFile<float>(0, expectedPatchData, patchFilePath);

    std::shared_ptr<config::FieldConfig> fieldConfig(new config::FieldConfig("field", ft_float, true));
    _attrConfig.reset(new AttributeConfig);
    ASSERT_TRUE(_attrConfig->Init(fieldConfig).IsOK());

    MultiValueAttributePatchReader<float> reader(_attrConfig);

    std::shared_ptr<indexlib::file_system::IDirectory> patchDirectory;
    std::string patchFileName;
    ASSERT_TRUE(GetPatchFileInfos(patchFilePath, patchDirectory, patchFileName).IsOK());
    ASSERT_TRUE(reader.AddPatchFile(patchDirectory, patchFileName, 0).IsOK());

    CheckReader<float>(reader, expectedPatchData);
}

TEST_F(MultiValueAttributePatchReaderTest, TestCaseRandomForNextUint32)
{
    std::map<docid_t, std::vector<uint32_t>> expectedPatchData;
    uint32_t patchFileNum = 2;
    std::vector<std::pair<std::string, segmentid_t>> patchFileVect;
    CreatePatchRandomData<uint32_t>(patchFileNum, expectedPatchData, patchFileVect, true);

    std::shared_ptr<config::FieldConfig> fieldConfig(new config::FieldConfig("field", ft_uint32, true));
    _attrConfig.reset(new AttributeConfig);
    ASSERT_TRUE(_attrConfig->Init(fieldConfig).IsOK());

    MultiValueAttributePatchReader<uint32_t> reader(_attrConfig);
    for (std::vector<std::pair<std::string, segmentid_t>>::iterator iter = patchFileVect.begin();
         iter != patchFileVect.end(); ++iter) {
        std::shared_ptr<indexlib::file_system::IDirectory> patchDirectory;
        std::string patchFileName;
        ASSERT_TRUE(GetPatchFileInfos((*iter).first, patchDirectory, patchFileName).IsOK());
        ASSERT_TRUE(reader.AddPatchFile(patchDirectory, patchFileName, (*iter).second).IsOK());
    }
    CheckReader<uint32_t>(reader, expectedPatchData);
}

TEST_F(MultiValueAttributePatchReaderTest, TestCaseRandomForNextUint64)
{
    std::map<docid_t, std::vector<uint64_t>> expectedPatchData;
    uint32_t patchFileNum = 2;
    std::vector<std::pair<std::string, segmentid_t>> patchFileVect;
    CreatePatchRandomData<uint64_t>(patchFileNum, expectedPatchData, patchFileVect, false);

    std::shared_ptr<config::FieldConfig> fieldConfig(new config::FieldConfig("field", ft_uint64, true));
    _attrConfig.reset(new AttributeConfig);
    ASSERT_TRUE(_attrConfig->Init(fieldConfig).IsOK());

    MultiValueAttributePatchReader<uint64_t> reader(_attrConfig);
    for (std::vector<std::pair<std::string, segmentid_t>>::iterator iter = patchFileVect.begin();
         iter != patchFileVect.end(); ++iter) {
        std::shared_ptr<indexlib::file_system::IDirectory> patchDirectory;
        std::string patchFileName;
        ASSERT_TRUE(GetPatchFileInfos((*iter).first, patchDirectory, patchFileName).IsOK());
        ASSERT_TRUE(reader.AddPatchFile(patchDirectory, patchFileName, (*iter).second).IsOK());
    }
    CheckReader<uint64_t>(reader, expectedPatchData);
}

TEST_F(MultiValueAttributePatchReaderTest, TestCaseForSkipReadUint16)
{
    std::map<docid_t, std::vector<uint16_t>> expectedPatchData;
    std::vector<uint16_t>& valueVector0 = expectedPatchData[0];
    valueVector0.push_back(1000);
    valueVector0.push_back(2000);

    std::vector<uint16_t>& valueVector2 = expectedPatchData[2];
    valueVector2.push_back(11);

    std::vector<uint16_t>& nullValueVector = expectedPatchData[4];
    nullValueVector.resize(MAX_VALUE_COUNT);

    std::string patchFilePath;
    CreatePatchFile<uint16_t>(0, expectedPatchData, patchFilePath);

    std::shared_ptr<config::FieldConfig> fieldConfig(new config::FieldConfig("field", ft_uint16, true));
    _attrConfig.reset(new AttributeConfig);
    auto st = _attrConfig->Init(fieldConfig);
    ASSERT_TRUE(st.IsOK());

    MultiValueAttributePatchReader<uint16_t> reader(_attrConfig);
    std::shared_ptr<indexlib::file_system::IDirectory> patchDirectory;
    std::string patchFileName;
    ASSERT_TRUE(GetPatchFileInfos(patchFilePath, patchDirectory, patchFileName).IsOK());
    ASSERT_TRUE(reader.AddPatchFile(patchDirectory, patchFileName, 0).IsOK());

    size_t buffLen = sizeof(uint32_t) * 4 + sizeof(uint16_t);
    uint8_t buff[buffLen];
    const size_t buffSize = sizeof(uint8_t) * buffLen;
    auto [status, ret] = reader.Seek(1, (uint8_t*)buff, buffSize);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ((size_t)0, ret);

    std::tie(status, ret) = reader.Seek(2, (uint8_t*)buff, buffSize);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ((size_t)3, ret);

    size_t encodeCountLen = 0;
    bool isNull = false;
    uint32_t recordNum = MultiValueAttributeFormatter::DecodeCount((const char*)buff, encodeCountLen, isNull);
    ASSERT_FALSE(isNull);
    uint16_t* value = (uint16_t*)(buff + encodeCountLen);
    ASSERT_EQ(valueVector2.size(), (size_t)recordNum);
    ASSERT_EQ((uint16_t)11, value[0]);

    std::tie(status, ret) = reader.Seek(4, (uint8_t*)buff, buffSize);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ((size_t)4, ret);
    recordNum = MultiValueAttributeFormatter::DecodeCount((const char*)buff, encodeCountLen, isNull);
    ASSERT_TRUE(isNull);
    ASSERT_EQ(0, recordNum);
}

} // namespace indexlibv2::index
