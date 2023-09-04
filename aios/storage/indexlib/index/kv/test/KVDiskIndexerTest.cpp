#include "indexlib/index/kv/KVDiskIndexer.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::file_system;

namespace indexlibv2 { namespace index {

class KVDiskIndexerTest : public TESTBASE
{
public:
    KVDiskIndexerTest() {}
    ~KVDiskIndexerTest() {}

public:
    void setUp() override {}
    void tearDown() override {}
    void EstimateMemUsed(const string& loadConfigStr, bool compressValue, bool expectedInMem);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KVDiskIndexerTest);

void KVDiskIndexerTest::EstimateMemUsed(const string& loadConfigStr, bool compressValue, bool expectedInMem)
{
    FileSystemOptions options;
    {
        LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigListFromJsonString(loadConfigStr);
        options.loadConfigList = loadConfigList;
        options.enableAsyncFlush = false;
        options.useCache = true;
    }
    auto fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    auto rootDirectory = Directory::Get(fileSystem);

    string fileName = "tmp_data";
    string indexName = "my_index";
    std::shared_ptr<Directory> dir = rootDirectory;
    std::shared_ptr<Directory> indexDir = rootDirectory->MakeDirectory(indexName);

    {
        FileWriterPtr keyFileWriter = indexDir->CreateFileWriter(KV_KEY_FILE_NAME);
        string tmpStr(1000, 'a');
        ASSERT_EQ(FSEC_OK, keyFileWriter->Write(tmpStr.data(), tmpStr.size()).Code());
        ASSERT_EQ(FSEC_OK, keyFileWriter->Close());
    }

    if (compressValue) {
        auto writeOption = WriterOption::Compress("lz4", 4096);
        writeOption.compressorParams["encode_address_mapper"] = "true";
        writeOption.compressorParams["address_mapper_batch_number"] = 16;
        writeOption.compressorParams["enable_meta_file"] = "false";
        writeOption.compressorParams["enable_hint_data"] = "true";
        writeOption.compressorParams["hint_sample_ratio"] = "0.1";
        FileWriterPtr valueFileWriter = indexDir->CreateFileWriter(KV_VALUE_FILE_NAME, writeOption);
        string tmpStr(10000, 'b');
        ASSERT_EQ(FSEC_OK, valueFileWriter->Write(tmpStr.data(), tmpStr.size()).Code());
        ASSERT_EQ(FSEC_OK, valueFileWriter->Close());
    } else {
        FileWriterPtr valueFileWriter = indexDir->CreateFileWriter(KV_VALUE_FILE_NAME);
        string tmpStr(10000, 'b');
        ASSERT_EQ(FSEC_OK, valueFileWriter->Write(tmpStr.data(), tmpStr.size()).Code());
        ASSERT_EQ(FSEC_OK, valueFileWriter->Close());
    }

    std::shared_ptr<indexlibv2::config::KVIndexConfig> kvConfig(new indexlibv2::config::KVIndexConfig());
    kvConfig->SetIndexName(indexName);
    kvConfig->SetFieldConfig(std::make_shared<indexlibv2::config::FieldConfig>(indexName, ft_int32, false));
    {
        std::shared_ptr<indexlibv2::config::ValueConfig> valueConfig(new indexlibv2::config::ValueConfig);
        kvConfig->SetValueConfig(valueConfig);
        auto& kvIndexPreference = kvConfig->GetIndexPreference();
        kvIndexPreference.GetValueParam().EnableFixLenAutoInline();
        kvIndexPreference.GetHashDictParam().SetEnableCompactHashKey(false);
        if (compressValue) {
            kvIndexPreference.GetValueParam().SetFileCompressType("lz4");
        }
        auto typeId = MakeKVTypeId(*kvConfig, nullptr);
        ASSERT_TRUE(typeId.isVarLen);
        ASSERT_EQ(compressValue, typeId.fileCompress);
    }

    size_t keyMemory = indexDir->EstimateFileMemoryUse(KV_KEY_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG);
    size_t valueMemory = indexDir->EstimateFileMemoryUse(KV_VALUE_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG);
    ASSERT_EQ(expectedInMem, valueMemory != 0);

    KVDiskIndexer kvDiskIndexer;
    size_t memoryUsed = kvDiskIndexer.EstimateMemUsed(kvConfig, dir->GetIDirectory());
    if (compressValue) {
        ASSERT_LE(keyMemory + valueMemory, memoryUsed);
    } else {
        ASSERT_EQ(keyMemory + valueMemory, memoryUsed);
    }
}

TEST_F(KVDiskIndexerTest, TestEstimateMemUsed)
{
    {
        string loadConfigStr = R"(
        {
            "load_config" :
            [
            {
                "file_patterns" : [".*"],
                "load_strategy" : "mmap", "load_strategy_param": { "lock": true }
            }
            ]
        })";
        EstimateMemUsed(loadConfigStr, true, true);
        EstimateMemUsed(loadConfigStr, false, true);
    }
    {
        string loadConfigStr = R"(
        {
            "load_config" :
            [
            {
                "file_patterns" : [".*"],
                "load_strategy" : "cache"
            }
            ]
        })";
        EstimateMemUsed(loadConfigStr, true, false);
        EstimateMemUsed(loadConfigStr, false, false);
    }
}

}} // namespace indexlibv2::index
