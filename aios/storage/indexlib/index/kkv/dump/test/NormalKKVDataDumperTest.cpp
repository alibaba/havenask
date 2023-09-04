#include "indexlib/index/kkv/dump/NormalKKVDataDumper.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index/kkv/dump/KKVDumpPhrase.h"
#include "indexlib/index/kkv/dump/KKVFileWriterOptionHelper.h"
#include "indexlib/index/kkv/dump/test/KKVVDumpTestHelper.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlibv2::config;

namespace indexlibv2::index {

class NormalKKVDataDumperTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = indexlib::file_system::Directory::Get(fs);
        _directory = rootDiretory->MakeDirectory("NormalKKVDataDumperTest", indexlib::file_system::DirectoryOption())
                         ->GetIDirectory();
        ASSERT_NE(nullptr, _directory);

        string schemaJsonStr = R"(
          {
              "index_name": "pkey_skey",
              "index_type": "PRIMARY_KEY",
              "index_fields": [
                  {
                      "field_name": "uid",
                      "key_type": "prefix"
                  },
                  {
                      "field_name": "friendid",
                      "key_type": "suffix",
                      "count_limits": 5
                  }
              ],
              "value_fields": [
                      "value"
              ]
          }
      )";
        auto fieldConfigs = config::FieldConfig::TEST_CreateFields("uid:string;friendid:int64;value:string");
        _kkvIndexConfig = std::make_shared<indexlibv2::config::KKVIndexConfig>();
        auto any = autil::legacy::json::ParseJson(schemaJsonStr);
        config::MutableJson runtimeSettings;
        config::IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
        ASSERT_NO_THROW(_kkvIndexConfig->Deserialize(any, 0, resource));
    }

    void tearDown() override {}

protected:
    autil::mem_pool::Pool _pool;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _kkvIndexConfig;
    std::shared_ptr<indexlib::file_system::IDirectory> _directory;
};

TEST_F(NormalKKVDataDumperTest, testNoSort)
{
    bool storeTs = false;
    NormalKKVDataDumper<uint64_t> dumper(_kkvIndexConfig, storeTs, KKVDumpPhrase::BUILD_DUMP);
    auto skeyOption = KKVFileWriterOptionHelper::Create(_kkvIndexConfig->GetIndexPreference().GetSkeyParam());
    auto valueOption = KKVFileWriterOptionHelper::Create(_kkvIndexConfig->GetIndexPreference().GetValueParam());
    size_t pkeyCount = 2;
    ASSERT_TRUE(dumper.Init(_directory, skeyOption, valueOption, pkeyCount).IsOK());

    bool isLastSkey = false;
    KKVVDumpTestHelper::Dump(dumper, "cmd=add,pkey=0,skey=1,value=1,ts=1,expire_time=1", isLastSkey, _pool);
    isLastSkey = true;
    KKVVDumpTestHelper::Dump(dumper, "cmd=add,pkey=0,skey=0,value=0,ts=0,expire_time=0", isLastSkey, _pool);
    // to make a different chunk offset
    dumper._valueDumper->_chunkWriter->TEST_ForceFlush();
    dumper._skeyDumper->_chunkWriter->TEST_ForceFlush();
    KKVVDumpTestHelper::Dump(dumper, "cmd=add,pkey=1,skey=2,value=2,ts=2,expire_time=2", isLastSkey, _pool);
    ASSERT_TRUE(dumper.Close().IsOK());

    string expectedValueData = R"(
     [
      {
        "chunkLength": 4,
        "chunkItems": ["1", "0"]
      },
      {
        "chunkLength": 2,
        "chunkItems":["2"]
      }
     ]
    )";
    KKVVDumpTestHelper::CheckValue(_directory, expectedValueData);

    string expectedSkeyData = R"(
     [
      {
        "chunkLength": 28,
        "chunkItems":
        [
         {"isLastNode":false,"skey":1,"chunkOffset":0,"inChunkOffset":0},
         {"isLastNode":true,"skey":0,"chunkOffset":0,"inChunkOffset":2}
        ]
      },
      {
        "chunkLength": 14,
        "chunkItems":
        [
         {"isLastNode":true,"skey":2,"chunkOffset":8,"inChunkOffset":0}
        ]
      }
     ]
    )";
    bool storeExpireTime = false;
    KKVVDumpTestHelper::CheckSKey<uint64_t>(_directory, storeTs, storeExpireTime, expectedSkeyData);

    string expectedPkeyData = R"(
        [
         {
           "pkey":0,
           "chunkOffset": 0,
           "inChunkOffset": 0,
           "blockHint": 0
         },
         {
           "pkey":1,
           "chunkOffset":32,
           "inChunkOffset": 0,
           "blockHint": 0
         }
        ]
    )";
    KKVVDumpTestHelper::CheckPKey(_directory, _kkvIndexConfig->GetIndexPreference(), expectedPkeyData);
}

TEST_F(NormalKKVDataDumperTest, testTruncSort)
{
    size_t skeyCount = 1;
    _kkvIndexConfig->SetSuffixKeyTruncateLimits(skeyCount);
    bool storeTs = false;
    NormalKKVDataDumper<uint64_t> dumper(_kkvIndexConfig, storeTs, KKVDumpPhrase::MERGE_BOTTOMLEVEL);
    auto skeyOption = KKVFileWriterOptionHelper::Create(_kkvIndexConfig->GetIndexPreference().GetSkeyParam());
    auto valueOption = KKVFileWriterOptionHelper::Create(_kkvIndexConfig->GetIndexPreference().GetValueParam());
    size_t pkeyCount = 2;
    ASSERT_TRUE(dumper.Init(_directory, skeyOption, valueOption, pkeyCount).IsOK());

    bool isLastSkey = false;
    KKVVDumpTestHelper::Dump(dumper, "cmd=add,pkey=0,skey=1,value=1,ts=1,expire_time=1", isLastSkey, _pool);
    isLastSkey = true;
    KKVVDumpTestHelper::Dump(dumper, "cmd=add,pkey=0,skey=0,value=0,ts=0,expire_time=0", isLastSkey, _pool);
    // to make a different chunk offset
    dumper._valueDumper->_chunkWriter->TEST_ForceFlush();
    dumper._skeyDumper->_chunkWriter->TEST_ForceFlush();
    KKVVDumpTestHelper::Dump(dumper, "cmd=add,pkey=1,skey=2,value=2,ts=2,expire_time=2", isLastSkey, _pool);
    ASSERT_TRUE(dumper.Close().IsOK());

    string expectedValueData = R"(
     [
      {
        "chunkLength": 2,
        "chunkItems": ["1"]
      },
      {
        "chunkLength": 2,
        "chunkItems":["2"]
      }
     ]
    )";
    KKVVDumpTestHelper::CheckValue(_directory, expectedValueData);

    string expectedSkeyData = R"(
     [
      {
        "chunkLength": 14,
        "chunkItems":
        [
         {"isLastNode":true,"skey":1,"chunkOffset":0,"inChunkOffset":0}
        ]
      },
      {
        "chunkLength": 14,
        "chunkItems":
        [
         {"isLastNode":true,"skey":2,"chunkOffset":8,"inChunkOffset":0}
        ]
      }
     ]
    )";
    bool storeExpireTime = false;
    KKVVDumpTestHelper::CheckSKey<uint64_t>(_directory, storeTs, storeExpireTime, expectedSkeyData);

    string expectedPkeyData = R"(
        [
         {
           "pkey":0,
           "chunkOffset": 0,
           "inChunkOffset": 0,
           "blockHint": 0
         },
         {
           "pkey":1,
           "chunkOffset":18,
           "inChunkOffset": 0,
           "blockHint": 0
         }
        ]
    )";
    // why second chunkOffset=18 bytes
    //  Because in first chunk, sizeof(NormalOnDiskSKeyNode<uint64_t>) is 14 bytes AND chunk meta is 4 bytes
    KKVVDumpTestHelper::CheckPKey(_directory, _kkvIndexConfig->GetIndexPreference(), expectedPkeyData);
}

} // namespace indexlibv2::index
