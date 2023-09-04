#include "indexlib/index/kkv/dump/InlineKKVDataDumper.h"

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

class InlineKKVDataDumperTest : public TESTBASE
{
public:
    void setUp() override
    {
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
        auto rootDiretory = indexlib::file_system::Directory::Get(fs);
        _directory = rootDiretory->MakeDirectory("InlineKKVDataDumperTest", indexlib::file_system::DirectoryOption())
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
                      "key_type": "suffix"
                  }
              ],
              "value_fields": [
                      "value"
              ]
          }
      )";
        auto fieldConfigs = config::FieldConfig::TEST_CreateFields("uid:string;friendid:int64;value:string");
        config::MutableJson runtimeSettings;
        config::IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
        auto any = autil::legacy::json::ParseJson(schemaJsonStr);
        _kkvIndexConfig = std::make_shared<indexlibv2::config::KKVIndexConfig>();
        ASSERT_NO_THROW(_kkvIndexConfig->Deserialize(any, 0, resource));
    }

    void tearDown() override {}

protected:
    autil::mem_pool::Pool _pool;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _kkvIndexConfig;
    std::shared_ptr<indexlib::file_system::IDirectory> _directory;
};

TEST_F(InlineKKVDataDumperTest, test)
{
    bool storeTs = false;
    InlineKKVDataDumper<uint64_t> dumper(_kkvIndexConfig, storeTs, KKVDumpPhrase::BUILD_DUMP);
    auto skeyOption = KKVFileWriterOptionHelper::Create(_kkvIndexConfig->GetIndexPreference().GetSkeyParam());
    auto valueOption = KKVFileWriterOptionHelper::Create(_kkvIndexConfig->GetIndexPreference().GetValueParam());
    size_t pkeyCount = 2;
    ASSERT_TRUE(dumper.Init(_directory, skeyOption, valueOption, pkeyCount).IsOK());

    bool isLastSkey = false;
    KKVVDumpTestHelper::Dump(dumper, "cmd=add,pkey=0,skey=1,value=1,ts=1,expire_time=1", isLastSkey, _pool);
    isLastSkey = true;
    KKVVDumpTestHelper::Dump(dumper, "cmd=add,pkey=0,skey=0,value=0,ts=0,expire_time=0", isLastSkey, _pool);
    // to make a different chunk offset
    dumper._skeyDumper->_chunkWriter->TEST_ForceFlush();
    KKVVDumpTestHelper::Dump(dumper, "cmd=add,pkey=1,skey=2,value=2,ts=2,expire_time=2", isLastSkey, _pool);
    ASSERT_TRUE(dumper.Close().IsOK());

    string expectedSkeyData = R"(
     [
      {
        "chunkLength": 22,
        "chunkItems":
        [
         {"isLastNode":false,"skey":1,"value":"1"},
         {"isLastNode":true,"skey":0,"value":"0"}
        ]
      },
      {
        "chunkLength": 11,
        "chunkItems":
        [
         {"isLastNode":true,"skey":2,"value":"2"}
        ]
      }
     ]
    )";
    bool storeExpireTime = false;
    KKVVDumpTestHelper::CheckInlineSKey<uint64_t>(_directory, storeTs, storeExpireTime, expectedSkeyData);

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
           "chunkOffset":26,
           "inChunkOffset": 0,
           "blockHint": 0
         }
        ]
    )";
    KKVVDumpTestHelper::CheckPKey(_directory, _kkvIndexConfig->GetIndexPreference(), expectedPkeyData);
}
} // namespace indexlibv2::index
