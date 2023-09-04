#include "indexlib/partition/test/offline_partition_reader_unittest.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;

using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OfflinePartitionReaderTest);

OfflinePartitionReaderTest::OfflinePartitionReaderTest() {}

OfflinePartitionReaderTest::~OfflinePartitionReaderTest() {}

void OfflinePartitionReaderTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    mSchema = SchemaMaker::MakeSchema(
        // Field schema
        "string0:string;string1:string;long1:uint32;long2:uint16:true;",
        // Primary key index schema
        "pk:primarykey64:string1;strIndex:string:string0",
        // Attribute schema
        "long1;string0;string1;",
        // Summary schema
        "long1;long2;string1");
}

void OfflinePartitionReaderTest::CaseTearDown() {}

void OfflinePartitionReaderTest::TestCacheLoadConfig()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    auto pkConfig =
        DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(pkConfig);
    pkConfig->SetPrimaryKeyIndexType(pk_block_array);
    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
    string docString = "cmd=add,string0=hi0,string1=ha0,long1=10,long2=20 30;"
                       "cmd=add,string0=hi1,string1=ha1,long1=11,long2=21 31;"
                       "cmd=add,string0=hi2,string1=ha2,long1=12,long2=22 32;"
                       "cmd=add,string0=hi3,string1=ha3,long1=13,long2=23 33;"
                       "cmd=delete,string1=ha3;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    OfflinePartition offlinePartition;
    options.SetIsOnline(false);
    options.GetOfflineConfig().readerConfig.useBlockCache = true;
    offlinePartition.Open(mRootDir, "", mSchema, options);
    IndexPartitionReaderPtr indexReader = offlinePartition.GetReader();
    ASSERT_TRUE(indexReader);
    PrimaryKeyIndexReaderPtr pkReader = indexReader->GetPrimaryKeyReader();
    ASSERT_TRUE(pkReader);
    AttributeReaderPtr attrReader = indexReader->GetAttributeReader("long1");
    ASSERT_TRUE(attrReader);

    DirectoryPtr rootDir = offlinePartition.GetRootDirectory();
    ASSERT_TRUE(rootDir);
    IFileSystemPtr fileSystem = rootDir->GetFileSystem();
    ASSERT_TRUE(fileSystem);
    StorageMetrics localMetrics = fileSystem->GetFileSystemMetrics().GetInputStorageMetrics();
    // summary data/offset & index posting & dictionary & pk attribute file load as cache
    ASSERT_EQ((int64_t)10, localMetrics.GetFileCount(FSMG_LOCAL, FSFT_MEM));
    ASSERT_EQ((int64_t)10, localMetrics.GetFileCount(FSMG_LOCAL, FSFT_BLOCK));
}

void OfflinePartitionReaderTest::TestSkipLoadIndex()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
    string docString = "cmd=add,string0=hi0,string1=ha0,long1=10,long2=20 30;"
                       "cmd=add,string0=hi1,string1=ha1,long1=11,long2=21 31;"
                       "cmd=add,string0=hi2,string1=ha2,long1=12,long2=22 32;"
                       "cmd=add,string0=hi3,string1=ha3,long1=13,long2=23 33;"
                       "cmd=delete,string1=ha3;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    OfflinePartition offlinePartition;
    options.SetIsOnline(false);
    options.GetOfflineConfig().readerConfig.loadIndex = false;
    offlinePartition.Open(mRootDir, "", mSchema, options);
    IndexPartitionReaderPtr indexReader = offlinePartition.GetReader();
    ASSERT_TRUE(indexReader);

    ASSERT_NO_THROW(indexReader->GetInvertedIndexReader("strIndex"));

    // pk reader will load on call
    PrimaryKeyIndexReaderPtr pkReader = indexReader->GetPrimaryKeyReader();
    ASSERT_TRUE(pkReader);
}

void OfflinePartitionReaderTest::TestUseLazyLoadAttributeReaders()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));
    OfflinePartition offlinePartition;
    options.SetIsOnline(false);
    offlinePartition.Open(mRootDir, "", mSchema, options);
    IndexPartitionReaderPtr indexReader = offlinePartition.GetReader();
    ASSERT_TRUE(indexReader);
}
}} // namespace indexlib::partition
