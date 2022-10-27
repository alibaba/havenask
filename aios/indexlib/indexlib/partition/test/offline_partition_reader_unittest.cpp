#include "indexlib/partition/test/offline_partition_reader_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OfflinePartitionReaderTest);

OfflinePartitionReaderTest::OfflinePartitionReaderTest()
{
}

OfflinePartitionReaderTest::~OfflinePartitionReaderTest()
{
}

void OfflinePartitionReaderTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSchema = SchemaMaker::MakeSchema(
            //Field schema
            "string0:string;string1:string;long1:uint32;long2:uint16:true;",
            //Primary key index schema
            "pk:primarykey64:string1;strIndex:string:string0",
            //Attribute schema
            "long1;string0;string1;",
            //Summary schema
            "long1;long2;string1");    
}

void OfflinePartitionReaderTest::CaseTearDown()
{
}

void OfflinePartitionReaderTest::TestCacheLoadConfig()
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
    options.GetOfflineConfig().readerConfig.useBlockCache = true;
    offlinePartition.Open(mRootDir, "", mSchema, options);
    IndexPartitionReaderPtr indexReader = offlinePartition.GetReader();
    ASSERT_TRUE(indexReader);
    PrimaryKeyIndexReaderPtr pkReader = indexReader->GetPrimaryKeyReader();
    ASSERT_TRUE(pkReader);
    
    DirectoryPtr rootDir = offlinePartition.GetRootDirectory();
    ASSERT_TRUE(rootDir);
    IndexlibFileSystemPtr fileSystem = rootDir->GetFileSystem();
    ASSERT_TRUE(fileSystem);
    StorageMetrics localMetrics = fileSystem->GetStorageMetrics(FSST_LOCAL);
    // summary & index posting & dictionary file load as cache
    ASSERT_EQ((int64_t)3, localMetrics.GetFileCount(FSFT_BLOCK));
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
    // normal index not support load on call yet
    ASSERT_ANY_THROW(indexReader->GetIndexReader("strIndex"));

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
    ASSERT_TRUE(indexReader->GetAttributeReader("long1")->IsLazyLoad());
    ASSERT_TRUE(indexReader->GetAttributeReader("string0")->IsLazyLoad());
    ASSERT_TRUE(indexReader->GetAttributeReader("string1")->IsLazyLoad());        
}

IE_NAMESPACE_END(partition);

