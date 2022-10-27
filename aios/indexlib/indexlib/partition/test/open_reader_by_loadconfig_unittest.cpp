#include "indexlib/util/path_util.h"
#include "indexlib/util/cache/block_cache.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/test/open_reader_by_loadconfig_unittest.h"

using namespace std;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OpenReaderByLoadconfigTest);

OpenReaderByLoadconfigTest::OpenReaderByLoadconfigTest()
{
}

OpenReaderByLoadconfigTest::~OpenReaderByLoadconfigTest()
{
}

void OpenReaderByLoadconfigTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void OpenReaderByLoadconfigTest::CaseTearDown()
{
}

void OpenReaderByLoadconfigTest::TestOpenReaderWithEmpty()
{
    DoTestReaderWithLoadConfig(LoadConfigList(), FSOT_LOAD_CONFIG, FSFT_MMAP);
}

void OpenReaderByLoadconfigTest::TestOpenReaderWithMmap()
{
    LoadConfigList loadConfigList = 
        LoadConfigListCreator::CreateLoadConfigList(READ_MODE_MMAP);
    DoTestReaderWithLoadConfig(loadConfigList, FSOT_LOAD_CONFIG, FSFT_MMAP);
    ASSERT_EQ(0, mPsm.GetIndexPartitionResource().fileBlockCache->GetBlockCache()->GetBlockCount());
}

void OpenReaderByLoadconfigTest::TestOpenReaderWithCache()
{
    LoadConfigList loadConfigList = 
        LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
    DoTestReaderWithLoadConfig(loadConfigList, FSOT_IN_MEM, FSFT_IN_MEM);
    ASSERT_EQ(0, mPsm.GetIndexPartitionResource().fileBlockCache->GetBlockCache()->GetBlockCount());
}

void OpenReaderByLoadconfigTest::TestOpenReaderWithGlobalCache()
{
    LoadConfigList loadConfigList = 
        LoadConfigListCreator::CreateLoadConfigList(READ_MODE_GLOBAL_CACHE);
    DoTestReaderWithLoadConfig(loadConfigList, FSOT_IN_MEM, FSFT_IN_MEM);
    ASSERT_LT(0, mPsm.GetIndexPartitionResource().fileBlockCache->GetBlockCache()->GetBlockCount());
}

void OpenReaderByLoadconfigTest::DoTestReaderWithLoadConfig(
        LoadConfigList loadConfigList, FSOpenType expectOpenType, 
        FSFileType expectFileType)
{
    string field = "text1:text;string1:string";
    string index = "text:pack:text1;"
                   "pk:primarykey64:string1";
    string summary = "string1";
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema(field, index, "", summary);
    IndexPartitionOptions options;
    options.GetOnlineConfig().loadConfigList = loadConfigList;
    INDEXLIB_TEST_TRUE(mPsm.Init(schema, options, mRootDir));
    ASSERT_EQ(0, mPsm.GetIndexPartitionResource().fileBlockCache->GetBlockCache()->GetBlockCount());

    string docString = "cmd=add,string1=hello,text1=world";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(QUERY, "", "pk:hello", "string1=hello"));
    OnlinePartitionPtr onlinePartition = DYNAMIC_POINTER_CAST(
            OnlinePartition, mPsm.GetIndexPartition());
    file_system::IndexlibFileSystemPtr ifs = onlinePartition->GetFileSystem();
    CheckFileStat(ifs, "index/pk/attribute_string1/data", expectOpenType, expectFileType);
    CheckFileStat(ifs, "index/text_section/offset", expectOpenType, expectFileType); 
    CheckFileStat(ifs, "index/text_section/data", expectOpenType, expectFileType);
}

void OpenReaderByLoadconfigTest::CheckFileStat(
        file_system::IndexlibFileSystemPtr fileSystem, string filePath, 
        FSOpenType expectOpenType, FSFileType expectFileType)
{
    SCOPED_TRACE(filePath);
    string absPath = PathUtil::JoinPath(mRootDir, "segment_0_level_0", filePath);
    file_system::FileStat fileStat;
    fileSystem->GetFileStat(absPath, fileStat);
    ASSERT_EQ(expectOpenType, fileStat.openType);
    ASSERT_EQ(expectFileType, fileStat.fileType);
}

IE_NAMESPACE_END(partition);

