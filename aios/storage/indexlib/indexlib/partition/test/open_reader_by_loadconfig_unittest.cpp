#include "indexlib/partition/test/open_reader_by_loadconfig_unittest.h"

#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/cache/BlockCache.h"

using namespace std;

using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::test;
using namespace indexlib::common;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OpenReaderByLoadconfigTest);

OpenReaderByLoadconfigTest::OpenReaderByLoadconfigTest() {}

OpenReaderByLoadconfigTest::~OpenReaderByLoadconfigTest() {}

void OpenReaderByLoadconfigTest::CaseSetUp() { mRootDir = GET_TEMP_DATA_PATH(); }

void OpenReaderByLoadconfigTest::CaseTearDown() {}

void OpenReaderByLoadconfigTest::TestOpenReaderWithEmpty()
{
    DoTestReaderWithLoadConfig(LoadConfigList(), FSOT_LOAD_CONFIG, FSFT_MMAP);
}

void OpenReaderByLoadconfigTest::TestOpenReaderWithMmap()
{
    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_MMAP);
    DoTestReaderWithLoadConfig(loadConfigList, FSOT_LOAD_CONFIG, FSFT_MMAP);
    ASSERT_EQ(0, mPsm.GetIndexPartitionResource()
                     .fileBlockCacheContainer->GetAvailableFileCache("")
                     ->GetBlockCache()
                     ->GetBlockCount());
}

void OpenReaderByLoadconfigTest::TestOpenReaderWithCache()
{
    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_CACHE);
    DoTestReaderWithLoadConfig(loadConfigList, FSOT_LOAD_CONFIG, FSFT_BLOCK);
    ASSERT_EQ(0, mPsm.GetIndexPartitionResource()
                     .fileBlockCacheContainer->GetAvailableFileCache("")
                     ->GetBlockCache()
                     ->GetBlockCount());
}

void OpenReaderByLoadconfigTest::TestOpenReaderWithGlobalCache()
{
    LoadConfigList loadConfigList = LoadConfigListCreator::CreateLoadConfigList(READ_MODE_GLOBAL_CACHE);
    DoTestReaderWithLoadConfig(loadConfigList, FSOT_LOAD_CONFIG, FSFT_BLOCK);
    ASSERT_LT(0, mPsm.GetIndexPartitionResource()
                     .fileBlockCacheContainer->GetAvailableFileCache("")
                     ->GetBlockCache()
                     ->GetBlockCount());
}

void OpenReaderByLoadconfigTest::DoTestReaderWithLoadConfig(LoadConfigList loadConfigList, FSOpenType expectOpenType,
                                                            FSFileType expectFileType)
{
    string field = "text1:text;string1:string";
    string index = "text:pack:text1;"
                   "pk:primarykey64:string1";
    string summary = "string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", summary);
    IndexPartitionOptions options;
    options.GetOnlineConfig().loadConfigList = loadConfigList;
    INDEXLIB_TEST_TRUE(mPsm.Init(schema, options, mRootDir));
    ASSERT_EQ(0, mPsm.GetIndexPartitionResource()
                     .fileBlockCacheContainer->GetAvailableFileCache("")
                     ->GetBlockCache()
                     ->GetBlockCount());

    string docString = "cmd=add,string1=hello,text1=world";
    INDEXLIB_TEST_TRUE(mPsm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    INDEXLIB_TEST_TRUE(mPsm.Transfer(QUERY, "", "pk:hello", "string1=hello"));
    OnlinePartitionPtr onlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, mPsm.GetIndexPartition());
    file_system::IFileSystemPtr ifs = onlinePartition->GetFileSystem();
    CheckFileStat(ifs, "segment_0_level_0/index/pk/attribute_string1/data", expectOpenType, expectFileType);
    CheckFileStat(ifs, "segment_0_level_0/index/text_section/offset", expectOpenType, expectFileType);
    CheckFileStat(ifs, "segment_0_level_0/index/text_section/data", expectOpenType, expectFileType);
}

void OpenReaderByLoadconfigTest::CheckFileStat(file_system::IFileSystemPtr fileSystem, string filePath,
                                               FSOpenType expectOpenType, FSFileType expectFileType)
{
    SCOPED_TRACE(filePath);
    file_system::FileStat fileStat;
    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(filePath, fileStat));
    ASSERT_EQ(expectOpenType, fileStat.openType);
    ASSERT_EQ(expectFileType, fileStat.fileType);
}
}} // namespace indexlib::partition
