#include <tr1/memory>
#include <autil/StringUtil.h>
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/load_config/load_config.h"
#include "indexlib/file_system/block_file_node.h"
#include "indexlib/file_system/block_file_node_creator.h"
#include "indexlib/file_system/test/block_file_node_creator_unittest.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/cache/block_allocator.h"

using namespace std;
using namespace std::tr1;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BlockFileNodeCreatorTest);

BlockFileNodeCreatorTest::BlockFileNodeCreatorTest()
{
}

BlockFileNodeCreatorTest::~BlockFileNodeCreatorTest()
{
}

void BlockFileNodeCreatorTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mMemoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void BlockFileNodeCreatorTest::CaseTearDown()
{
}

void BlockFileNodeCreatorTest::TestCaseForInit()
{
    BlockFileNodeCreatorPtr creator(new BlockFileNodeCreator);
    LoadConfig loadConfig =
        LoadConfigListCreator::MakeBlockLoadConfig(1024000, 1024);
    creator->Init(loadConfig, mMemoryController);
    INDEXLIB_TEST_TRUE(creator->mFileBlockCache->GetBlockCache() != NULL);
    FileSystemTestUtil::CreateDiskFile(mRootDir + "file", "abcd");
    FileNodePtr fileNode = creator->CreateFileNode(mRootDir + "file", FSOT_CACHE, true);
    BlockFileNodePtr blockFileNode = dynamic_pointer_cast<BlockFileNode>(fileNode);
    INDEXLIB_TEST_TRUE(blockFileNode != NULL);
        
    creator.reset();
}

void BlockFileNodeCreatorTest::TestCaseForBadInit()
{
    BlockFileNodeCreator creator;
    LoadConfig loadConfig = LoadConfigListCreator::MakeBlockLoadConfig(10240, 102400);
    ASSERT_FALSE(creator.Init(loadConfig, mMemoryController));
}

IE_NAMESPACE_END(file_system);

