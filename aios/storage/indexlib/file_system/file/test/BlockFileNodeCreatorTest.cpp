#include "indexlib/file_system/file/BlockFileNodeCreator.h"

#include "gtest/gtest.h"
#include <cstddef>
#include <memory>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "indexlib/file_system/DiskStorage.h"
#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {

class BlockFileNodeCreatorTest : public INDEXLIB_TESTBASE
{
public:
    BlockFileNodeCreatorTest();
    ~BlockFileNodeCreatorTest();

    DECLARE_CLASS_NAME(BlockFileNodeCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForInit();
    void TestCaseForBadInit();

private:
    std::string _rootDir;
    util::BlockMemoryQuotaControllerPtr _memoryController;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, BlockFileNodeCreatorTest);

INDEXLIB_UNIT_TEST_CASE(BlockFileNodeCreatorTest, TestCaseForInit);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeCreatorTest, TestCaseForBadInit);
//////////////////////////////////////////////////////////////////////

BlockFileNodeCreatorTest::BlockFileNodeCreatorTest() {}

BlockFileNodeCreatorTest::~BlockFileNodeCreatorTest() {}

void BlockFileNodeCreatorTest::CaseSetUp()
{
    _rootDir = PathUtil::NormalizePath(GET_TEMPLATE_DATA_PATH());
    _memoryController = MemoryQuotaControllerCreator::CreateBlockMemoryController();
}

void BlockFileNodeCreatorTest::CaseTearDown() {}

void BlockFileNodeCreatorTest::TestCaseForInit()
{
    FileBlockCacheContainerPtr cacheContainer(new FileBlockCacheContainer());
    cacheContainer->Init("", nullptr);
    BlockFileNodeCreatorPtr creator(new BlockFileNodeCreator(cacheContainer, ""));
    LoadConfig loadConfig = LoadConfigListCreator::MakeBlockLoadConfig("lru", 1024000, 1024, 4);
    creator->Init(loadConfig, _memoryController);
    ASSERT_TRUE(creator->_fileBlockCache->GetBlockCache() != NULL);
    FileSystemTestUtil::CreateDiskFile(_rootDir + "file", "abcd");
    std::shared_ptr<FileNode> fileNode = creator->CreateFileNode(FSOT_CACHE, true, "");
    std::shared_ptr<BlockFileNode> blockFileNode = dynamic_pointer_cast<BlockFileNode>(fileNode);
    ASSERT_TRUE(blockFileNode != NULL);

    creator.reset();
}

void BlockFileNodeCreatorTest::TestCaseForBadInit()
{
    FileBlockCacheContainerPtr cacheContainer(new FileBlockCacheContainer());
    cacheContainer->Init("", nullptr);
    BlockFileNodeCreator creator(cacheContainer, "");
    LoadConfig loadConfig = LoadConfigListCreator::MakeBlockLoadConfig("lru", 10240, 102400, 4);
    ASSERT_FALSE(creator.Init(loadConfig, _memoryController));
}
}} // namespace indexlib::file_system
