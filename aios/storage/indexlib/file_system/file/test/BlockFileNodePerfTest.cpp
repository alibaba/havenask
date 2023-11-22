#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/cache/BlockCacheCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class BlockFileNodePerfTest : public INDEXLIB_TESTBASE
{
public:
    BlockFileNodePerfTest();
    ~BlockFileNodePerfTest();

    DECLARE_CLASS_NAME(BlockFileNodePerfTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForReadMultipleBlocks();

private:
    std::shared_ptr<BlockFileNode> CreateFileNode(bool useDirectIO);
    void GenerateFile(const std::string& fileName, size_t size, size_t blockSize);

private:
    std::string _rootDir;
    std::string _fileName;
    uint64_t _blockSize;
    uint64_t _iOBatchSize;
    util::BlockCachePtr _blockCache;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BlockFileNodePerfTest, TestCaseForReadMultipleBlocks);
AUTIL_LOG_SETUP(indexlib.file_system, BlockFileNodePerfTest);

BlockFileNodePerfTest::BlockFileNodePerfTest() {}

BlockFileNodePerfTest::~BlockFileNodePerfTest() {}

void BlockFileNodePerfTest::CaseSetUp()
{
    _rootDir = PathUtil::NormalizePath(GET_NON_RAMDISK_PATH());

    _fileName = _rootDir + "block_file";
    _blockSize = 4;
    _iOBatchSize = 4;
}

void BlockFileNodePerfTest::CaseTearDown() {}

void BlockFileNodePerfTest::TestCaseForReadMultipleBlocks()
{
    constexpr uint32_t fileSize = 20000 * 1024 + 1;
    _blockSize = 512;
    GenerateFile(_fileName, fileSize, _blockSize);
    std::shared_ptr<BlockFileNode> blockFileNode = CreateFileNode(true);
    ASSERT_EQ(FSEC_OK, blockFileNode->Open("LOGICAL_PATH", _fileName, FSOT_CACHE, -1));

    // The stack is used to occupy most of the default stack size(8Mb) so that the following
    // blockFileNode->Read can be tested to not used too much stack.
    // Previously blockFileNode->Read had problem stack overflow due to too many internal
    // recursion.
    static constexpr size_t STACK_ARRAY_SIZE = 8300000;
    char stack[STACK_ARRAY_SIZE] = {0};
    constexpr size_t length = fileSize;
    size_t sum = 0;
    for (size_t i = 0; i < STACK_ARRAY_SIZE; ++i) {
        sum += (int)stack[i];
    }
    cout << sum << endl;
    auto buffer = new char[length];
    auto readSize = blockFileNode->Read(buffer, length, 0, ReadOption()).GetOrThrow();
    ASSERT_EQ(fileSize, readSize);
    delete[] buffer;
}

void BlockFileNodePerfTest::GenerateFile(const std::string& fileName, size_t size, size_t blockSize)
{
    if (FslibWrapper::IsExist(fileName).GetOrThrow()) {
        ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(fileName, DeleteOption::NoFence(false)).Code());
    }
    auto file = FslibWrapper::OpenFile(fileName, fslib::WRITE).GetOrThrow();
    assert(file != NULL);
    char* buffer = new char[blockSize];
    char content = 0;
    size_t n = size / blockSize;
    size_t tail = size % blockSize;
    for (size_t i = 0; i < n; ++i) {
        memset(buffer, 'a' + content, blockSize);
        file->WriteE((void*)buffer, blockSize);
        content = (content + 1) % 26;
    }
    if (tail != 0) {
        memset(buffer, 'a' + content, tail);
        file->WriteE((void*)buffer, tail);
    }
    file->CloseE();
    delete[] buffer;
    buffer = NULL;
}

std::shared_ptr<BlockFileNode> BlockFileNodePerfTest::CreateFileNode(bool useDirectIO)
{
    BlockCacheOption option = BlockCacheOption::LRU(4 * _blockSize, _blockSize, _iOBatchSize);
    option.cacheParams["num_shard_bits"] = "1";
    _blockCache.reset(BlockCacheCreator::Create(option));
    std::shared_ptr<BlockFileNode> blockFileNode(new BlockFileNode(_blockCache.get(), useDirectIO, false, false, ""));
    return blockFileNode;
}
}} // namespace indexlib::file_system
