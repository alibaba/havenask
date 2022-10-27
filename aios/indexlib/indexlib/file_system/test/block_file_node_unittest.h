#ifndef __INDEXLIB_BLOCKFILENODETEST_H
#define __INDEXLIB_BLOCKFILENODETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/block_file_node.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/cache/block_cache.h"
#include "indexlib/util/cache/block_allocator.h"

IE_NAMESPACE_BEGIN(file_system);

class BlockFileNodeTest : public INDEXLIB_TESTBASE
{
public:
    BlockFileNodeTest();
    ~BlockFileNodeTest();

    DECLARE_CLASS_NAME(BlockFileNodeTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForReadWithBuffer();
    void TestCaseForReadWithDirectIO();
    void TestCaseForReadWithByteSliceList();
    void TestCaseForReadFileOutOfRangeWithBuffer();
    void TestCaseForReadFileOutOfRangeWithByteSliceList();
    void TestCaseForReadEmptyFileWithCache();
    void TestCaseForGetBlock();
    void TestCaseForGetBlocks();
    void TestCaseForPutSameBlockToCache();
    void TestCaseForPutBlockToFullCache();
    void TestCaseForGetBaseAddressException();
    void TestCaseForWriteException();
    void TestCaseForReadWithByteSliceListException();
    void TestCaseForPrefetch();
    void TestCaseForReadUInt32Async();
    void TestCaseForReadMultipleBlocks();

private:
    void GenerateFile(const std::string& fileName, size_t size, 
                      size_t blockSize);
    void GenerateFile(const std::string& fileName, const std::string& content,
                      size_t blockSize);    
    BlockFileNodePtr CreateFileNode(bool useDirectIO = false);

private:
    std::string mRootDir;
    std::string mFileName;
    uint64_t mBlockSize;
    util::BlockCachePtr mBlockCache;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadEmptyFileWithCache);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForGetBlock);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadWithBuffer);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadWithDirectIO);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadWithByteSliceList);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForPutSameBlockToCache);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForPutBlockToFullCache);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadFileOutOfRangeWithBuffer);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadFileOutOfRangeWithByteSliceList);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForGetBaseAddressException);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForWriteException);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadWithByteSliceListException);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForPrefetch);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForGetBlocks);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadUInt32Async);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeTest, TestCaseForReadMultipleBlocks);
IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BLOCKFILENODETEST_H
