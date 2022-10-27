#ifndef __INDEXLIB_POSTINGMERGERIMPLTEST_H
#define __INDEXLIB_POSTINGMERGERIMPLTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger_impl.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/directory.h"
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/SimpleAllocator.h>

IE_NAMESPACE_BEGIN(index);

class PostingMergerImplTest : public INDEXLIB_TESTBASE {
public:
    PostingMergerImplTest();
    ~PostingMergerImplTest();

    DECLARE_CLASS_NAME(PostingMergerImplTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestDump();

private:
    void InitFiles();
    void CloseFiles();

private:
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::RecyclePool* mBufferPool;
    util::SimplePool mSimplePool;
    autil::mem_pool::SimpleAllocator mAllocator;
    std::string mRootPath;
    file_system::DirectoryPtr mRootDirectory;
    file_system::FileWriterPtr mPostingFileWriter;
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PostingMergerImplTest, TestDump);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTINGMERGERIMPLTEST_H
