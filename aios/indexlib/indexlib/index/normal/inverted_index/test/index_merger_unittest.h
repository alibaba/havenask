#ifndef __INDEXLIB_INDEXMERGERTEST_H
#define __INDEXLIB_INDEXMERGERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger_impl.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_writer.h"
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/SimpleAllocator.h>

IE_NAMESPACE_BEGIN(index);

class IndexMergerTest : public INDEXLIB_TESTBASE {
public:
    class IndexMergerMock : public IndexMerger
    {
    public:
        MOCK_METHOD0(CreateOnDiskIndexIteratorCreator, OnDiskIndexIteratorCreatorPtr());
        DECLARE_INDEX_MERGER_IDENTIFIER(IndexMergerMock);
    };

    class PostingMergerMock : public index::PostingMergerImpl
    {
    public:
        PostingMergerMock(PostingWriterResource* postingWriterResource)
            : PostingMergerImpl(postingWriterResource, {})
        {
        }

        MOCK_METHOD1(GetDictInlinePostingValue, bool(uint64_t& inlinePostingValue));
        MOCK_METHOD0(GetDocFreq, df_t());
        MOCK_METHOD0(GetTotalTF, ttf_t());
    
        MOCK_METHOD1(Dump, void(storage::FileWriter* postingFile));
    };

    // class PostingDumperMock : public index::PostingDumperImpl
    // {
    // public:
    //     PostingDumperMock(PostingWriterResource* postingWriterResource)
    //         : PostingDumperImpl(postingWriterResource)
    //     {
    //     }

    //     MOCK_METHOD1(GetDictInlinePostingValue, bool(uint64_t& inlinePostingValue));
    //     MOCK_CONST_METHOD0(GetDocFreq, df_t());
    //     MOCK_CONST_METHOD0(GetTotalTF, ttf_t());
    
    //     MOCK_METHOD1(Dump, void(storage::FileWriter* postingFile));
    // };


    class DictionaryWriterMock : public DictionaryWriter
    {
    public:
        MOCK_METHOD1(Open, void(const std::string&));
        MOCK_METHOD2(Open, void(const file_system::DirectoryPtr& directory,
                                const std::string& fileName));
        MOCK_METHOD2(AddItem, void(dictkey_t, dictvalue_t));
        MOCK_METHOD0(Close, void());
        MOCK_METHOD1(Reserve, void(size_t itemCount));
    };

public:
    IndexMergerTest();
    ~IndexMergerTest();

    DECLARE_CLASS_NAME(IndexMergerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    //void TestGetDictValue();
    //void TestDumpPosting();
    void TestGetMaxLengthOfPostingWithDictInline();
    void TestMergeTermPoolReset();
    void TestGetMergedDir();
private:
    std::tr1::shared_ptr<autil::mem_pool::Pool> mByteSlicePool;
    std::tr1::shared_ptr<autil::mem_pool::RecyclePool> mBufferPool;
    util::SimplePool mSimplePool;
    autil::mem_pool::SimpleAllocator mAllocator;
    PostingWriterResourcePtr mPostingWriterResource;

private:
    IE_LOG_DECLARE();
};

//INDEXLIB_UNIT_TEST_CASE(IndexMergerTest, TestGetDictValue);
//INDEXLIB_UNIT_TEST_CASE(IndexMergerTest, TestDumpPosting);
INDEXLIB_UNIT_TEST_CASE(IndexMergerTest, TestGetMaxLengthOfPostingWithDictInline);
INDEXLIB_UNIT_TEST_CASE(IndexMergerTest, TestMergeTermPoolReset);
INDEXLIB_UNIT_TEST_CASE(IndexMergerTest, TestGetMergedDir);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEXMERGERTEST_H

