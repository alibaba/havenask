#pragma once

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryWriter.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_merger.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger_impl.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class IndexMergerTest : public INDEXLIB_TESTBASE
{
public:
    class IndexMergerMock : public NormalIndexMerger
    {
    public:
        MOCK_METHOD(std::shared_ptr<legacy::OnDiskIndexIteratorCreator>, CreateOnDiskIndexIteratorCreator, (),
                    (override));
        DECLARE_INDEX_MERGER_IDENTIFIER(IndexMergerMock);
    };

    class PostingMergerMock : public legacy::PostingMergerImpl
    {
    public:
        PostingMergerMock(PostingWriterResource* postingWriterResource)
            : legacy::PostingMergerImpl(postingWriterResource, {})
        {
        }

        MOCK_METHOD(bool, GetDictInlinePostingValue, (uint64_t & inlinePostingValue), (override));
        MOCK_METHOD(df_t, GetDocFreq, (), (override));
        MOCK_METHOD(ttf_t, GetTotalTF, (), (override));
    };

    // class PostingDumperMock : public index::PostingDumperImpl
    // {
    // public:
    //     PostingDumperMock(PostingWriterResource* postingWriterResource)
    //         : PostingDumperImpl(postingWriterResource)
    //     {
    //     }

    //     MOCK_METHOD(bool, GetDictInlinePostingValue, (uint64_t& inlinePostingValue), (override));
    //     MOCK_METHOD(df_t, GetDocFreq, (), (const, override));
    //     MOCK_METHOD(ttf_t, GetTotalTF, (), (const, override));

    //     MOCK_METHOD(void, Dump, (storage::FileWriter* postingFile), (override));
    // };

    class DictionaryWriterMock : public DictionaryWriter
    {
    public:
        using DictionaryWriter::AddItem;
        using DictionaryWriter::Open;
        MOCK_METHOD(void, Open, (const std::string&), ());
        MOCK_METHOD(void, Open, (const file_system::DirectoryPtr& directory, const std::string& fileName), (override));
        MOCK_METHOD(void, AddItem, (dictkey_t, dictvalue_t), ());
        MOCK_METHOD(void, Close, (), (override));
        MOCK_METHOD(void, Reserve, (size_t itemCount), ());
    };

public:
    IndexMergerTest();
    ~IndexMergerTest();

    DECLARE_CLASS_NAME(IndexMergerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    // void TestGetDictValue();
    // void TestDumpPosting();
    void TestGetMaxLengthOfPostingWithDictInline();
    void TestMergeTermPoolReset();
    void TestGetMergedDir();

private:
    std::shared_ptr<autil::mem_pool::Pool> mByteSlicePool;
    std::shared_ptr<autil::mem_pool::RecyclePool> mBufferPool;
    util::SimplePool mSimplePool;
    autil::mem_pool::SimpleAllocator mAllocator;
    std::shared_ptr<PostingWriterResource> mPostingWriterResource;

private:
    IE_LOG_DECLARE();
};

// INDEXLIB_UNIT_TEST_CASE(IndexMergerTest, TestGetDictValue);
// INDEXLIB_UNIT_TEST_CASE(IndexMergerTest, TestDumpPosting);
INDEXLIB_UNIT_TEST_CASE(IndexMergerTest, TestGetMaxLengthOfPostingWithDictInline);
INDEXLIB_UNIT_TEST_CASE(IndexMergerTest, TestMergeTermPoolReset);
INDEXLIB_UNIT_TEST_CASE(IndexMergerTest, TestGetMergedDir);
}} // namespace indexlib::index
