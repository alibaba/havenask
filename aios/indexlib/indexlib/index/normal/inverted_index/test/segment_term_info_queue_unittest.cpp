#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/inverted_index/test/index_iterator_mock.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info_queue.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index/normal/inverted_index/test/segment_term_info_queue_mock.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

class SegmentTermInfoQueueTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SegmentTermInfoQueueTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForAddQueueItem()
    {
        {
            //case nomal 
            SegmentTermInfoQueuePtr termInfoQueue(
                    CreateSegmentTermInfoQueueMock(DictionaryConfigPtr()));
            SegmentMergeInfo info;
            info.segmentId = 1;
            info.baseDocId = 2;

            vector<dictkey_t> keys;
            keys.push_back(1);
            IndexIteratorMockPtr indexIt(new IndexIteratorMock(keys));

            termInfoQueue->AddQueueItem(info, indexIt, SegmentTermInfo::TM_NORMAL);
            SegmentTermInfo* item = termInfoQueue->Top();
            INDEXLIB_TEST_EQUAL(1, item->segId);
            INDEXLIB_TEST_EQUAL(2, item->baseDocId);
            INDEXLIB_TEST_EQUAL((size_t)1, termInfoQueue->Size());
        }

        {
            //case queItem has no value
            SegmentTermInfoQueuePtr termInfoQueue(
                    CreateSegmentTermInfoQueueMock(DictionaryConfigPtr()));
            SegmentMergeInfo info;
            vector<dictkey_t> keys;
            IndexIteratorMockPtr indexIt(new IndexIteratorMock(keys));

            termInfoQueue->AddQueueItem(info, indexIt, SegmentTermInfo::TM_NORMAL);
            INDEXLIB_TEST_EQUAL((size_t)0, termInfoQueue->Size());
        }

        {
            //case indexIterator null
            SegmentTermInfoQueuePtr termInfoQueue(
                    CreateSegmentTermInfoQueueMock(DictionaryConfigPtr()));
            SegmentMergeInfo info;
            IndexIteratorMockPtr indexIt;
            
            termInfoQueue->AddQueueItem(info, indexIt, SegmentTermInfo::TM_NORMAL);
            INDEXLIB_TEST_EQUAL((size_t)0, termInfoQueue->Size());
        }
    }

    void TestCaseForAddOnDiskTermInfo()
    {
        {  
            //case for only normal
            vector<dictkey_t> keys;
            keys.push_back(1);
            
            SegmentTermInfoQueueMockPtr termInfoQueue(
                    CreateSegmentTermInfoQueueMock(
                            DictionaryConfigPtr()));
            termInfoQueue->SetNormalIndexIterator(keys);

            SegmentMergeInfo info;
            termInfoQueue->AddOnDiskTermInfo(info);

            INDEXLIB_TEST_EQUAL((size_t)1, termInfoQueue->Size());
            SegmentTermInfo* item = termInfoQueue->Top();
            INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, item->termIndexMode);
        }
        
        {  
            //case for only bitmap
            DictionaryConfigPtr dicConfig(
                    new DictionaryConfig("test", ""));
            vector<dictkey_t> keys;
            keys.push_back(1);

            SegmentTermInfoQueueMockPtr termInfoQueue(
                    CreateSegmentTermInfoQueueMock(dicConfig));
            termInfoQueue->SetBitmapIndexIterator(keys);

            SegmentMergeInfo info;
            termInfoQueue->AddOnDiskTermInfo(info);

            INDEXLIB_TEST_EQUAL((size_t)1, termInfoQueue->Size());
            SegmentTermInfo* item = termInfoQueue->Top();
            INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_BITMAP, item->termIndexMode);
        }
    }

    void TestCaseForCurrentTermInfos()
    {
        {
            //single value and normal mode
            
            DictionaryConfigPtr dicConfig(
                    new DictionaryConfig("test", ""));
            SegmentTermInfoQueueMockPtr termInfoQueue(
                    CreateSegmentTermInfoQueueMock(dicConfig));

            vector<dictkey_t> keys;
            keys.push_back(1);

            SegmentMergeInfo info;
            info.segmentId = 1;
            info.baseDocId = 2;

            AddTermInfo(keys, SegmentTermInfo::TM_NORMAL,
                        info, termInfoQueue);

            
            dictkey_t key;
            SegmentTermInfo::TermIndexMode mode;
            const SegmentTermInfos &termInfos = termInfoQueue->CurrentTermInfos(key, mode);
            INDEXLIB_TEST_EQUAL((size_t)1, termInfos.size());
            INDEXLIB_TEST_EQUAL((dictkey_t)1, key);
            INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, mode);
            termInfoQueue->MoveToNextTerm();
        }

        {
            //multi value and normal mode            
            DictionaryConfigPtr dicConfig(
                    new DictionaryConfig("test", ""));
            SegmentTermInfoQueueMockPtr termInfoQueue(
                    CreateSegmentTermInfoQueueMock(dicConfig));

            vector<dictkey_t> keys;
            keys.push_back(1);

            SegmentMergeInfo info;
            info.segmentId = 1;
            info.baseDocId = 2;

            AddTermInfo(keys, SegmentTermInfo::TM_NORMAL,
                        info, termInfoQueue);

            keys.push_back(2);
            info.segmentId = 2;
            info.baseDocId = 3;
            AddTermInfo(keys, SegmentTermInfo::TM_NORMAL,
                        info, termInfoQueue);


            dictkey_t key;
            SegmentTermInfo::TermIndexMode mode;
            const SegmentTermInfos &termInfos = termInfoQueue->CurrentTermInfos(key, mode);
            INDEXLIB_TEST_EQUAL((size_t)2, termInfos.size());
            INDEXLIB_TEST_EQUAL((dictkey_t)1, key);
            INDEXLIB_TEST_EQUAL(1, termInfos[0]->segId);
            INDEXLIB_TEST_EQUAL(2, termInfos[1]->segId);
            INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, mode);
            termInfoQueue->MoveToNextTerm();
        }

        {
            //multi value and bitmap and normal            
            DictionaryConfigPtr dicConfig(
                    new DictionaryConfig("test", ""));
            SegmentTermInfoQueueMockPtr termInfoQueue(
                    CreateSegmentTermInfoQueueMock(dicConfig));

            vector<dictkey_t> keys;
            keys.push_back(1);

            SegmentMergeInfo info;
            info.segmentId = 1;
            info.baseDocId = 2;

            AddTermInfo(keys, SegmentTermInfo::TM_BITMAP,
                        info, termInfoQueue);

            info.segmentId = 2;
            info.baseDocId = 3;
            AddTermInfo(keys, SegmentTermInfo::TM_NORMAL,
                        info, termInfoQueue);


            dictkey_t key;
            SegmentTermInfo::TermIndexMode mode;
            const SegmentTermInfos &termInfos = termInfoQueue->CurrentTermInfos(key, mode);
            INDEXLIB_TEST_EQUAL((size_t)1, termInfos.size());
            INDEXLIB_TEST_EQUAL((dictkey_t)1, key);
            INDEXLIB_TEST_EQUAL(2, termInfos[0]->segId);
            INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, mode);
            termInfoQueue->MoveToNextTerm();
        }

    }
    
    void TestCaseForGetSortedSegmentTermInfos()
    {
        DictionaryConfigPtr dicConfig(
                new DictionaryConfig("test", ""));
        SegmentTermInfoQueueMockPtr termInfoQueue(
                CreateSegmentTermInfoQueueMock(dicConfig));
        PrepareForCaseGetSortedSegmentTermInfos(termInfoQueue);

        dictkey_t key;
        SegmentTermInfo::TermIndexMode mode;
        SegmentTermInfos termInfos = termInfoQueue->CurrentTermInfos(key, mode);
        INDEXLIB_TEST_EQUAL((size_t)2, termInfos.size());
        INDEXLIB_TEST_EQUAL((dictkey_t)1, key);
        INDEXLIB_TEST_EQUAL(1, termInfos[0]->segId);
        INDEXLIB_TEST_EQUAL(2, termInfos[1]->segId);
        INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, mode);
        
        termInfoQueue->MoveToNextTerm();
        termInfos.clear();
        termInfos = termInfoQueue->CurrentTermInfos(key, mode);
        INDEXLIB_TEST_EQUAL((size_t)2, termInfos.size());
        INDEXLIB_TEST_EQUAL((dictkey_t)1, key);
        INDEXLIB_TEST_EQUAL(1, termInfos[0]->segId);
        INDEXLIB_TEST_EQUAL(2, termInfos[1]->segId);
        INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_BITMAP, mode);

        termInfoQueue->MoveToNextTerm();
        termInfos.clear();
        termInfos = termInfoQueue->CurrentTermInfos(key, mode);
        INDEXLIB_TEST_EQUAL((size_t)1, termInfos.size());
        INDEXLIB_TEST_EQUAL((dictkey_t)2, key);
        INDEXLIB_TEST_EQUAL(2, termInfos[0]->segId);
        INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, mode);
        
        termInfoQueue->MoveToNextTerm();
        termInfos.clear();
        termInfos = termInfoQueue->CurrentTermInfos(key, mode);
        INDEXLIB_TEST_EQUAL((size_t)1, termInfos.size());
        INDEXLIB_TEST_EQUAL((dictkey_t)2, key);
        INDEXLIB_TEST_EQUAL(1, termInfos[0]->segId);
        INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_BITMAP, mode);
        termInfoQueue->MoveToNextTerm();
    }

    void AddTermInfo(const vector<dictkey_t>& keys,
                     const SegmentTermInfo::TermIndexMode mode,
                     const SegmentMergeInfo& info,
                     SegmentTermInfoQueueMockPtr& termInfoQueue)
    {
        termInfoQueue->ClearNormalIndexIterator();
        termInfoQueue->ClearBitmapIndexIterator();

        if (mode == SegmentTermInfo::TM_NORMAL)
        {
            termInfoQueue->SetNormalIndexIterator(keys);
        }
        else
        {
            termInfoQueue->SetBitmapIndexIterator(keys);
        }

        termInfoQueue->AddOnDiskTermInfo(info);     
    }

    SegmentTermInfoQueueMockPtr CreateSegmentTermInfoQueueMock(
            const DictionaryConfigPtr& dictConfig)
    {
        IndexConfigPtr indexConfig(new PackageIndexConfig("test", it_pack));
        indexConfig->SetDictConfig(dictConfig);
        SegmentTermInfoQueueMockPtr termInfoQueue(
                new SegmentTermInfoQueueMock(indexConfig,
                        OnDiskIndexIteratorCreatorPtr()));
        return termInfoQueue;
    }

    void PrepareForCaseGetSortedSegmentTermInfos(
            SegmentTermInfoQueueMockPtr& termInfoQueue)
    {
        //segmentInfo : NormalTerm,      BitmapTerm     
        //segment_1   : TM_NORMAL(1),    TM_BITMAP(1, 2)
        //segment_2   : TM_NORMAL(1, 2), TM_BITMAP(1)
        //
        //expect result:
        //1 (segment_1, TM_NORMAL(1)), (segment_2, TM_NORMAL(1))
        //2 (segment_1, TM_BITMAP(1)), (segment_2, TM_BITMAP(1))
        //3 (segment_2, TM_NORMAL(2))
        //4 (segment_1, TM_BITMAP(2))

        vector<dictkey_t> keys;
        keys.push_back(1);
        
        SegmentMergeInfo info;
        info.segmentId = 1;

        AddTermInfo(keys, SegmentTermInfo::TM_NORMAL,
                    info, termInfoQueue);

        keys.clear();
        keys.push_back(1);
        keys.push_back(2);
        info.segmentId = 2;

        AddTermInfo(keys, SegmentTermInfo::TM_NORMAL,
                    info, termInfoQueue);

        keys.clear();
        keys.push_back(1);
        keys.push_back(2);
        info.segmentId = 1;

        AddTermInfo(keys, SegmentTermInfo::TM_BITMAP,
                    info, termInfoQueue);

        keys.clear();
        keys.push_back(1);
        info.segmentId = 2;

        AddTermInfo(keys, SegmentTermInfo::TM_BITMAP,
                    info, termInfoQueue);
    }


private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, SegmentTermInfoQueueTest);

INDEXLIB_UNIT_TEST_CASE(SegmentTermInfoQueueTest, TestCaseForAddQueueItem);
INDEXLIB_UNIT_TEST_CASE(SegmentTermInfoQueueTest, TestCaseForAddOnDiskTermInfo);
INDEXLIB_UNIT_TEST_CASE(SegmentTermInfoQueueTest, TestCaseForCurrentTermInfos);
INDEXLIB_UNIT_TEST_CASE(SegmentTermInfoQueueTest, TestCaseForGetSortedSegmentTermInfos);

IE_NAMESPACE_END(index);
