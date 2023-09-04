#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/test/IndexIteratorMock.h"
#include "indexlib/index/normal/inverted_index/accessor/on_disk_index_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info_queue.h"
#include "indexlib/index/normal/inverted_index/test/segment_term_info_queue_mock.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/test/unittest.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index { namespace legacy {

class SegmentTermInfoQueueTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SegmentTermInfoQueueTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForAddQueueItem()
    {
        {
            // case nomal
            SegmentTermInfoQueuePtr termInfoQueue(CreateSegmentTermInfoQueueMock(std::shared_ptr<DictionaryConfig>()));
            SegmentMergeInfo info;
            info.segmentId = 1;
            info.baseDocId = 2;

            vector<index::DictKeyInfo> keys;
            keys.push_back(index::DictKeyInfo(1));
            std::shared_ptr<IndexIteratorMock> indexIt(new IndexIteratorMock(keys));

            termInfoQueue->AddQueueItem(info, indexIt,
                                        std::shared_ptr<indexlib::index::SingleFieldIndexSegmentPatchIterator>(),
                                        SegmentTermInfo::TM_NORMAL);
            SegmentTermInfo* item = termInfoQueue->Top();
            INDEXLIB_TEST_EQUAL(1, item->GetSegmentId());
            INDEXLIB_TEST_EQUAL(2, item->GetBaseDocId());
            INDEXLIB_TEST_EQUAL((size_t)1, termInfoQueue->Size());
        }

        {
            // case queItem has no value
            SegmentTermInfoQueuePtr termInfoQueue(CreateSegmentTermInfoQueueMock(std::shared_ptr<DictionaryConfig>()));
            SegmentMergeInfo info;
            vector<index::DictKeyInfo> keys;
            std::shared_ptr<IndexIteratorMock> indexIt(new IndexIteratorMock(keys));

            termInfoQueue->AddQueueItem(info, indexIt,
                                        std::shared_ptr<indexlib::index::SingleFieldIndexSegmentPatchIterator>(),
                                        SegmentTermInfo::TM_NORMAL);
            INDEXLIB_TEST_EQUAL((size_t)0, termInfoQueue->Size());
        }

        {
            // case indexIterator null
            SegmentTermInfoQueuePtr termInfoQueue(CreateSegmentTermInfoQueueMock(std::shared_ptr<DictionaryConfig>()));
            SegmentMergeInfo info;
            std::shared_ptr<IndexIteratorMock> indexIt;

            termInfoQueue->AddQueueItem(info, indexIt,
                                        std::shared_ptr<indexlib::index::SingleFieldIndexSegmentPatchIterator>(),
                                        SegmentTermInfo::TM_NORMAL);
            INDEXLIB_TEST_EQUAL((size_t)0, termInfoQueue->Size());
        }
    }

    void TestCaseForAddOnDiskTermInfo()
    {
        {
            // case for only normal
            vector<index::DictKeyInfo> keys;
            keys.push_back(index::DictKeyInfo(1));

            SegmentTermInfoQueueMockPtr termInfoQueue(
                CreateSegmentTermInfoQueueMock(std::shared_ptr<DictionaryConfig>()));
            termInfoQueue->SetNormalIndexIterator(keys);

            SegmentMergeInfo info;
            termInfoQueue->AddOnDiskTermInfo(info);

            INDEXLIB_TEST_EQUAL((size_t)1, termInfoQueue->Size());
            SegmentTermInfo* item = termInfoQueue->Top();
            INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, item->GetTermIndexMode());
        }

        {
            // case for only bitmap
            std::shared_ptr<DictionaryConfig> dicConfig(new DictionaryConfig("test", ""));
            vector<index::DictKeyInfo> keys;
            keys.push_back(index::DictKeyInfo(1));

            SegmentTermInfoQueueMockPtr termInfoQueue(CreateSegmentTermInfoQueueMock(dicConfig));
            termInfoQueue->SetBitmapIndexIterator(keys);

            SegmentMergeInfo info;
            termInfoQueue->AddOnDiskTermInfo(info);

            INDEXLIB_TEST_EQUAL((size_t)1, termInfoQueue->Size());
            SegmentTermInfo* item = termInfoQueue->Top();
            INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_BITMAP, item->GetTermIndexMode());
        }
    }

    void TestCaseForCurrentTermInfos()
    {
        {
            // single value and normal mode
            std::shared_ptr<DictionaryConfig> dicConfig(new DictionaryConfig("test", ""));
            SegmentTermInfoQueueMockPtr termInfoQueue(CreateSegmentTermInfoQueueMock(dicConfig));

            vector<index::DictKeyInfo> keys;
            keys.push_back(index::DictKeyInfo(1));
            SegmentMergeInfo info;
            info.segmentId = 1;
            info.baseDocId = 2;
            AddTermInfo(keys, SegmentTermInfo::TM_NORMAL, info, termInfoQueue);

            index::DictKeyInfo key;
            SegmentTermInfo::TermIndexMode mode;
            const SegmentTermInfos& termInfos = termInfoQueue->CurrentTermInfos(key, mode);
            INDEXLIB_TEST_EQUAL((size_t)1, termInfos.size());
            INDEXLIB_TEST_EQUAL(index::DictKeyInfo(1), key);
            INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, mode);
            termInfoQueue->MoveToNextTerm();
        }

        {
            // multi value and normal mode
            std::shared_ptr<DictionaryConfig> dicConfig(new DictionaryConfig("test", ""));
            SegmentTermInfoQueueMockPtr termInfoQueue(CreateSegmentTermInfoQueueMock(dicConfig));

            vector<index::DictKeyInfo> keys;
            keys.push_back(index::DictKeyInfo(1));

            SegmentMergeInfo info;
            info.segmentId = 1;
            info.baseDocId = 2;

            AddTermInfo(keys, SegmentTermInfo::TM_NORMAL, info, termInfoQueue);

            keys.push_back(index::DictKeyInfo(2));
            info.segmentId = 2;
            info.baseDocId = 3;
            AddTermInfo(keys, SegmentTermInfo::TM_NORMAL, info, termInfoQueue);

            index::DictKeyInfo key;
            SegmentTermInfo::TermIndexMode mode;
            const SegmentTermInfos& termInfos = termInfoQueue->CurrentTermInfos(key, mode);
            INDEXLIB_TEST_EQUAL((size_t)2, termInfos.size());
            INDEXLIB_TEST_EQUAL(index::DictKeyInfo(1), key);
            INDEXLIB_TEST_EQUAL(1, termInfos[0]->GetSegmentId());
            INDEXLIB_TEST_EQUAL(2, termInfos[1]->GetSegmentId());
            INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, mode);
            termInfoQueue->MoveToNextTerm();
        }

        {
            // multi value and bitmap and normal
            std::shared_ptr<DictionaryConfig> dicConfig(new DictionaryConfig("test", ""));
            SegmentTermInfoQueueMockPtr termInfoQueue(CreateSegmentTermInfoQueueMock(dicConfig));

            vector<index::DictKeyInfo> keys;
            keys.push_back(index::DictKeyInfo(1));

            SegmentMergeInfo info;
            info.segmentId = 1;
            info.baseDocId = 2;
            AddTermInfo(keys, SegmentTermInfo::TM_BITMAP, info, termInfoQueue);

            info.segmentId = 2;
            info.baseDocId = 3;
            AddTermInfo(keys, SegmentTermInfo::TM_NORMAL, info, termInfoQueue);

            index::DictKeyInfo key;
            SegmentTermInfo::TermIndexMode mode;
            const SegmentTermInfos& termInfos = termInfoQueue->CurrentTermInfos(key, mode);
            INDEXLIB_TEST_EQUAL((size_t)1, termInfos.size());
            INDEXLIB_TEST_EQUAL(index::DictKeyInfo(1), key);
            INDEXLIB_TEST_EQUAL(2, termInfos[0]->GetSegmentId());
            INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, mode);
            termInfoQueue->MoveToNextTerm();
        }
    }

    void TestCaseForGetSortedSegmentTermInfos()
    {
        std::shared_ptr<DictionaryConfig> dicConfig(new DictionaryConfig("test", ""));
        SegmentTermInfoQueueMockPtr termInfoQueue(CreateSegmentTermInfoQueueMock(dicConfig));
        PrepareForCaseGetSortedSegmentTermInfos(termInfoQueue);

        index::DictKeyInfo key;
        SegmentTermInfo::TermIndexMode mode;
        SegmentTermInfos termInfos = termInfoQueue->CurrentTermInfos(key, mode);
        INDEXLIB_TEST_EQUAL((size_t)2, termInfos.size());
        INDEXLIB_TEST_EQUAL(index::DictKeyInfo(1), key);
        INDEXLIB_TEST_EQUAL(1, termInfos[0]->GetSegmentId());
        INDEXLIB_TEST_EQUAL(2, termInfos[1]->GetSegmentId());
        INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, mode);

        termInfoQueue->MoveToNextTerm();
        termInfos.clear();
        termInfos = termInfoQueue->CurrentTermInfos(key, mode);
        INDEXLIB_TEST_EQUAL((size_t)2, termInfos.size());
        INDEXLIB_TEST_EQUAL(index::DictKeyInfo(1), key);
        INDEXLIB_TEST_EQUAL(1, termInfos[0]->GetSegmentId());
        INDEXLIB_TEST_EQUAL(2, termInfos[1]->GetSegmentId());
        INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_BITMAP, mode);

        termInfoQueue->MoveToNextTerm();
        termInfos.clear();
        termInfos = termInfoQueue->CurrentTermInfos(key, mode);
        INDEXLIB_TEST_EQUAL((size_t)1, termInfos.size());
        INDEXLIB_TEST_EQUAL(index::DictKeyInfo(2), key);
        INDEXLIB_TEST_EQUAL(2, termInfos[0]->GetSegmentId());
        INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, mode);

        termInfoQueue->MoveToNextTerm();
        termInfos.clear();
        termInfos = termInfoQueue->CurrentTermInfos(key, mode);
        INDEXLIB_TEST_EQUAL((size_t)1, termInfos.size());
        INDEXLIB_TEST_EQUAL(index::DictKeyInfo(2), key);
        INDEXLIB_TEST_EQUAL(1, termInfos[0]->GetSegmentId());
        INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_BITMAP, mode);

        termInfoQueue->MoveToNextTerm();
        termInfos.clear();
        termInfos = termInfoQueue->CurrentTermInfos(key, mode);
        INDEXLIB_TEST_EQUAL((size_t)1, termInfos.size());
        INDEXLIB_TEST_EQUAL(index::DictKeyInfo::NULL_TERM, key);
        INDEXLIB_TEST_EQUAL(2, termInfos[0]->GetSegmentId());
        INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_NORMAL, mode);

        termInfoQueue->MoveToNextTerm();
        termInfos.clear();
        termInfos = termInfoQueue->CurrentTermInfos(key, mode);
        INDEXLIB_TEST_EQUAL((size_t)2, termInfos.size());
        INDEXLIB_TEST_EQUAL(index::DictKeyInfo::NULL_TERM, key);
        INDEXLIB_TEST_EQUAL(1, termInfos[0]->GetSegmentId());
        INDEXLIB_TEST_EQUAL(2, termInfos[1]->GetSegmentId());
        INDEXLIB_TEST_EQUAL(SegmentTermInfo::TM_BITMAP, mode);
        termInfoQueue->MoveToNextTerm();
    }

    void AddTermInfo(const vector<index::DictKeyInfo>& keys, const SegmentTermInfo::TermIndexMode mode,
                     const SegmentMergeInfo& info, SegmentTermInfoQueueMockPtr& termInfoQueue)
    {
        termInfoQueue->ClearNormalIndexIterator();
        termInfoQueue->ClearBitmapIndexIterator();

        if (mode == SegmentTermInfo::TM_NORMAL) {
            termInfoQueue->SetNormalIndexIterator(keys);
        } else {
            termInfoQueue->SetBitmapIndexIterator(keys);
        }

        termInfoQueue->AddOnDiskTermInfo(info);
    }

    SegmentTermInfoQueueMockPtr CreateSegmentTermInfoQueueMock(const std::shared_ptr<DictionaryConfig>& dictConfig)
    {
        IndexConfigPtr indexConfig(new PackageIndexConfig("test", it_pack));
        indexConfig->SetDictConfig(dictConfig);
        SegmentTermInfoQueueMockPtr termInfoQueue(
            new SegmentTermInfoQueueMock(indexConfig, OnDiskIndexIteratorCreatorPtr()));
        return termInfoQueue;
    }

    void PrepareForCaseGetSortedSegmentTermInfos(SegmentTermInfoQueueMockPtr& termInfoQueue)
    {
        // segmentInfo : NormalTerm,            BitmapTerm
        // segment_1   : TM_NORMAL(1),          TM_BITMAP(1, 2, NULL)
        // segment_2   : TM_NORMAL(1, 2, NULL), TM_BITMAP(1, NULL)
        //
        // expect result:
        // 1 (segment_1, TM_NORMAL(1)), (segment_2, TM_NORMAL(1))
        // 2 (segment_1, TM_BITMAP(1)), (segment_2, TM_BITMAP(1))
        // 3 (segment_2, TM_NORMAL(2))
        // 4 (segment_1, TM_BITMAP(2))
        // 5 (segment_2, TM_NORMAL(NULL))
        // 6 (segment_1, TM_BITMAP(NULL)), (segment_2, TM_BITMAP(NULL))

        vector<index::DictKeyInfo> keys;
        keys.push_back(index::DictKeyInfo(1));
        SegmentMergeInfo info;
        info.segmentId = 1;
        AddTermInfo(keys, SegmentTermInfo::TM_NORMAL, info, termInfoQueue);

        keys.clear();
        keys.push_back(index::DictKeyInfo(1));
        keys.push_back(index::DictKeyInfo(2));
        keys.push_back(index::DictKeyInfo::NULL_TERM);
        info.segmentId = 2;
        AddTermInfo(keys, SegmentTermInfo::TM_NORMAL, info, termInfoQueue);

        keys.clear();
        keys.push_back(index::DictKeyInfo(1));
        keys.push_back(index::DictKeyInfo(2));
        keys.push_back(index::DictKeyInfo::NULL_TERM);
        info.segmentId = 1;
        AddTermInfo(keys, SegmentTermInfo::TM_BITMAP, info, termInfoQueue);

        keys.clear();
        keys.push_back(index::DictKeyInfo(1));
        keys.push_back(index::DictKeyInfo::NULL_TERM);
        info.segmentId = 2;
        AddTermInfo(keys, SegmentTermInfo::TM_BITMAP, info, termInfoQueue);
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, SegmentTermInfoQueueTest);

INDEXLIB_UNIT_TEST_CASE(SegmentTermInfoQueueTest, TestCaseForAddQueueItem);
INDEXLIB_UNIT_TEST_CASE(SegmentTermInfoQueueTest, TestCaseForAddOnDiskTermInfo);
INDEXLIB_UNIT_TEST_CASE(SegmentTermInfoQueueTest, TestCaseForCurrentTermInfos);
INDEXLIB_UNIT_TEST_CASE(SegmentTermInfoQueueTest, TestCaseForGetSortedSegmentTermInfos);
}}} // namespace indexlib::index::legacy
