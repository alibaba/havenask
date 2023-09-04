#include "indexlib/index/merger_util/reclaim_map/test/reclaim_map_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/merger_util/reclaim_map/reclaim_map_creator.h"
#include "indexlib/index/merger_util/reclaim_map/sorted_reclaim_map_creator.h"
#include "indexlib/index/merger_util/reclaim_map/sub_reclaim_map_creator.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/mock_segment_directory.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::merger;

namespace indexlib { namespace index {

void ReclaimMapTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();

    mMainJoinAttrConfig.reset(new config::AttributeConfig);
    mMainJoinAttrConfig->SetAttrId(2);
    config::FieldConfigPtr mainJoinFieldConfig(
        new config::FieldConfig(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME, ft_int32, false));
    mMainJoinAttrConfig->Init(mainJoinFieldConfig);

    mSubDocCounts.clear();
    mMainJoinAttrValue.clear();
    mSubDocCount = 0;

    mSchema.reset(new IndexPartitionSchema());
    mSchema->AddFieldConfig("attr_name", ft_integer, false);
    mSchema->AddAttributeConfig("attr_name");
    mSchema->AddFieldConfig("attr_name1", ft_integer, false);
    mSchema->AddAttributeConfig("attr_name1");

    mSortDescs.clear();
    mSortDescs.push_back({"attr_name", indexlibv2::config::sp_desc});
    mSortDescs.push_back({"attr_name1", indexlibv2::config::sp_asc});

    mDocCountStr = "2,3,5,3";
    string highAttrValueStr = "31,3,19,11,8,21,19,16,2,1,13,12,11";
    string lowAttrValueStr = "31,3,19,11,8,21,19,16,2,1,13,12,11";
    string subDocCountsStr = "2,0,1,1,0,2,0,1,3,2,0,2,1";
    StringUtil::fromString(highAttrValueStr, mHighAttrValue, ",");
    StringUtil::fromString(lowAttrValueStr, mLowAttrValue, ",");
    StringUtil::fromString(mDocCountStr, mDocCounts, ",");

    std::vector<uint32_t> subDocCounts;
    StringUtil::fromString(subDocCountsStr, subDocCounts, ",");

    for (size_t i = 0; i < mDocCounts.size(); ++i) {
        docid_t subDocCount = 0;
        for (uint32_t j = 0; j < mDocCounts[i]; ++j) {
            subDocCount += subDocCounts[mMainJoinAttrValue.size()];
            mMainJoinAttrValue.push_back(subDocCount);
        }
        mSubDocCount += subDocCount;
        mSubDocCounts.push_back(subDocCount);
    }
}

void ReclaimMapTest::CaseTearDown() {}

void ReclaimMapTest::TestCaseForMergePart()
{
    TestMergePartWithoutDelete(false);
    TestMergePartWithoutDelete(true);
    TestMergePartWithDelete(false);
    TestMergePartWithDelete(true);

    TestMergePartWithoutDelete(false, true);
    TestMergePartWithoutDelete(true, true);
    TestMergePartWithDelete(false, true);
    TestMergePartWithDelete(true, true);
}

void ReclaimMapTest::TestCaseForSortMergePart()
{
    TestSortMergePartWithoutDelete(false);
    TestSortMergePartWithoutDelete(true);
    TestSortMergePartWithDelete(false);
    TestSortMergePartWithDelete(true);

    TestSortMergePartWithoutDelete(false, true);
    TestSortMergePartWithoutDelete(true, true);
    TestSortMergePartWithDelete(false, true);
    TestSortMergePartWithDelete(true, true);
}

void ReclaimMapTest::TestCaseForMergeAll()
{
    TestMergeAllWithoutDelete(false);
    TestMergeAllWithoutDelete(true);
    TestMergeAllWithDelete(false);
    TestMergeAllWithDelete(true);

    TestMergeAllWithoutDelete(false, true);
    TestMergeAllWithoutDelete(true, true);
    TestMergeAllWithDelete(false, true);
    TestMergeAllWithDelete(true, true);
}

void ReclaimMapTest::TestCaseForSortMergeAll()
{
    TestSortMergeAllWithoutDelete(false);
    TestSortMergeAllWithoutDelete(true);
    TestSortMergeAllWithDelete(false);
    TestSortMergeAllWithDelete(true);

    TestSortMergeAllWithoutDelete(false, true);
    TestSortMergeAllWithoutDelete(true, true);
    TestSortMergeAllWithDelete(false, true);
    TestSortMergeAllWithDelete(true, true);
}

void ReclaimMapTest::TestCaseForMergeSegmentApart()
{
    TestMergeSegmentApartWithoutDelete();
    TestMergeSegmentApartWithDelete();

    TestMergeSegmentApartWithoutDelete(true);
    TestMergeSegmentApartWithDelete(true);
}

void ReclaimMapTest::TestCaseForStoreAndLoad()
{
    {
        // with empty joinAttrValues
        string docCountsStr = "2,3,5,3";
        string segIdStr = "0,1,2,3";
        string seg2MergeStr = "1,2";
        Prepare(docCountsStr, segIdStr, seg2MergeStr, IndexTestUtil::DeleteEven);
        SegmentMergeInfos segMergeInfos;
        CreateSegmentMergeInfos(mDocCounts, mSegIds, mDelMapReader, mMergePlan, segMergeInfos);
        ReclaimMapCreator creator(true);
        ReclaimMapPtr reclaimMap = creator.Create(segMergeInfos, mDelMapReader, nullptr);
        reclaimMap->StoreForMerge(mRootDir + "reclaim_map", segMergeInfos);
        INDEXLIB_TEST_EQUAL(size_t(2), segMergeInfos.size());
        ReclaimMap reclaimMap2;
        reclaimMap2.LoadForMerge(mRootDir + "reclaim_map", segMergeInfos, 2);
        INDEXLIB_TEST_EQUAL(reclaimMap->GetDeletedDocCount(), reclaimMap2.GetDeletedDocCount());
        INDEXLIB_TEST_EQUAL(reclaimMap->GetNewDocCount(), reclaimMap2.GetNewDocCount());
        INDEXLIB_TEST_TRUE(reclaimMap->GetSliceArray() == reclaimMap2.GetSliceArray());
        for (uint32_t i = 0; i < reclaimMap2.GetNewDocCount(); i++) {
            segmentid_t expectedSegId, actualSegId;
            docid_t expectedDocId, actualDocId;

            docid_t docId = (docid_t)i;
            expectedDocId = reclaimMap->GetOldDocIdAndSegId(docId, expectedSegId);
            actualDocId = reclaimMap2.GetOldDocIdAndSegId(docId, actualSegId);

            INDEXLIB_TEST_EQUAL(expectedSegId, actualSegId);
            INDEXLIB_TEST_EQUAL(expectedDocId, actualDocId);
        }
    }

    {
        // with joinAttrValues
        string docCountsStr = "2,3,5,3";
        string segIdStr = "0,1,2,3";
        string seg2MergeStr = "1,2";
        Prepare(docCountsStr, segIdStr, seg2MergeStr, IndexTestUtil::DeleteEven);
        SegmentMergeInfos segMergeInfos;
        CreateSegmentMergeInfos(mDocCounts, mSegIds, mDelMapReader, mMergePlan, segMergeInfos);
        ReclaimMapCreator creator(true);
        ReclaimMapPtr reclaimMap = creator.Create(segMergeInfos, mDelMapReader, nullptr);

        vector<docid_t> joinValueArrays;
        for (size_t i = 0; i < reclaimMap->GetNewDocCount(); ++i) {
            joinValueArrays.push_back((docid_t)i);
        }
        reclaimMap->InitJoinValueArray(joinValueArrays);

        reclaimMap->StoreForMerge(mRootDir + "reclaim_map", segMergeInfos);
        ReclaimMap reclaimMap2;
        reclaimMap2.LoadForMerge(mRootDir + "reclaim_map", segMergeInfos, 2);
        INDEXLIB_TEST_EQUAL(reclaimMap->GetDeletedDocCount(), reclaimMap2.GetDeletedDocCount());
        INDEXLIB_TEST_EQUAL(reclaimMap->GetNewDocCount(), reclaimMap2.GetNewDocCount());
        INDEXLIB_TEST_TRUE(reclaimMap->GetSliceArray() == reclaimMap2.GetSliceArray());
        for (size_t i = 0; i < reclaimMap2.GetNewDocCount(); ++i) {
            INDEXLIB_TEST_EQUAL((docid_t)i, reclaimMap2.GetJoinValueArray()[i]);
        }
    }

    {
        // with multi target segs
        string docCountsStr = "2,3,5,3";
        string segIdStr = "0,1,2,3";
        string seg2MergeStr = "1,2";
        Prepare(docCountsStr, segIdStr, seg2MergeStr, IndexTestUtil::DeleteEven);
        SegmentMergeInfos segMergeInfos;
        CreateSegmentMergeInfos(mDocCounts, mSegIds, mDelMapReader, mMergePlan, segMergeInfos);
        ReclaimMapCreator creator(true);
        ReclaimMapPtr reclaimMap = creator.Create(segMergeInfos, mDelMapReader, nullptr);
        reclaimMap->TEST_SetTargetBaseDocIds({0, 3});
        reclaimMap->StoreForMerge(mRootDir + "reclaim_map", segMergeInfos);
        INDEXLIB_TEST_EQUAL(size_t(2), segMergeInfos.size());
        ReclaimMap reclaimMap2;
        reclaimMap2.LoadForMerge(mRootDir + "reclaim_map", segMergeInfos, 2);
        INDEXLIB_TEST_EQUAL(reclaimMap->GetDeletedDocCount(), reclaimMap2.GetDeletedDocCount());
        INDEXLIB_TEST_EQUAL(reclaimMap->GetNewDocCount(), reclaimMap2.GetNewDocCount());
        INDEXLIB_TEST_TRUE(reclaimMap->GetSliceArray() == reclaimMap2.GetSliceArray());
        ASSERT_EQ(reclaimMap->TEST_GetTargetBaseDocIds(), reclaimMap2.TEST_GetTargetBaseDocIds());
        for (uint32_t i = 0; i < reclaimMap2.GetNewDocCount(); i++) {
            segmentid_t expectedSegId, actualSegId;
            docid_t expectedDocId, actualDocId;

            docid_t docId = (docid_t)i;
            expectedDocId = reclaimMap->GetOldDocIdAndSegId(docId, expectedSegId);
            actualDocId = reclaimMap2.GetOldDocIdAndSegId(docId, actualSegId);

            INDEXLIB_TEST_EQUAL(expectedSegId, actualSegId);
            INDEXLIB_TEST_EQUAL(expectedDocId, actualDocId);
        }
    }
}

void ReclaimMapTest::TestMergePartWithoutDelete(bool needReverseMapping, bool splitSegment)
{
    string expectedDocIdStr = "-2,-2,0,1,2,3,4,5,6,7,-2,-2,-2";
    string expectedOldDocIdStr = "";
    string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "0,1,2,0,1,2,3,4";
        expectedSegIdStr = "1,1,1,2,2,2,2,2";
    }
    if (splitSegment) {
        // 2,3,5,3
        expectedDocIdStr = "-2,-2,0,4,1,5,2,6,3,7,-2,-2,-2";
        if (needReverseMapping) {
            expectedOldDocIdStr = "0,2,1,3,1,0,2,4";
            expectedSegIdStr = "1,1,2,2,1,2,2,2";
        }
    }
    TestMergePart(IndexTestUtil::NoDelete, false, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                  expectedSegIdStr, splitSegment);
}

void ReclaimMapTest::TestMergePartWithDelete(bool needReverseMapping, bool splitSegment)
{
    string expectedDocIdStr = "-2,-2,-1,0,-1,1,-1,2,-1,3,-2,-2,-2";
    string expectedOldDocIdStr = "";
    string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "1,0,2,4";
        expectedSegIdStr = "1,2,2,2";
    }
    if (splitSegment) {
        // 2,3,5,3
        expectedDocIdStr = "-2,-2,-1,0,-1,2,-1,1,-1,3,-2,-2,-2";
        if (needReverseMapping) {
            expectedOldDocIdStr = "1,2,0,4";
            expectedSegIdStr = "1,2,2,2";
        }
    }

    TestMergePart(IndexTestUtil::DeleteEven, false, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                  expectedSegIdStr, splitSegment);
}

void ReclaimMapTest::TestSortMergePartWithoutDelete(bool needReverseMapping, bool splitSegment)
{
    string expectedDocIdStr = "-2,-2,1,4,5,0,2,3,6,7,-2,-2,-2";
    string expectedOldDocIdStr = "";
    string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "0,0,1,2,1,2,3,4";
        expectedSegIdStr = "2,1,2,2,1,1,2,2";
    }
    if (splitSegment) {
        // 2,3,5,3
        expectedDocIdStr = "-2,-2,4,2,6,0,1,5,3,7,-2,-2,-2";
        if (needReverseMapping) {
            expectedOldDocIdStr = "0,1,1,3,0,2,2,4";
            expectedSegIdStr = "2,2,1,2,1,2,1,2";
        }
    }

    TestMergePart(IndexTestUtil::NoDelete, true, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                  expectedSegIdStr, splitSegment);
}

void ReclaimMapTest::TestSortMergePartWithDelete(bool needReverseMapping, bool splitSegment)
{
    string expectedDocIdStr = "-2,-2,-1,2,-1,0,-1,1,-1,3,-2,-2,-2";
    string expectedOldDocIdStr = "";
    string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "0,2,1,4";
        expectedSegIdStr = "2,2,1,2";
    }
    if (splitSegment) {
        // 2,3,5,3
        expectedDocIdStr = "-2,-2,-1,1,-1,0,-1,2,-1,3,-2,-2,-2";
        if (needReverseMapping) {
            expectedOldDocIdStr = "0,1,2,4";
            expectedSegIdStr = "2,1,2,2";
        }
    }
    TestMergePart(IndexTestUtil::DeleteEven, true, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                  expectedSegIdStr, splitSegment);
}

void ReclaimMapTest::TestCaseForSubReclaimMapInit()
{
    {
        // Test no sort, no delete
        string expectedDocIdStr = "-2,-2,0,1,2,3,4,5,6,7,8,9,-2,-2,-2";
        string expectedReverseDocIdStr = "0,1,0,1,2,3,4,5,6,7";
        string expectedSegIdStr = "1,1,2,2,2,2,2,2,2,2";
        string expectedMainJoinStr = "1,2,2,4,4,5,8,10";
        string expectedSubJoinStr = "0,1,3,3,5,6,6,6,7,7";

        InnerSubReclaimMapInit(IndexTestUtil::NoDelete, IndexTestUtil::NoDelete, false, true, expectedDocIdStr,
                               expectedReverseDocIdStr, expectedSegIdStr, expectedMainJoinStr, expectedSubJoinStr);
    }
    {
        // Test no sort, sub doc delete even
        string expectedDocIdStr = "-2,-2,-1,0,-1,1,-1,2,-1,3,-1,4,-2,-2,-2";
        string expectedReverseDocIdStr = "1,1,3,5,7";
        string expectedSegIdStr = "1,2,2,2,2";
        string expectedMainJoinStr = "0,1,1,2,2,2,4,5";
        string expectedSubJoinStr = "1,3,6,6,7";
        InnerSubReclaimMapInit(IndexTestUtil::NoDelete, IndexTestUtil::DeleteEven, false, true, expectedDocIdStr,
                               expectedReverseDocIdStr, expectedSegIdStr, expectedMainJoinStr, expectedSubJoinStr);
    }
    {
        // Test no sort, main doc delete even
        string expectedDocIdStr = "-2,-2,-1,0,1,2,3,-1,-1,-1,4,5,-2,-2,-2";
        string expectedReverseDocIdStr = "1,0,1,2,6,7";
        string expectedSegIdStr = "1,2,2,2,2,2";
        string expectedMainJoinStr = "1,3,4,6";
        string expectedSubJoinStr = "0,1,1,2,3,3";
        InnerSubReclaimMapInit(IndexTestUtil::DeleteEven, IndexTestUtil::NoDelete, false, true, expectedDocIdStr,
                               expectedReverseDocIdStr, expectedSegIdStr, expectedMainJoinStr, expectedSubJoinStr);
    }
    {
        // Test no sort, main and sub doc delete even
        string expectedDocIdStr = "-2,-2,-1,0,-1,1,-1,-1,-1,-1,-1,2,-2,-2,-2";
        string expectedReverseDocIdStr = "1,1,7";
        string expectedSegIdStr = "1,2,2";
        string expectedMainJoinStr = "1,2,2,3";
        string expectedSubJoinStr = "0,1,3";
        InnerSubReclaimMapInit(IndexTestUtil::DeleteEven, IndexTestUtil::DeleteEven, false, true, expectedDocIdStr,
                               expectedReverseDocIdStr, expectedSegIdStr, expectedMainJoinStr, expectedSubJoinStr);
    }
    {
        // Test sort, main and sub doc delete even
        string expectedDocIdStr = "-2,-2,-1,1,-1,0,-1,-1,-1,-1,-1,2,-2,-2,-2";
        string expectedReverseDocIdStr = "1,1,7";
        string expectedSegIdStr = "2,1,2";
        string expectedMainJoinStr = "1,1,2,3";
        string expectedSubJoinStr = "0,2,3";
        InnerSubReclaimMapInit(IndexTestUtil::DeleteEven, IndexTestUtil::DeleteEven, true, true, expectedDocIdStr,
                               expectedReverseDocIdStr, expectedSegIdStr, expectedMainJoinStr, expectedSubJoinStr);
    }
}

void ReclaimMapTest::InnerSubReclaimMapInit(IndexTestUtil::ToDelete mainToDel, IndexTestUtil::ToDelete subToDel,
                                            bool sortByWeight, bool needReverseMapping, const string& expectedDocIdStr,
                                            const string& expectedOldDocIdStr, const string& expectedSegIdStr,
                                            const string& expectedMainJoinValueStr,
                                            const string& expectedSubJoinValueStr)
{
    string docCountsStr = mDocCountStr;
    string segIdStr = "0,1,2,3";
    string seg2MergeStr = "1,2";
    Prepare(docCountsStr, segIdStr, seg2MergeStr, mainToDel, subToDel);

    SegmentMergeInfos segMergeInfos;
    ReclaimMapPtr mainReclaimMap;
    if (sortByWeight) {
        CreateSegmentMergeInfosForSort(mDocCounts, mSegIds, mDelMapReader, mMergePlan, segMergeInfos, mHighAttrValue,
                                       mLowAttrValue);
        OfflineAttributeSegmentReaderContainerPtr readerContainer;
        SortedReclaimMapCreator creator(needReverseMapping, readerContainer, mSchema, mSortDescs);
        mainReclaimMap = creator.Create(segMergeInfos, mDelMapReader, nullptr);
    } else {
        CreateSegmentMergeInfos(mDocCounts, mSegIds, mDelMapReader, mMergePlan, segMergeInfos);
        ReclaimMapCreator creator(needReverseMapping);
        mainReclaimMap = creator.Create(segMergeInfos, mDelMapReader, nullptr);
    }

    SegmentMergeInfos subSegMergeInfos;
    CreateSegmentMergeInfos(mSubDocCounts, mSegIds, mSubDelMapReader, mMergePlan, subSegMergeInfos, true);

    SubReclaimMapCreator creator(needReverseMapping, mMainJoinAttrConfig);
    ReclaimMapPtr subReclaimMap =
        creator.CreateSubReclaimMap(mainReclaimMap.get(), segMergeInfos, subSegMergeInfos, mSegDir, mSubDelMapReader);

    CheckAnswer(*subReclaimMap, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr, expectedSegIdStr);

    CheckJoinValue(*mainReclaimMap, expectedMainJoinValueStr);
    CheckJoinValue(*subReclaimMap, expectedSubJoinValueStr);
}

void ReclaimMapTest::TestMergePart(IndexTestUtil::ToDelete toDel, bool sortByWeight, const string& expectedDocIdStr,
                                   bool needReverseMapping, const string& expectedOldDocIdStr,
                                   const string& expectedSegIdStr, bool splitSegment)
{
    string docCountsStr = mDocCountStr;
    string segIdStr = "0,1,2,3";
    string seg2MergeStr = "1,2";
    Prepare(docCountsStr, segIdStr, seg2MergeStr, toDel);
    InnerTestMerge(docCountsStr, segIdStr, seg2MergeStr, sortByWeight, needReverseMapping, toDel, expectedDocIdStr,
                   expectedOldDocIdStr, expectedSegIdStr, splitSegment);
}

void ReclaimMapTest::TestMergeAllWithoutDelete(bool needReverseMapping, bool multiTargetSegment)
{
    string expectedDocIdStr = "0,1,2,3,4,5,6,7,8,9,10,11,12";
    string expectedOldDocIdStr = "";
    string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "0,1,0,1,2,0,1,2,3,4,0,1,2";
        expectedSegIdStr = "0,0,1,1,1,2,2,2,2,2,3,3,3";
    }

    if (multiTargetSegment) {
        expectedDocIdStr = "0,7,1,8,2,9,3,10,4,11,5,12,6";
        if (needReverseMapping) {
            // 2,3,5,3
            expectedOldDocIdStr = "0,0,2,1,3,0,2,1,1,0,2,4,1";
            expectedSegIdStr = "0,1,1,2,2,3,3,0,1,2,2,2,3";
        }
    }
    TestMergeAll(IndexTestUtil::NoDelete, false, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                 expectedSegIdStr, multiTargetSegment);
}

void ReclaimMapTest::TestMergeAllWithDelete(bool needReverseMapping, bool multiTargetSegment)
{
    string expectedDocIdStr = "-1,0,-1,1,-1,2,-1,3,-1,4,-1,5,-1";
    string expectedOldDocIdStr = "";
    string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "1,1,0,2,4,1";
        expectedSegIdStr = "0,1,2,2,2,3";
    }

    if (multiTargetSegment) {
        // 2,3,5,3
        expectedDocIdStr = "-1,0,-1,3,-1,1,-1,4,-1,2,-1,5,-1";
        if (needReverseMapping) {
            expectedOldDocIdStr = "1,0,4,1,2,1";
            expectedSegIdStr = "0,2,2,1,2,3";
        }
    }
    TestMergeAll(IndexTestUtil::DeleteEven, false, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                 expectedSegIdStr, multiTargetSegment);
}

void ReclaimMapTest::TestSortMergeAllWithoutDelete(bool needReverseMapping, bool splitSegment)
{
    string expectedDocIdStr = "0,10,2,7,9,1,3,4,11,12,5,6,8";
    string expectedOldDocIdStr = "";
    string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "0,0,0,1,2,0,1,1,2,2,1,3,4";
        expectedSegIdStr = "0,2,1,2,2,3,3,1,3,1,0,2,2";
    }
    if (splitSegment) {
        // 2,3,5,3
        expectedDocIdStr = "0,5,1,10,11,7,8,2,12,6,9,3,4";
        if (needReverseMapping) {
            expectedOldDocIdStr = "0,0,2,1,2,1,4,0,1,0,1,2,3";
            expectedSegIdStr = "0,1,2,3,3,0,2,2,2,3,1,1,2";
        }
    }

    TestMergeAll(IndexTestUtil::NoDelete, true, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                 expectedSegIdStr, splitSegment);
}

void ReclaimMapTest::TestSortMergeAllWithDelete(bool needReverseMapping, bool splitSegment)
{
    string expectedDocIdStr = "-1,4,-1,3,-1,0,-1,1,-1,5,-1,2,-1";
    string expectedOldDocIdStr = "";
    string expectedSegIdStr = "";
    if (needReverseMapping) {
        expectedOldDocIdStr = "0,2,1,1,1,4";
        expectedSegIdStr = "2,2,3,1,0,2";
    }
    if (splitSegment) {
        // 2,3,5,3
        expectedDocIdStr = "-1,2,-1,4,-1,0,-1,3,-1,5,-1,1,-1";
        if (needReverseMapping) {
            expectedOldDocIdStr = "0,1,1,2,1,4";
            expectedSegIdStr = "2,3,0,2,1,2";
        }
    }

    TestMergeAll(IndexTestUtil::DeleteEven, true, expectedDocIdStr, needReverseMapping, expectedOldDocIdStr,
                 expectedSegIdStr, splitSegment);
}

void ReclaimMapTest::TestMergeAll(IndexTestUtil::ToDelete toDel, bool sortByWeight, const string& expectedDocIdStr,
                                  bool needReverseMapping, const string& expectedOldDocIdStr,
                                  const string& expectedSegIdStr, bool multiTargetSegment)
{
    string docCountsStr = mDocCountStr;
    string segIdStr = "0,1,2,3";
    string seg2MergeStr = "0,1,2,3";
    Prepare(docCountsStr, segIdStr, seg2MergeStr, toDel, IndexTestUtil::NoDelete, multiTargetSegment);
    InnerTestMerge(docCountsStr, segIdStr, seg2MergeStr, sortByWeight, needReverseMapping, toDel, expectedDocIdStr,
                   expectedOldDocIdStr, expectedSegIdStr, multiTargetSegment);
}

void ReclaimMapTest::TestMergeSegmentApartWithoutDelete(bool splitSegment)
{
    string expectedDocIdStr = "-2,-2,0,1,2,-2,-2,-2,-2,-2,3,4,5";
    if (splitSegment) {
        expectedDocIdStr = "-2,-2,0,3,1,-2,-2,-2,-2,-2,4,2,5";
    }
    TestMergeSegmentApart(IndexTestUtil::NoDelete, expectedDocIdStr, false, splitSegment);
}

void ReclaimMapTest::TestMergeSegmentApartWithDelete(bool splitSegment)
{
    string expectedDocIdStr = "-2,-2,-1,0,-1,-2,-2,-2,-2,-2,-1,1,-1";
    if (splitSegment) {
        expectedDocIdStr = "-2,-2,-1,0,-1,-2,-2,-2,-2,-2,-1,1,-1";
    }
    TestMergeSegmentApart(IndexTestUtil::DeleteEven, expectedDocIdStr, false, splitSegment);
}

void ReclaimMapTest::TestMergeSegmentApart(IndexTestUtil::ToDelete toDel, const string& expectedDocIdStr,
                                           bool needReverseMapping, bool splitSegment)
{
    string docCountsStr = mDocCountStr;
    string segIdStr = "1,3,4,6";
    string seg2MergeStr = "3,6";
    InnerTestMerge(docCountsStr, segIdStr, seg2MergeStr, false, needReverseMapping, toDel, expectedDocIdStr, "", "",
                   splitSegment);
}

void ReclaimMapTest::Prepare(const string& docCountsStr, const string& segIdStr, const string& seg2MergeStr,
                             IndexTestUtil::ToDelete toDel, IndexTestUtil::ToDelete subDocToDel,
                             bool multiTargetSegment)
{
    mDocCounts.clear();
    mSegIds.clear();
    vector<segmentid_t> seg2Merge;

    StringUtil::fromString(docCountsStr, mDocCounts, ",");
    StringUtil::fromString(segIdStr, mSegIds, ",");
    StringUtil::fromString(seg2MergeStr, seg2Merge, ",");

    INDEXLIB_TEST_EQUAL(mDocCounts.size(), mSegIds.size());
    INDEXLIB_TEST_TRUE(seg2Merge.size() <= mSegIds.size());

    auto partDir = GET_PARTITION_DIRECTORY();
    Version v(0);
    for (size_t i = 0; i < mSegIds.size(); i++) {
        v.AddSegment(mSegIds[i]);
        partDir->MakeDirectory(v.GetSegmentDirName(mSegIds[i]));
    }
    mSegDir.reset(new MockSegmentDirectory(partDir, v));
    mSegDir->Init(true);
    mSubSegDir = mSegDir->GetSubSegmentDirectory();
    mDelMapReader.reset();
    mDelMapReader = IndexTestUtil::CreateDeletionMap(mSegDir->GetVersion(), mDocCounts, toDel);
    mSubDelMapReader.reset();
    mSubDelMapReader = IndexTestUtil::CreateDeletionMap(mSubSegDir->GetVersion(), mSubDocCounts, subDocToDel);

    mMergePlan.Clear();

    mMergePlan.AddTargetSegment();
    if (multiTargetSegment) {
        mMergePlan.AddTargetSegment();
    }
    for (size_t i = 0; i < seg2Merge.size(); i++) {
        SegmentMergeInfo segMergeInfo;
        segMergeInfo.segmentId = seg2Merge[i];
        mMergePlan.AddSegment(segMergeInfo);
    }
}

void ReclaimMapTest::InnerTestMerge(const string& docCountsStr, // input doc counts
                                    const string& segIdStr,     // input segment ids
                                    const string& seg2MergeStr, // to merge segids
                                    bool sortByWeight, bool needReverseMapping, IndexTestUtil::ToDelete toDel,
                                    const string& expectedDocIdStr,        // reclaimed docids
                                    const string& expectedReverseDocIdStr, // old docids
                                    const string& expectedSegIdStr,        // old segids
                                    bool multiTargetSegment)

{
    Prepare(docCountsStr, segIdStr, seg2MergeStr, toDel, IndexTestUtil::NoDelete, multiTargetSegment);

    SegmentMergeInfos segMergeInfos;
    ReclaimMapPtr reclaimMap;
    std::function<segmentindex_t(segmentid_t, docid_t)> segmentSplitHandler = nullptr;
    if (multiTargetSegment) {
        size_t cnt = 0;
        segmentSplitHandler = [cnt](segmentid_t, docid_t) mutable -> segmentindex_t {
            auto ret = cnt % 2;
            ++cnt;
            return ret;
        };
    }
    if (sortByWeight) {
        CreateSegmentMergeInfosForSort(mDocCounts, mSegIds, mDelMapReader, mMergePlan, segMergeInfos, mHighAttrValue,
                                       mLowAttrValue);
        OfflineAttributeSegmentReaderContainerPtr readerContainer;
        SortedReclaimMapCreator creator(needReverseMapping, readerContainer, mSchema, mSortDescs);
        reclaimMap = creator.Create(segMergeInfos, mDelMapReader, segmentSplitHandler);
    } else {
        CreateSegmentMergeInfos(mDocCounts, mSegIds, mDelMapReader, mMergePlan, segMergeInfos);
        ReclaimMapCreator creator(needReverseMapping);
        reclaimMap = creator.Create(segMergeInfos, mDelMapReader, segmentSplitHandler);
    }
    CheckAnswer(*reclaimMap, expectedDocIdStr, needReverseMapping, expectedReverseDocIdStr, expectedSegIdStr);
}

void ReclaimMapTest::CheckJoinValue(const ReclaimMap& reclaimMap, const string& expectedJoinValueStr)
{
    const vector<docid_t>& joinValue = reclaimMap.GetJoinValueArray();

    vector<docid_t> expectedJoinValues;
    StringUtil::fromString(expectedJoinValueStr, expectedJoinValues, ",");

    INDEXLIB_TEST_EQUAL(expectedJoinValues.size(), joinValue.size());
    for (size_t i = 0; i < joinValue.size(); i++) {
        INDEXLIB_TEST_EQUAL(expectedJoinValues[i], joinValue[i]);
    }
}

void ReclaimMapTest::CheckAnswer(const ReclaimMap& reclaimMap, const string& expectedDocIdStr, bool needReverseMapping,
                                 const string& expectedOldDocIdStr, const string& expectedSegIdStr)
{
    vector<docid_t> expectedNewDocIds;
    StringUtil::fromString(expectedDocIdStr, expectedNewDocIds, ",");
    // check new docids
    docid_t oldDocId = 0;
    for (size_t i = 0; i < expectedNewDocIds.size(); i++) {
        if (expectedNewDocIds[i] == -2) {
            continue;
        }
        docid_t newDocId = reclaimMap.GetNewId(oldDocId);
        INDEXLIB_TEST_EQUAL(expectedNewDocIds[i], newDocId);
        oldDocId++;
    }
    // check old docids and segment ids
    if (needReverseMapping) {
        vector<docid_t> expectedOldDocIds;
        vector<segmentid_t> expectedSegIds;
        StringUtil::fromString(expectedOldDocIdStr, expectedOldDocIds, ",");
        StringUtil::fromString(expectedSegIdStr, expectedSegIds, ",");
        INDEXLIB_TEST_EQUAL(expectedSegIds.size(), expectedOldDocIds.size());
        INDEXLIB_TEST_EQUAL(expectedSegIds.size(), reclaimMap.GetNewDocCount());
        for (uint32_t i = 0; i < reclaimMap.GetNewDocCount(); i++) {
            segmentid_t segId = INVALID_SEGMENTID;
            docid_t docId = reclaimMap.GetOldDocIdAndSegId((docid_t)i, segId);
            INDEXLIB_TEST_EQUAL(expectedOldDocIds[i], docId);
            INDEXLIB_TEST_EQUAL(expectedSegIds[i], segId);
        }
    }
}

void ReclaimMapTest::CreateSegmentMergeInfos(const vector<uint32_t>& docCounts, const vector<segmentid_t>& segmentIds,
                                             const DeletionMapReaderPtr& deletionMapReader, const MergePlan& plan,
                                             SegmentMergeInfos& segMergeInfos, bool isSubDoc)
{
    docid_t oldBaseDocId = 0;
    docid_t newBaseDocId = 0;
    for (size_t i = 0; i < docCounts.size(); ++i) {
        if (!plan.HasSegment(segmentIds[i])) {
            oldBaseDocId += docCounts[i];
            continue;
        }

        SegmentInfo segInfo;
        segInfo.docCount = docCounts[i];
        uint32_t deletedDocCount = deletionMapReader->GetDeletedDocCount(segmentIds[i]);
        SegmentMergeInfo segMergeInfo(segmentIds[i], segInfo, deletedDocCount, newBaseDocId);

        if (!isSubDoc) {
            vector<docid_t> joinAttrs(mMainJoinAttrValue.begin() + oldBaseDocId,
                                      mMainJoinAttrValue.begin() + oldBaseDocId + docCounts[i]);

            AttributeSegmentReaderPtr joinAttrSegReader(new FakeAttributeSegmentReaderRM(joinAttrs));

            std::vector<AttributeSegmentReaderPtr> attrSegReaders;
            attrSegReaders.push_back(joinAttrSegReader);

            InMemorySegmentReaderPtr inMemSegReader(new FakeInMemorySegmentReader(attrSegReaders));

            // TODO: use in memory segment reader
            segMergeInfo.TEST_inMemorySegmentReader = inMemSegReader;
        }
        segMergeInfos.push_back(segMergeInfo);
        oldBaseDocId += docCounts[i];
        newBaseDocId += docCounts[i];
    }
}

void ReclaimMapTest::CreateSegmentMergeInfosForSort(const vector<uint32_t>& docCounts,
                                                    const vector<segmentid_t>& segmentIds,
                                                    const DeletionMapReaderPtr& deletionMapReader,
                                                    const MergePlan& plan, SegmentMergeInfos& segMergeInfos,
                                                    const vector<int32_t>& highAttrValues,
                                                    const vector<int32_t>& lowAttrValues)
{
    docid_t oldBaseDocId = 0;
    docid_t newBaseDocId = 0;
    for (size_t i = 0; i < docCounts.size(); ++i) {
        if (!plan.HasSegment(segmentIds[i])) {
            oldBaseDocId += docCounts[i];
            continue;
        }

        SegmentInfo segInfo;
        segInfo.docCount = docCounts[i];
        uint32_t deletedDocCount = deletionMapReader->GetDeletedDocCount(segmentIds[i]);

        vector<int32_t> localHighAttrs(highAttrValues.begin() + oldBaseDocId,
                                       highAttrValues.begin() + oldBaseDocId + docCounts[i]);

        AttributeSegmentReaderPtr highAttrSegReader(new FakeAttributeSegmentReaderRM(localHighAttrs));

        vector<int32_t> localLowAttrs(lowAttrValues.begin() + oldBaseDocId,
                                      lowAttrValues.begin() + oldBaseDocId + docCounts[i]);

        AttributeSegmentReaderPtr lowAttrSegReader(new FakeAttributeSegmentReaderRM(localLowAttrs));

        vector<docid_t> joinAttrs(mMainJoinAttrValue.begin() + oldBaseDocId,
                                  mMainJoinAttrValue.begin() + oldBaseDocId + docCounts[i]);

        AttributeSegmentReaderPtr joinAttrSegReader(new FakeAttributeSegmentReaderRM(joinAttrs));

        std::vector<AttributeSegmentReaderPtr> attrSegReaders;
        attrSegReaders.push_back(highAttrSegReader);
        attrSegReaders.push_back(lowAttrSegReader);
        attrSegReaders.push_back(joinAttrSegReader);

        InMemorySegmentReaderPtr inMemSegReader(new FakeInMemorySegmentReader(attrSegReaders));

        SegmentMergeInfo segMergeInfo(segmentIds[i], segInfo, deletedDocCount, newBaseDocId);
        // TODO: use in memory segment reader
        segMergeInfo.TEST_inMemorySegmentReader = inMemSegReader;
        segMergeInfos.push_back(segMergeInfo);

        oldBaseDocId += docCounts[i];
        newBaseDocId += docCounts[i];
    }
}

void ReclaimMapTest::TestLoadDocIdArrayWithOldVersion()
{
    file_system::BufferedFileWriter bufferedFileWriter;
    string filePath = mRootDir + "reclaim_map";
    ASSERT_EQ(FSEC_OK, bufferedFileWriter.Open(filePath, filePath));
    vector<docid_t> oldDocFormat;
    for (size_t i = 0; i < 1000; ++i) {
        oldDocFormat.push_back(docid_t(i));
    }
    uint32_t docCount = oldDocFormat.size();
    bufferedFileWriter.Write(&docCount, sizeof(docCount)).GetOrThrow();
    bufferedFileWriter.Write(oldDocFormat.data(), sizeof(oldDocFormat[0]) * docCount).GetOrThrow();
    ASSERT_EQ(FSEC_OK, bufferedFileWriter.Close());
    ReclaimMap reclaimMap;
    SegmentMergeInfos segMergeInfos;
    BufferedFileReader* reader = new BufferedFileReader;
    ASSERT_EQ(FSEC_OK, reader->Open(filePath));
    FileReaderPtr readerPtr(reader);
    INDEXLIB_TEST_TRUE(reclaimMap.LoadDocIdArray(readerPtr, segMergeInfos, -1));
    const vector<docid_t>& docIds = reclaimMap.GetSliceArray();
    for (size_t i = 0; i < oldDocFormat.size(); ++i) {
        INDEXLIB_TEST_EQUAL(oldDocFormat[i], docIds[i]);
    }
}
}} // namespace indexlib::index
