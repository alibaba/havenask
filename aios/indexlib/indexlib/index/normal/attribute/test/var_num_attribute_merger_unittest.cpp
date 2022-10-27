#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/util/hash_string.h"
#include "indexlib/index/normal/attribute/accessor/uniq_encoded_var_num_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/index/normal/attribute/test/attribute_test_util.h"
#include "indexlib/index/normal/attribute/test/attribute_writer_helper.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory_creator.h"
#include "autil/mem_pool/Pool.h"

#include "indexlib/index/test/index_test_util.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

// used for testing average threshold
#define AVERAGE_DATA_FILE_LEN (200 * sizeof(T))

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(index);

class VarNumAttributeMergerTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override
    {
        mAttribueMerger = NULL;
        mDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
        DELETE_AND_SET_NULL(mAttribueMerger);
    }

    void TestCaseForMerge()
    {
        DoNoDeleteTest<int32_t>();
        DoTestMergeWithDeletedDocs<int32_t>();
        DoSortMergeNoDeleteTest<int32_t>();
        DoTestSortMergeWithDeleteDocs<int32_t>();
    }

    void TestCaseForCompressDataMerge()
    {
        // uniq encode
        DoNoDeleteTest<int32_t>(true, false);
        DoTestMergeWithDeletedDocs<int32_t>(true, false);
        DoSortMergeNoDeleteTest<int32_t>(true, false);
        DoTestSortMergeWithDeleteDocs<int32_t>(true, false);

        // equal compress
        DoNoDeleteTest<int32_t>(false, true);
        DoTestMergeWithDeletedDocs<int32_t>(false, true);
        DoSortMergeNoDeleteTest<int32_t>(false, true);
        DoTestSortMergeWithDeleteDocs<int32_t>(false, true);

        // uniq & equal compress
        DoNoDeleteTest<int32_t>(true, true);
        DoTestMergeWithDeletedDocs<int32_t>(true, true);
        DoSortMergeNoDeleteTest<int32_t>(true, true);
        DoTestSortMergeWithDeleteDocs<int32_t>(true, true);
    }

    void TestCaseForMergeWithSameSegments()
    {
        InnerTestMergeSameSegments<uint32_t>(100, 5);
        InnerTestMergeSameSegments<uint32_t>(100, 5, true);
        InnerTestMergeSameSegments<uint32_t>(100, 5, true, 1);        
    }

    void TestCaseForMergeWithEmptySegment() {
        vector<uint32_t> docCounts(5, 10);
        docCounts[1] = 0;
        docCounts[4] = 0;
        DoInnerTestMergeSameSegments<uint32_t>(docCounts);
        DoInnerTestMergeSameSegments<uint32_t>(docCounts, true);
        DoInnerTestMergeSameSegments<uint32_t>(docCounts, false, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2);
        DoInnerTestMergeSameSegments<uint32_t>(docCounts, true, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2);
    }

    // merge should not seek back
    void TestCaseForBug256578()
    {
        uint32_t valueSize = 
            ITEM_COUNT_PER_FIELD * sizeof(uint32_t) + 
            VarNumAttributeFormatter::GetEncodedCountLength(ITEM_COUNT_PER_FIELD);

        vector<uint64_t> expectedOffsets;
        expectedOffsets.push_back(2* valueSize);
        expectedOffsets.push_back(0);
        expectedOffsets.push_back(valueSize);
        expectedOffsets.push_back(2 * valueSize);
        expectedOffsets.push_back(0);
        expectedOffsets.push_back(valueSize);
        expectedOffsets.push_back(valueSize);
        expectedOffsets.push_back(2 * valueSize);
        InnerTestForBug256578(false, expectedOffsets);

        // sort merge
        expectedOffsets.clear();
        expectedOffsets.push_back(valueSize * 2);
        expectedOffsets.push_back(0);
        expectedOffsets.push_back(valueSize);
        expectedOffsets.push_back(0);
        expectedOffsets.push_back(valueSize);
        expectedOffsets.push_back(2 * valueSize);
        expectedOffsets.push_back(valueSize);
        expectedOffsets.push_back(2 * valueSize);
        InnerTestForBug256578(true, expectedOffsets);
    }

private:
    void InnerTestForBug256578(bool sortMerge, const vector<uint64_t> &expectedOffsets)
    {
        vector<uint32_t> docCounts;
        docCounts.push_back(6);
        docCounts.push_back(6);
        vector<set<docid_t> > delDocIds;
        delDocIds.resize(2);
        delDocIds[0].insert(0);
        delDocIds[0].insert(1);
        delDocIds[1].insert(2);
        delDocIds[1].insert(3);
        ReclaimMapPtr reclaimMap;
        if (!sortMerge)
        {
            reclaimMap = IndexTestUtil::CreateReclaimMap(docCounts, delDocIds);
        }
        else
        {
            reclaimMap = IndexTestUtil::CreateSortMergingReclaimMap(docCounts, delDocIds);
        }
        InnerTestMerge<uint32_t>(docCounts, delDocIds, sortMerge, true, false);
        // check attribute data and offset
        VarNumAttributeReader<uint32_t> attrReader;
        //SegmentDirectoryPtr segDir = IndexTestUtil::CreateSegmentDirectory(mDir, 1, 2);
        PartitionDataPtr partitionData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), 1, 2);
        INDEXLIB_TEST_TRUE(attrReader.Open(mAttrConfig, partitionData));
        typedef VarNumAttributeReader<uint32_t>::SegmentReaderPtr SegmentReaderPtr;
        const vector<SegmentReaderPtr> &segReaders = attrReader.GetSegmentReaders();
        INDEXLIB_TEST_EQUAL((size_t)1, segReaders.size());
        const SegmentReaderPtr segReader = segReaders[0];
        for (uint32_t i = 0; i < expectedOffsets.size(); i++)
        {
            INDEXLIB_TEST_EQUAL(expectedOffsets[i], segReader->GetOffset(i));
        }
    }

    template <typename T>
    void DoNoDeleteTest(bool uniqEncode = false, bool equalCompress = false)
    {
        vector<uint32_t> docCounts;
        vector<set<docid_t> > delDocIdSets;
        AttributeTestUtil::CreateDocCounts(1, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode, equalCompress);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2);

        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode, equalCompress);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2);

        docCounts.push_back(0);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode, equalCompress);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2);

        // test for uint64 offset
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode, equalCompress, 1);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode, equalCompress, 1, 2);

        // test for average data file length
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode, equalCompress, AVERAGE_DATA_FILE_LEN);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode, equalCompress, AVERAGE_DATA_FILE_LEN, 2);
    }

    template <typename T>
    void DoTestMergeWithDeletedDocs(bool uniqEncode = false, bool equalCompress = false)
    {
        vector<uint32_t> docCounts;
        vector<set<docid_t> > delDocIdSets;
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2);

        docCounts.push_back(0);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2);

        // test for uint64 offset
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress, 1);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress, 1, 2);

        // test for averge data file length 
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress, AVERAGE_DATA_FILE_LEN);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress, AVERAGE_DATA_FILE_LEN, 2);

    }

    template <typename T>
    void DoSortMergeNoDeleteTest(bool uniqEncode = false, bool equalCompress = false)
    {
        vector<uint32_t> docCounts;
        vector<set<docid_t> > delDocIdSets;
        ReclaimMapPtr reclaimMap;

        AttributeTestUtil::CreateDocCounts(1, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 1,
                          1);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2,
                          1);

        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 1,
                          1);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2,
                          1);

        docCounts.push_back(0);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 1,
                          1);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2,
                          1);

        // test for uint64_t offset
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, 1, 1, 1);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, 1, 2, 1);

        // test for average data file length
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, AVERAGE_DATA_FILE_LEN, 1, 1);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, AVERAGE_DATA_FILE_LEN, 2, 1);

    }

    template <typename T>
    void DoTestSortMergeWithDeleteDocs(bool uniqEncode = false, bool equalCompress = false)
    {
        vector<set<docid_t> > delDocIdSets;
        vector<uint32_t> docCounts;
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);

        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2);

        docCounts.push_back(0);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress);
        InnerTestMerge<T>(docCounts, delDocIdSets, false, uniqEncode,
                          equalCompress, VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD, 2);

        // test for uint64 offset
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, 1);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, 1, 2);

        // test for average data file length
        AttributeTestUtil::CreateDocCounts(3, docCounts);
        AttributeTestUtil::CreateDelSets(docCounts, delDocIdSets);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, AVERAGE_DATA_FILE_LEN);
        InnerTestMerge<T>(docCounts, delDocIdSets, true, uniqEncode,
                          equalCompress, AVERAGE_DATA_FILE_LEN, 2);

    }

    template <typename T>
    void InnerTestMergeSameSegments(uint32_t docCount, 
                                    uint32_t segCount,
                                    bool sortMerge = false,
                                    uint64_t offsetThresHold = VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD)
    {
        vector<uint32_t> docCounts(segCount, docCount);
        DoInnerTestMergeSameSegments<T>(docCounts, sortMerge, offsetThresHold);
        DoInnerTestMergeSameSegments<T>(docCounts, sortMerge, offsetThresHold, 2);
    }

    template <typename T>
    void DoInnerTestMergeSameSegments(const std::vector<uint32_t> &docCounts, 
                                      bool sortMerge = false,
                                      uint64_t offsetThresHold = VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD,
                                      uint32_t targetSegmentCount = 1)
    {
        TearDown();
        SetUp();
        mAttrConfig = AttributeTestUtil::CreateAttrConfig<T>(true);
        mAttrConfig->SetUniqEncode(true);
        mAttrConfig->GetFieldConfig()->SetU32OffsetThreshold(offsetThresHold);

        uint32_t segCount = docCounts.size();
        uint32_t validSegment = 0;
        for (uint32_t count : docCounts) {
            if (count > 0) {
                validSegment++;
            }
        }

        vector<set<docid_t> > delDocIdSets;
        AttributeTestUtil::CreateEmptyDelSets(docCounts, delDocIdSets);

        ReclaimMapPtr reclaimMap;
        SegmentMergeInfos emptyMergeInfos;
        vector<docid_t> baseDocIds = IndexTestUtil::GetTargetBaseDocIds(
            docCounts, delDocIdSets, emptyMergeInfos,
            targetSegmentCount);
        if (sortMerge)
        {
            reclaimMap = IndexTestUtil::CreateSortMergingReclaimMap(docCounts, delDocIdSets, baseDocIds);
        }
        else
        {
            reclaimMap = IndexTestUtil::CreateReclaimMap(docCounts, delDocIdSets,
                                                       false, baseDocIds);
        }

        static const uint32_t FIELD_NUM_PER_DOC = 3;
        vector<vector<vector<T> > > multiSegmentData;
        CreateMultiSegmentData(docCounts, FIELD_NUM_PER_DOC, multiSegmentData);
        BuildData(multiSegmentData);
        
        merger::SegmentDirectoryPtr segDir = IndexTestUtil::CreateSegmentDirectory(
                mDir, segCount);
        segDir->Init(false, false);
        
        mAttribueMerger = new UniqEncodedVarNumAttributeMerger<T>(false);
        mAttribueMerger->Init(mAttrConfig);

        vector<string> segmentDirs(targetSegmentCount);
        OutputSegmentMergeInfos outputSegs(targetSegmentCount);
        vector<string> attrPaths(targetSegmentCount);
        uint32_t mergeSegmentId = segCount;

        for (uint32_t i = 0; i < targetSegmentCount; ++i) {
            stringstream segmentDir;
            segmentDir << mDir << SEGMENT_FILE_NAME_PREFIX << "_"
                       << mergeSegmentId + i << "_level_0/";
            segmentDirs[i] = segmentDir.str();
            attrPaths[i] = segmentDir.str() + ATTRIBUTE_DIR_NAME + "/";
            IndexTestUtil::MkDir(attrPaths[i]);

            outputSegs[i].targetSegmentIndex = i;
            outputSegs[i].path = attrPaths[i];
            outputSegs[i].directory = DirectoryCreator::Create(attrPaths[i]);
        }

        SegmentMergeInfos segMergeInfos = CreateSegmentMergeInfos(
                docCounts, delDocIdSets);

        MergerResource resource;
        resource.targetSegmentCount = targetSegmentCount;
        resource.reclaimMap = reclaimMap;
        
        mAttribueMerger->BeginMerge(segDir);
        if (sortMerge)
        {
            mAttribueMerger->SortByWeightMerge(resource, segMergeInfos, outputSegs);
        } 
        else 
        {
            mAttribueMerger->Merge(resource, segMergeInfos, outputSegs);
        }

        uint32_t total =
            IndexTestUtil::GetTotalDocCount(docCounts, delDocIdSets, {});
        // same segment
        uint32_t mergedDocCount = docCounts.size() > 0 ? docCounts[0] : 0;
        vector<uint32_t> segInfoDocCounts(targetSegmentCount);
        vector<uint32_t> mergedDocCounts(targetSegmentCount);
        uint32_t splitFactor = total / targetSegmentCount;
        uint32_t mergedSplitFactor = mergedDocCount / targetSegmentCount;
        for (uint32_t i = 0; i < targetSegmentCount; ++i) {
            SegmentInfo segInfo;
            segInfo.docCount = splitFactor;
            mergedDocCounts[i] = mergedSplitFactor;
            if (i == targetSegmentCount - 1) {
              segInfo.docCount = total - (splitFactor * i);
              mergedDocCounts[i] = mergedDocCount - (mergedSplitFactor * i);
            }
            segInfoDocCounts[i] = segInfo.docCount;
            segInfo.Store(FileSystemWrapper::JoinPath(segmentDirs[i],
                                                      SEGMENT_INFO_FILE_NAME));
        }
        
        VarNumAttributeReader<T> attrReader;
        PartitionDataPtr partitionData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), targetSegmentCount, mergeSegmentId);
        INDEXLIB_TEST_TRUE(attrReader.Open(mAttrConfig, partitionData));
        for (uint32_t i = 0; i < targetSegmentCount; ++i) {
          CheckOffsetFile(segInfoDocCounts[i],
                          attrPaths[i] + mAttrConfig->GetAttrName(),
                          offsetThresHold);
          uint64_t dataFileSize;
          if (sortMerge) {
              dataFileSize =
                  (1 + sizeof(T) * FIELD_NUM_PER_DOC) * ((segInfoDocCounts[i] + validSegment -1)/validSegment);
          } else {
              // count take 1 byte
              dataFileSize =
                  (1 + sizeof(T) * FIELD_NUM_PER_DOC) * mergedDocCount;
          }
            CheckDataFileSize(dataFileSize,
                              attrPaths[i] + mAttrConfig->GetAttrName());
        }
        Check(attrReader, attrPaths, mAttrConfig, multiSegmentData, reclaimMap,
              segMergeInfos);
    }

    template <typename T>
    void InnerTestMerge(const vector<uint32_t>& docCounts, 
                        const vector<set<docid_t> >& delDocIdSets,
                        bool sortMerge,
                        bool uniqEncode,
                        bool equalCompress,
                        uint64_t offsetThresHold = VAR_NUM_ATTRIBUTE_OFFSET_THRESHOLD,
                        uint32_t targetSegmentCount = 1,
                        int32_t reclaimMapMode = 0) 
    {
        TearDown();
        SetUp();
        
        
        mAttrConfig = AttributeTestUtil::CreateAttrConfig<T>(true);
        FieldConfigPtr fieldConfig = mAttrConfig->GetFieldConfig();
        string compTypeStr = CompressTypeOption::GetCompressStr(uniqEncode, equalCompress, 
                mAttrConfig->GetCompressType().HasPatchCompress());
        fieldConfig->SetCompressType(compTypeStr);
        fieldConfig->SetU32OffsetThreshold(offsetThresHold);

        vector<vector<vector<T> > > multiSegmentData;
        CreateMultiSegmentData(docCounts, multiSegmentData);
        BuildData(multiSegmentData);
        SegmentMergeInfos segMergeInfos = CreateSegmentMergeInfos(
                docCounts, delDocIdSets);

        ReclaimMapPtr reclaimMap;
        SegmentMergeInfos emptyMergeInfos;
        vector<docid_t> baseDocIds = IndexTestUtil::GetTargetBaseDocIds(
            docCounts, delDocIdSets, emptyMergeInfos,
            targetSegmentCount);
        // Compatible with old ut: sort merge use normal reclaimMap
        if (reclaimMapMode == 0) {
            if (sortMerge) {
                reclaimMap = IndexTestUtil::CreateSortMergingReclaimMap(
                    docCounts, delDocIdSets, baseDocIds);
            } else {
                reclaimMap = IndexTestUtil::CreateReclaimMap(
                    docCounts, delDocIdSets, false, baseDocIds);
            }
        } else if (reclaimMapMode == 1) {
            reclaimMap = IndexTestUtil::CreateReclaimMap(
                docCounts, delDocIdSets, false, baseDocIds);
        }
        
        uint32_t segmentCount = docCounts.size();
        merger::SegmentDirectoryPtr segDir = IndexTestUtil::CreateSegmentDirectory(
                mDir, segmentCount);
        segDir->Init(false, false);
        if (uniqEncode)
        {
            mAttribueMerger = new UniqEncodedVarNumAttributeMerger<T>(false);
        }
        else
        {
            mAttribueMerger = new VarNumAttributeMerger<T>(false);
        }
        mAttribueMerger->Init(mAttrConfig);        

        vector<string> segmentDirs(targetSegmentCount);
        OutputSegmentMergeInfos outputSegs(targetSegmentCount);
        vector<string> attrPaths(targetSegmentCount);

        segmentid_t mergeSegmentId = segmentCount;
        for (uint32_t i = 0; i < targetSegmentCount; ++i) {
            stringstream segmentDir;
            segmentDir << mDir << SEGMENT_FILE_NAME_PREFIX << "_"
                       << mergeSegmentId + i << "_level_0/";
            segmentDirs[i] = segmentDir.str();
            attrPaths[i] = segmentDir.str() + ATTRIBUTE_DIR_NAME + "/";
            IndexTestUtil::MkDir(attrPaths[i]);

            outputSegs[i].targetSegmentIndex = i;
            outputSegs[i].path = attrPaths[i];
            outputSegs[i].directory = DirectoryCreator::Create(attrPaths[i]);
        }

        MergerResource resource;
        resource.targetSegmentCount = targetSegmentCount;
        resource.reclaimMap = reclaimMap;
        
        mAttribueMerger->BeginMerge(segDir);
        if (sortMerge)
        {
            mAttribueMerger->SortByWeightMerge(resource, segMergeInfos, outputSegs);
        } 
        else 
        {
            mAttribueMerger->Merge(resource, segMergeInfos, outputSegs);
        }

        uint32_t total =
            IndexTestUtil::GetTotalDocCount(docCounts, delDocIdSets, {});
        vector<uint32_t> segInfoDocCounts(targetSegmentCount);
        uint32_t splitFactor = total / targetSegmentCount;
        for (uint32_t i = 0; i < targetSegmentCount; ++i) {
            SegmentInfo segInfo;
            segInfo.docCount = splitFactor;
            if (i == targetSegmentCount - 1) {
                segInfo.docCount = total - (splitFactor*i);
            }
            segInfoDocCounts[i] = segInfo.docCount;
            segInfo.Store(FileSystemWrapper::JoinPath(segmentDirs[i],
                                                      SEGMENT_INFO_FILE_NAME));
        }
         
        VarNumAttributeReader<T> attrReader;
        PartitionDataPtr partitionData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), targetSegmentCount, mergeSegmentId);
        INDEXLIB_TEST_TRUE(attrReader.Open(mAttrConfig, partitionData));

        for (uint32_t i = 0; i < targetSegmentCount; ++i) {
            CheckOffsetFile(segInfoDocCounts[i],
                            attrPaths[i] + mAttrConfig->GetAttrName(),
                            offsetThresHold);
        }
        Check(attrReader, attrPaths, mAttrConfig, multiSegmentData, reclaimMap,
              segMergeInfos);
    }

    template <typename T>
    void CreateMultiSegmentData(const vector<uint32_t>& docCounts, 
                                vector<vector<vector<T> > >& multiSegmentData)
    {
        for (size_t i = 0; i < docCounts.size(); ++i)
        {
            uint32_t docCount = docCounts[i];
            vector<vector<T> > segmentData;
            CreateSegmentData(docCount, segmentData);
            multiSegmentData.push_back(segmentData);
        }
    }

    template <typename T>
    void CreateMultiSegmentData(const std::vector<uint32_t>& docCounts,
                                uint32_t fieldNumPerDoc,
                                vector<vector<vector<T> > >& multiSegmentData)
    {
        for (uint32_t i = 0; i < docCounts.size(); i++)
        {
            vector<vector<T> > oneSeg;
            // create one segment data
            for (uint32_t j = 0; j < docCounts[i]; j++)
            {
                vector<T> oneDoc;
                for (uint32_t k = 0; k < fieldNumPerDoc; k++)
                {
                    oneDoc.push_back(j+k);
                }
                oneSeg.push_back(oneDoc);
            }
            multiSegmentData.push_back(oneSeg);
        }
    }

    template <typename T>
    void CreateSegmentData(uint32_t docCount, vector<vector<T> >& segmentData)
    {
        vector<T> v0(ITEM_COUNT_PER_FIELD, 0);
        vector<T> v1(ITEM_COUNT_PER_FIELD, 1);
        vector<T> v2(ITEM_COUNT_PER_FIELD, 2);
        for (size_t i = 0; i < docCount; ++i)
        {
            if (i % 3 == 0)
            {
                segmentData.push_back(v0);
            }
            else if (i % 3 == 1)
            {
                segmentData.push_back(v1);
            }
            else
            {
                segmentData.push_back(v2);
            }
        }
    }

    SegmentMergeInfos CreateSegmentMergeInfos(const vector<uint32_t>& docCounts,
            const vector<set<docid_t> >& delDocIdSets)
    {
        SegmentMergeInfos segMergeInfos;
        docid_t baseDocId = 0;
        for (size_t i = 0; i < docCounts.size(); ++i)
        {
            SegmentMergeInfo mergeInfo;
            mergeInfo.segmentId = i;
            mergeInfo.segmentInfo.docCount = docCounts[i];
            mergeInfo.deletedDocCount = delDocIdSets[i].size();
            mergeInfo.baseDocId = baseDocId;
            segMergeInfos.push_back(mergeInfo);

            baseDocId += docCounts[i];
        }
        return segMergeInfos;
    }

    uint32_t GetTotalDocCount(const vector<uint32_t>& docCounts,
                              const vector<set<docid_t> >& delDocIdSets)
    {
        uint32_t total = 0;
        for (size_t i = 0; i < docCounts.size(); ++i)
        {
            total += docCounts[i] - delDocIdSets[i].size();
        }
        return total;
    }

    template <typename T>
    void BuildData(vector<vector<vector<T> > >& multiSegmentData)
    {
        for (size_t i = 0; i < multiSegmentData.size(); ++i)
        {
            segmentid_t id = i;
            stringstream segPath;
            segPath << mDir << SEGMENT_FILE_NAME_PREFIX << "_" << id << "_level_0/";
            string attrPath = segPath.str() + ATTRIBUTE_DIR_NAME + "/";
            IndexTestUtil::MkDir(attrPath);
            
            VarNumAttributeWriter<T> writer(mAttrConfig);
            AttributeConvertor* convertor = AttributeConvertorFactory::GetInstance()
                                            ->CreateAttrConvertor(mAttrConfig->GetFieldConfig());
            writer.SetAttrConvertor(AttributeConvertorPtr(convertor));

            writer.Init(FSWriterParamDeciderPtr(), NULL);
            autil::mem_pool::Pool pool;
            for (size_t j = 0; j < multiSegmentData[i].size(); ++j)
            {
                stringstream value;
                for (size_t k = 0; k < multiSegmentData[i][j].size(); ++k)
                {
                    value << multiSegmentData[i][j][k] << MULTI_VALUE_SEPARATOR;
                }
                ConstString encodeValue = convertor->Encode(ConstString(value.str()), &pool);
                writer.AddField(j, encodeValue);
            }
            AttributeWriterHelper::DumpWriter(writer, attrPath);

            SegmentInfo segInfo;
            segInfo.docCount = multiSegmentData[i].size();
            segInfo.Store(segPath.str() + SEGMENT_INFO_FILE_NAME);
        }
    }

    void CheckOffsetFile(uint32_t docCnt, string attrPath, uint64_t thresHold)
    {
        if (mAttrConfig->GetCompressType().HasEquivalentCompress())
        {
            return;
        }

        string dataFilePath = attrPath + "/" + ATTRIBUTE_DATA_FILE_NAME;
        string offsetFilePath = attrPath + "/" + ATTRIBUTE_OFFSET_FILE_NAME;
        FileMeta fileMeta;
        uint64_t dataLen = 0L;
        fslib::ErrorCode err = FileSystem::getFileMeta(dataFilePath, fileMeta);
        INDEXLIB_TEST_EQUAL(EC_OK, err);
        dataLen = fileMeta.fileLength;

        err = FileSystem::getFileMeta(offsetFilePath, fileMeta);
        INDEXLIB_TEST_EQUAL(EC_OK, err);
        if (dataLen <= thresHold) 
        {
            INDEXLIB_TEST_EQUAL(sizeof(uint32_t) * (docCnt + 1), (uint64_t)fileMeta.fileLength);
        }
        else 
        {
            INDEXLIB_TEST_EQUAL(sizeof(uint64_t) * (docCnt + 1), (uint64_t)fileMeta.fileLength);
        }
    }

    void CheckDataFileSize(uint64_t expectedSize, const string& attrPath)
    {
        string dataFilePath = attrPath + "/" + ATTRIBUTE_DATA_FILE_NAME;
        FileMeta fileMeta;
        uint64_t dataLen = 0L;
        fslib::ErrorCode err = FileSystem::getFileMeta(dataFilePath, fileMeta);
        INDEXLIB_TEST_EQUAL(EC_OK, err);
        dataLen = fileMeta.fileLength;
        INDEXLIB_TEST_EQUAL(expectedSize, dataLen);
    }

    struct AttrCheckInfo { 
        uint32_t maxItemLen = 0;
        std::set<std::string> valueSet;
        uint32_t valueCount = 0;
        AttributeDataInfo dataInfo;
    };
    
    template <typename T>
    void Check(const VarNumAttributeReader<T>& reader, 
               const vector<string>& attrDirs,
               const config::AttributeConfigPtr& attrConfig,
               const vector<vector<vector<T> > >& answer,
               const ReclaimMapPtr& reclaimMap,
               const SegmentMergeInfos& segMergeInfos)
    {

        vector<AttrCheckInfo> checkInfos(attrDirs.size());
        for (size_t i = 0, size = attrDirs.size(); i < size; ++i) {
            string dataInfoPath = FileSystemWrapper::JoinPath(
                attrDirs[i] + attrConfig->GetAttrName(),
                ATTRIBUTE_DATA_INFO_FILE_NAME);
            ASSERT_TRUE(FileSystemWrapper::IsExist(dataInfoPath));
            string fileContent;
            FileSystemWrapper::AtomicLoad(dataInfoPath, fileContent);
            checkInfos[i].dataInfo.FromString(fileContent);
        }
        AttributeIteratorBase* attrIter = reader.CreateIterator(&_pool);
        typename VarNumAttributeReader<T>::AttributeIterator* typedIter = 
            dynamic_cast<typename VarNumAttributeReader<T>::AttributeIterator*> (attrIter);
        for (size_t i = 0; i < answer.size(); ++i)
        {
            const vector<vector<T> >& answerSegment = answer[i];
            for (size_t j = 0; j < answerSegment.size(); ++j)
            {
              docid_t newId =
                  reclaimMap->GetNewId(j + segMergeInfos[i].baseDocId);
              segmentindex_t segIdx = reclaimMap->GetTargetSegmentIndex(newId);
              if (newId == INVALID_DOCID) {
                    continue;
                }
                const vector<T>& answerDoc = answerSegment[j];
                MultiValueType<T> multiValue;
                INDEXLIB_TEST_TRUE(typedIter->Seek(newId, multiValue));
                INDEXLIB_TEST_EQUAL(answerDoc.size(), multiValue.size());

                for (size_t k = 0; k < multiValue.size(); ++k)
                {
                    INDEXLIB_TEST_EQUAL(answerDoc[k], multiValue[k]);
                }

                stringstream ss;
                ss << multiValue;
                checkInfos[segIdx].valueSet.insert(ss.str());
                checkInfos[segIdx].maxItemLen =
                    std::max(checkInfos[segIdx].maxItemLen, multiValue.length());
                checkInfos[segIdx].valueCount++;
            }
        }

        if (mAttrConfig->IsUniqEncode())
        {
            for (auto& info: checkInfos) {
                ASSERT_EQ((uint32_t)info.valueSet.size(), info.dataInfo.uniqItemCount);
            }
        }
        else
        {
            for (auto& info: checkInfos) {
                ASSERT_EQ(info.valueCount, info.dataInfo.uniqItemCount);                
            }
        }
        for (auto &info : checkInfos) {
            ASSERT_EQ(info.maxItemLen, info.dataInfo.maxItemLen);
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(&_pool, attrIter);
    }
private:
    string mDir;
    AttributeConfigPtr mAttrConfig;
    AttributeMerger *mAttribueMerger;
    autil::mem_pool::Pool _pool;
    static const uint32_t ITEM_COUNT_PER_FIELD = 5;
};

INDEXLIB_UNIT_TEST_CASE(VarNumAttributeMergerTest, TestCaseForMerge);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeMergerTest, TestCaseForCompressDataMerge);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeMergerTest, TestCaseForMergeWithSameSegments);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeMergerTest, TestCaseForMergeWithEmptySegment);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributeMergerTest, TestCaseForBug256578);

IE_NAMESPACE_END(index);
