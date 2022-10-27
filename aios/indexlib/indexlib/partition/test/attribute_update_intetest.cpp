#include <string>
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/partition/test/index_partition_maker.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index/test/document_checker_for_gtest.h"
#include "indexlib/merger/test/merge_task_maker.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(merger);

IE_NAMESPACE_BEGIN(partition);

class AttributeUpdateInteTest : public INDEXLIB_TESTBASE
{
public:
    typedef DocumentMaker::MockIndexPart MockIndexPart;
    typedef DocumentMaker::MockDeletionMap MockDeletionMap;
    typedef IndexTestUtil::DeletionMode DeletionMode;
    typedef vector<pair<uint32_t, DeletionMode> > SegVect;

public:
    AttributeUpdateInteTest()
    {
        uint32_t docsForFullBuild[] = {50,21,30,5};
        mFullBuildDocVec.assign(
                docsForFullBuild, 
                docsForFullBuild + sizeof(docsForFullBuild) / sizeof(uint32_t));
        uint32_t docsForIncBuild[] = {2, 3, 2};
        mIncBuildDocVec.assign(
                docsForIncBuild, 
                docsForIncBuild + sizeof(docsForIncBuild) / sizeof(uint32_t));

        mDocCountInFullBuild = 0;
        for (size_t i = 0; i < mFullBuildDocVec.size(); i++)
        {
            mDocCountInFullBuild += mFullBuildDocVec[i];
        }
    }

    DECLARE_CLASS_NAME(AttributeUpdateInteTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();

        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                //Field schema
                "text1:text;text2:text;string1:string;long1:uint32;string2:string:true;long2:uint32:true", 
                //Index schema
                "pack1:pack:text1,text2;txt1:text:text1;txt2:text:text2;string2:string:string2;expack1:expack:text1,text2;"
                //Primary key index schema
                "pk:primarykey64:string1",
                //Attribute schema
                "long1;string1",
                //Summary schema
                "string1;text1" );
    }

    void CaseTearDown() override
    {
        if (FileSystemWrapper::IsExist(mRootDir))
        {
            FileSystemWrapper::DeleteDir(mRootDir);
        }
        FileSystemWrapper::ClearError();
    }

    void TestCaseForAttrUpdateNoSortOptMergeLongCaseTest()
    {
        mOptions.GetMergeConfig().mergeStrategyStr = "optimize";
        InnerTestForAttributeUpdate();
    }

    void TestCaseForAttrUpdateNoSortBalanceTreeMergeLongCaseTest()
    {   
        mOptions.GetMergeConfig().mergeStrategyStr = "balance_tree";
        mOptions.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
                "base-doc-count=100;max-doc-count=8000;"
                "conflict-segment-number=3");

        InnerTestForAttributeUpdate();
    }

    void TestCaseForAttrUpdateNoSortPriorityQueueMergeLongCaseTest()
    {   
        mOptions.GetMergeConfig().mergeStrategyStr = "priority_queue";
        mOptions.GetMergeConfig().mergeStrategyParameter.inputLimitParam = "max-doc-count=8000";
        mOptions.GetMergeConfig().mergeStrategyParameter.strategyConditions = "conflict-segment-count=3";
        mOptions.GetMergeConfig().mergeStrategyParameter.outputLimitParam = "max-merged-segment-doc-count=1000";
        InnerTestForAttributeUpdate();
    }

    uint32_t GetDocCount(const string& segmentPath)
    {
        stringstream ss;
        ss << mRootDir << segmentPath << "/" << SEGMENT_INFO_FILE_NAME;
        SegmentInfo segInfo;
        segInfo.Load(ss.str());
        return segInfo.docCount;
    }

    void InnerTestForAttributeUpdate()
    {
        for (uint32_t i = 0; i < (uint32_t)IndexTestUtil::DM_MAX_MODE; i++)
        {
            DocumentMaker::MockIndexPart mockIndexPart;
            InnerTestForAttributeUpdateInFullBuild(mSchema, mFullBuildDocVec, 
                    IndexTestUtil::DeletionMode(i), mockIndexPart);

            InnerTestForAttributeUpdateInIncBuild(mSchema, mIncBuildDocVec, 
                    IndexTestUtil::DeletionMode(i), 3, mDocCountInFullBuild, 
                    mockIndexPart);
            FileSystemWrapper::DeleteDir(mRootDir);
        }        
    }

    void InnerTestForAttributeUpdateInFullBuild(IndexPartitionSchemaPtr schema,
            vector<uint32_t>& docCounts, IndexTestUtil::DeletionMode delMode, 
            DocumentMaker::MockIndexPart& newMockIndexPart)
    {
        int32_t pkStartSuffix = 0;
        DocumentMaker::MockIndexPart mockIndexPart;

        {
            OfflinePartition indexPart4Write;
            IndexPartitionOptions options = mOptions;
            options.SetIsOnline(false);
            indexPart4Write.Open(mRootDir, "", schema, options);
            MakeSegmentData(docCounts, indexPart4Write, mockIndexPart, 
                            pkStartSuffix, delMode);
            
            GetNewMockIndexPartition(mockIndexPart, docCounts,
                    delMode, false, 0, 0, newMockIndexPart);
            
            PartitionMergerPtr merger(
                    PartitionMergerCreator::CreateSinglePartitionMerger(
                            mRootDir, options, NULL, ""));
            merger->Merge(true);
        }
        OnlinePartition indexPart4Read;
        indexPart4Read.Open(mRootDir, "", schema, mOptions);
        IndexPartitionReaderPtr reader = indexPart4Read.GetReader();
        DocumentCheckerForGtest checker;
        checker.CheckData(reader, schema, newMockIndexPart);
    }

    void InnerTestForAttributeUpdateInIncBuild(IndexPartitionSchemaPtr schema,
            vector<uint32_t>& docCounts, IndexTestUtil::DeletionMode delMode, 
            uint32_t incCount, int32_t pkStartSuffix,
            DocumentMaker::MockIndexPart& mockIndexPart)
    {
        DocumentMaker::MockIndexPart newMockIndexPart;

        uint32_t docCountInIncBuild = 0;
        for (size_t i = 0; i < docCounts.size(); i++)
        {
            docCountInIncBuild += docCounts[i];
        }

        for (uint32_t i = 0; i < incCount; i++)
        {
            {
                Version version;
                VersionLoader::GetVersion(mRootDir, version, INVALID_VERSION);
                
                OfflinePartition indexPart4Write;
                IndexPartitionOptions options = mOptions;
                options.SetIsOnline(false);
                indexPart4Write.Open(mRootDir, "", schema, options);
                MakeSegmentData(docCounts, indexPart4Write, mockIndexPart, 
                        pkStartSuffix, delMode);

                if (mOptions.GetMergeConfig().mergeStrategyStr == "optimize")
                {
                    uint32_t startSegmentId = (uint32_t)version[0];
                    uint32_t oldDocCount = GetDocCount(version.GetSegmentDirName(startSegmentId));
                    GetNewMockIndexPartition(mockIndexPart, docCounts,
                            delMode, true, startSegmentId, oldDocCount, 
                            newMockIndexPart);
                }
                else
                {
                    vector<uint32_t> docs;
                    vector<segmentid_t> segmentIds;
                    segmentid_t lastSegmentId;
                    for (size_t i = 0; i < version.GetSegmentCount(); i++)
                    {
                        lastSegmentId = version[i];
                        docs.push_back(GetDocCount(version.GetSegmentDirName(lastSegmentId)));
                        segmentIds.push_back(lastSegmentId);
                    }
                    docs.insert(docs.end(), docCounts.begin(), docCounts.end());
                    for (size_t i = 0; i < docCounts.size(); i++)
                    {
                        segmentIds.push_back(lastSegmentId + 1 + i);
                    }
                    GetNewMockIndexPartition(mockIndexPart, docs, segmentIds,
                            delMode, newMockIndexPart);
                }

                PartitionMergerPtr merger(
                        PartitionMergerCreator::CreateSinglePartitionMerger(
                                mRootDir, options, NULL, ""));
                merger->Merge(false);
            }
            OnlinePartition indexPart4Read;
            indexPart4Read.Open(mRootDir, "", schema, mOptions);
            IndexPartitionReaderPtr newReader = indexPart4Read.GetReader();
            {
                DocumentCheckerForGtest checker;
                checker.CheckData(newReader, schema, newMockIndexPart);
            }

            pkStartSuffix += docCountInIncBuild;
            mockIndexPart = newMockIndexPart;
        }
    }

    void MakeSegmentData(vector<uint32_t>& docCounts, 
                         IndexPartition& indexPart4Write,
                         DocumentMaker::MockIndexPart& mockIndexPart,
                         uint32_t pkStartSuffix,
                         IndexTestUtil::DeletionMode delMode)
    {
        IndexPartitionMaker::MakeMultiSegmentData(
                docCounts, indexPart4Write, mockIndexPart, 
                pkStartSuffix, delMode, true);
    }

    // for balance tree merge
    void GetNewMockIndexPartition(const DocumentMaker::MockIndexPart& mockIndexPart, 
                                  const vector<uint32_t>& docCounts,
                                  vector<segmentid_t> segmentIds,
                                  IndexTestUtil::DeletionMode delMode,
                                  DocumentMaker::MockIndexPart& newMockIndexPart)
    {
        IndexPartitionOptions options = mOptions;
        options.SetIsOnline(false);
        PartitionMergerPtr mergerBase(
                PartitionMergerCreator::CreateSinglePartitionMerger(
                        mRootDir, options, NULL, ""));

        IndexPartitionMergerPtr merger = 
            DYNAMIC_POINTER_CAST(IndexPartitionMerger, mergerBase);
        assert(merger);
        MergeTask mergeTask;
        SegmentMergeInfos segMergeInfos = merger->CreateSegmentMergeInfos(
                merger->mSegmentDirectory);
        LevelInfo levelInfo;
        if (segMergeInfos.size() != 0)        
        {
            mergeTask = merger->CreateMergeTaskByStrategy(
                false, options.GetMergeConfig(), segMergeInfos, levelInfo);
        }
        SegVect segments;
        for (size_t i = 0; i < docCounts.size(); ++i)
        {
            segments.push_back(make_pair(docCounts[i], delMode));
        }

        ReclaimMockIndexWithTask(mockIndexPart, segments,
                segmentIds, mergeTask, newMockIndexPart);
    }

    // for optimize merge
    void GetNewMockIndexPartition(const DocumentMaker::MockIndexPart& mockIndexPart, 
                                  const vector<uint32_t>& docCounts,
                                  IndexTestUtil::DeletionMode delMode,
                                  bool isInc,
                                  int32_t startSegmentId,
                                  uint32_t oldDocCount,
                                  DocumentMaker::MockIndexPart& newMockIndexPart)
    {
        string mergeTaskStr;
        SegVect segments;
        vector<segmentid_t> segmentIds;

        if (isInc)
        {
            mergeTaskStr.append(StringUtil::toString(startSegmentId));
            segments.push_back(make_pair(oldDocCount, IndexTestUtil::DM_NODELETE));
            segmentIds.push_back(startSegmentId);
        }
        else
        {
            startSegmentId = -1;
        }

        for (size_t i = 0; i < docCounts.size(); ++i)
        {
            segments.push_back(make_pair(docCounts[i], delMode));
            mergeTaskStr.append(",");
            mergeTaskStr.append(StringUtil::toString(startSegmentId + i + 1));
            segmentIds.push_back(startSegmentId + i + 1);
        }

        if (!isInc)
        {
            mergeTaskStr = mergeTaskStr.substr(1, mergeTaskStr.size() - 1);
        }

        MergeTask mergeTask;
        MergeTaskMaker::CreateMergeTask(mergeTaskStr, mergeTask);
        
        ReclaimMockIndexWithTask(mockIndexPart, segments, 
                segmentIds, mergeTask, newMockIndexPart);
    }

    //To do: extract into test utils
    void ReclaimMockIndexWithTask(const DocumentMaker::MockIndexPart& mockIndexPart,
                                  const SegVect& segments,
                                  vector<segmentid_t> segmentIds,
                                  const MergeTask& mergeTask,
                                  DocumentMaker::MockIndexPart& newMockIndexPart)
    {
        SegmentMergeInfos segMergeInfos;
        segMergeInfos.resize(segments.size());
        uint32_t baseDocId = 0;
        for (size_t i = 0; i < segments.size(); ++i)
        {
            SegmentMergeInfo& segMergeInfo = segMergeInfos[i];
            segMergeInfo.segmentId = segmentIds[i];
            segMergeInfo.baseDocId = baseDocId;
            segMergeInfo.segmentInfo.docCount = segments[i].first;
            baseDocId += segments[i].first;
        }

        newMockIndexPart = mockIndexPart;
        for (size_t i = 0; i < mergeTask.GetPlanCount(); ++i)
        {
            SegmentMergeInfos mergedSegInfos;
            ReclaimMapPtr reclaimMap = CreateReclaimMapWithPlan(
                    newMockIndexPart.deletionMap, segMergeInfos, 
                    mergeTask[i], mergedSegInfos);
            
            MockIndexPart tmpMockIndexPart;
            DocumentMaker::ReclaimDocuments(newMockIndexPart, reclaimMap, 
                    tmpMockIndexPart);
            
            segMergeInfos = mergedSegInfos;
            newMockIndexPart = tmpMockIndexPart;
        }
    }

    ReclaimMapPtr CreateReclaimMapWithPlan(const MockDeletionMap& deletionMap,
            const SegmentMergeInfos& segMergeInfos,
            const MergePlan& mergePlan,
            SegmentMergeInfos& mergedSegInfos)
    {
        uint32_t totalDocCount = 0;
        SegmentMergeInfos updatedSegInfos;
        CalcMergedSegInfos(segMergeInfos, mergePlan, deletionMap,
                           mergedSegInfos, updatedSegInfos, totalDocCount);

        ReclaimMapPtr reclaimMap(new ReclaimMap);
        reclaimMap->Init(totalDocCount);
        for (size_t i = 0; i < segMergeInfos.size(); ++i) 
        {
            const SegmentMergeInfo& segInfo = segMergeInfos[i];
            docid_t newDocId = updatedSegInfos[i].baseDocId;
            if (mergePlan.HasSegment(segInfo.segmentId))
            {
                for (docid_t j  = 0;
                     j < (docid_t)segInfo.segmentInfo.docCount; ++j) 
                {
                    docid_t oldDocId = j + segInfo.baseDocId;
                    if (deletionMap.find(oldDocId) != deletionMap.end()) 
                    {
                        reclaimMap->SetNewId(oldDocId, INVALID_DOCID);
                    } 
                    else 
                    {
                        reclaimMap->SetNewId(oldDocId, newDocId++);
                    }
                }
            }
            else
            {
                for (docid_t j  = 0;
                     j < (docid_t)segInfo.segmentInfo.docCount; ++j) 
                {
                    docid_t oldDocId = j + segInfo.baseDocId;
                    reclaimMap->SetNewId(oldDocId, newDocId++);
                }
            }
        }

        return reclaimMap;
    }

    void CalcMergedSegInfos(const SegmentMergeInfos& segMergeInfos, 
                            const MergePlan& plan,
                            const MockDeletionMap& deletionMap,
                            SegmentMergeInfos& mergedSegInfos,
                            SegmentMergeInfos& updatedSegInfos,
                            uint32_t& totalDocCount)
    {
        updatedSegInfos = segMergeInfos;

        totalDocCount = 0;
        docid_t newBaseDocId = 0;
        for (size_t i = 0; i < segMergeInfos.size(); ++i)
        {
            const SegmentMergeInfo& segMergeInfo = segMergeInfos[i];

            if (!plan.HasSegment(segMergeInfo.segmentId))
            {
                SegmentMergeInfo newSegMergeInfo = segMergeInfo;
                newSegMergeInfo.baseDocId = newBaseDocId;

                updatedSegInfos[i] = newSegMergeInfo;

                mergedSegInfos.push_back(newSegMergeInfo);

                newBaseDocId += segMergeInfo.segmentInfo.docCount;
            }
            totalDocCount += segMergeInfo.segmentInfo.docCount;
        }
        
        uint32_t newSegBaseId = newBaseDocId;
        for (size_t i = 0; i < segMergeInfos.size(); ++i)
        {
            const SegmentMergeInfo& segMergeInfo = segMergeInfos[i];
            if (!plan.HasSegment(segMergeInfo.segmentId))
            {
                continue;
            }
         
            uint32_t delDocCount = 0;
            for (docid_t j = 0;
                 j < (docid_t)segMergeInfo.segmentInfo.docCount; ++j)
            {
                MockDeletionMap::const_iterator it = 
                    deletionMap.find(j + segMergeInfo.baseDocId);
                if (it != deletionMap.end()) 
                {
                    ++delDocCount;
                }
            }

            SegmentMergeInfo newSegMergeInfo = segMergeInfo;
            newSegMergeInfo.baseDocId = newBaseDocId;
            newSegMergeInfo.deletedDocCount = delDocCount;
            updatedSegInfos[i] = newSegMergeInfo;
            newBaseDocId += (segMergeInfo.segmentInfo.docCount - delDocCount);
        }

        SegmentMergeInfo newSegMergeInfo;
        newSegMergeInfo.baseDocId = newSegBaseId;
        newSegMergeInfo.deletedDocCount = 0;
        newSegMergeInfo.segmentInfo.docCount = newBaseDocId - newSegBaseId;
        newSegMergeInfo.segmentId =
            segMergeInfos[segMergeInfos.size() - 1].segmentId + 1;
        mergedSegInfos.push_back(newSegMergeInfo);        
    }


private:
    string mRootDir;
    IndexPartitionSchemaPtr mSchema;
    IndexPartitionOptions mOptions;
    vector<uint32_t> mFullBuildDocVec;
    vector<uint32_t> mIncBuildDocVec;
    uint32_t mDocCountInFullBuild;
};

INDEXLIB_UNIT_TEST_CASE(AttributeUpdateInteTest, TestCaseForAttrUpdateNoSortOptMergeLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(AttributeUpdateInteTest, TestCaseForAttrUpdateNoSortBalanceTreeMergeLongCaseTest);
INDEXLIB_UNIT_TEST_CASE(AttributeUpdateInteTest, TestCaseForAttrUpdateNoSortPriorityQueueMergeLongCaseTest);

IE_NAMESPACE_END(partition);
