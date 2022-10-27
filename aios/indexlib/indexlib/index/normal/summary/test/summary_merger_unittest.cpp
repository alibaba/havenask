#define SUMMARY_MERGER_TEST
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/summary/local_disk_summary_merger.h"
#include "indexlib/index/normal/summary/summary_writer_impl.h"
#include "indexlib/index/normal/summary/local_disk_summary_reader.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/test/document_maker.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/test/fake_reclaim_map.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

class SummaryMergerTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SummaryMergerTest);
public:
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH() + "/";

        mSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mSchema,
                "string1:string;", // above is field schema
                "", // above is index schema
                "", // above is attribute schema
                "string1" );// above is summary schema
    }

    void CaseTearDown() override
    {
    }

    void TestForMerge()
    {
        FOR_EACH(i, IndexTestUtil::deleteFuncs)
        {
            TestMerge(IndexTestUtil::deleteFuncs[i], false);
        }
    }

    void TestForSortByWeightMerge()
    {
        FOR_EACH(i, IndexTestUtil::deleteFuncs)
        {
            TestMerge(IndexTestUtil::deleteFuncs[i], true);
        }
    }

private:
    void TestMerge(IndexTestUtil::ToDelete toDel, bool isSortByWeight = false)
    {
        TearDown();
        SetUp();

        uint32_t docCounts[] = {2, 3, 0, 4, 0, 6}; // two empty segment
        uint32_t segCount = sizeof(docCounts) / sizeof(docCounts[0]);
        vector<uint32_t> docCountsVec(docCounts, docCounts + segCount);

        uint32_t docCount = 0;
        for (size_t i = 0; i < segCount; ++i)
        {
            docCount += docCounts[i];
        }
        
        DocumentMaker::DocumentVect ansDocVect;
        SegmentMergeInfos segMergeInfos;
        Version version;
        MakeData(docCountsVec, segMergeInfos, version, ansDocVect);

        //do merge
        merger::SegmentDirectoryPtr segDir(
            new merger::SegmentDirectory(mRootDir, version));
        segDir->Init(false);
        DeletionMapReaderPtr delReader = IndexTestUtil::CreateDeletionMap(
                segDir->GetVersion(), docCountsVec, toDel);
        
        if (isSortByWeight)
        {
            mReclaimMap.reset(new FakeReclaimMap());
        }
        else
        {
            mReclaimMap.reset(new ReclaimMap());
        }
        OfflineAttributeSegmentReaderContainerPtr readerContainer;
        mReclaimMap->Init(segMergeInfos, delReader, readerContainer, false);

        LocalDiskSummaryMerger merger(mSchema->GetSummarySchema()->GetSummaryGroupConfig(0));
        merger.BeginMerge(segDir);

//        file_system::DirectoryPtr lastSegDirectory = MAKE_SEGMENT_DIRECTORY(segCount);
//        file_system::DirectoryPtr summaryDirectory = lastSegDirectory->MakeDirectory(
//                SUMMARY_DIR_NAME);
//        string summaryPath = summaryDirectory->GetPath();

        stringstream ss;
        ss << mRootDir << SEGMENT_FILE_NAME_PREFIX << '_' 
           << segCount << "_level_0/";
        string summaryPath = ss.str() + SUMMARY_DIR_NAME + "/";
        FileSystemWrapper::MkDir(summaryPath, true);

        MergerResource resource;
        resource.targetSegmentCount = 1;
        resource.reclaimMap = mReclaimMap;
        OutputSegmentMergeInfo segInfo;
        segInfo.targetSegmentIndex = 0;
        segInfo.path = summaryPath;
        segInfo.directory = DirectoryCreator::Create(segInfo.path);
        
        if (isSortByWeight)
        {
          merger.SortByWeightMerge(resource, segMergeInfos, { segInfo });
        }
        else 
        {
          merger.Merge(resource, segMergeInfos, { segInfo });
        }
        SegmentInfo mergedSegInfo;
        mergedSegInfo.docCount = docCount - 
                                 mReclaimMap->GetDeletedDocCount();
//        mergedSegInfo.Store(lastSegDirectory);
        mergedSegInfo.Store(ss.str() + SEGMENT_INFO_FILE_NAME);

        Version mergedVersion(1);
        mergedVersion.AddSegment(segCount);
        
        //using summary reader to check merge
        Check(mergedVersion, ansDocVect);
    }
    
    void MakeData(const vector<uint32_t>& docCounts, 
                  SegmentMergeInfos& segMergeInfos, Version& version,
                  DocumentMaker::DocumentVect& ansDocVect)
    {
        //used to keep summary writer, while in memory merge
        mSummaryWriters.clear();
        version.SetVersionId(0);
        size_t segCount = docCounts.size();
        docid_t baseDocId = 0;

        for (uint32_t i = 0; i < segCount; ++i)
        {
            SegmentInfo segInfo;
            segInfo.docCount = docCounts[i];
            SegmentMergeInfo segMergeInfo(i, segInfo, 0, baseDocId);
            segMergeInfos.push_back(segMergeInfo);
            
            version.AddSegment(i);

            string docStr;
            MakeSummaryDocStr(docCounts[i], segCount, docStr, baseDocId);
            DocumentMaker::DocumentVect docVect;
            DocumentMaker::MockIndexPart mockIndexPart;
            DocumentMaker::MakeDocuments(docStr, mSchema, docVect, mockIndexPart);

            SummaryWriterPtr writer(new SummaryWriterImpl());
            writer->Init(mSchema->GetSummarySchema(), NULL);
            mSummaryWriters.push_back(writer);

            for (uint32_t j = 0; j < docCounts[i]; ++j)
            {
                ansDocVect.push_back(docVect[j]);
                writer->AddDocument(docVect[j]->GetSummaryDocument());
            }

            file_system::DirectoryPtr segDirectory = MAKE_SEGMENT_DIRECTORY(i);
            file_system::DirectoryPtr summaryDirectory = segDirectory->MakeDirectory(
                    SUMMARY_DIR_NAME);
            writer->Dump(summaryDirectory);
            segInfo.Store(segDirectory);

            baseDocId += docCounts[i];
        }
        GET_FILE_SYSTEM()->Sync(true);
    }

    void Check(const Version& version, const DocumentMaker::DocumentVect& ansDocVect)
    {
        index_base::PartitionDataPtr partitionData = IndexTestUtil::CreatePartitionData(
                GET_FILE_SYSTEM(), version);
        LocalDiskSummaryReader reader(mSchema->GetSummarySchema(), 0);
        reader.Open(partitionData, PrimaryKeyIndexReaderPtr());
        
        uint32_t docCount = 0;
        for (docid_t i = 0; i < (docid_t)ansDocVect.size(); ++i)
        {
            docid_t newDocId = mReclaimMap->GetNewId(i);
            if (newDocId == INVALID_DOCID)
            {
                continue;
            }
            docCount++;
            
            const SummarySchemaPtr& summarySchema = mSchema->GetSummarySchema();
            SearchSummaryDocumentPtr summaryDoc(new SearchSummaryDocument(NULL, summarySchema->GetSummaryCount()));
            reader.GetDocument(newDocId, summaryDoc.get());
            INDEXLIB_TEST_TRUE(summaryDoc);

            SearchSummaryDocumentPtr desDoc(new SearchSummaryDocument(NULL, summarySchema->GetSummaryCount()));
            SummaryFormatter formatter(summarySchema);
            formatter.TEST_DeserializeSummaryDoc(ansDocVect[i]->GetSummaryDocument(), desDoc.get());

            summaryfieldid_t summaryFieldId = summarySchema->GetSummaryFieldId((fieldid_t)0);
            const autil::ConstString* actual = summaryDoc->GetFieldValue(summaryFieldId);
            const autil::ConstString* expect = desDoc->GetFieldValue(summaryFieldId);
            string actualStr(actual->data(), actual->size());
            string expectStr(expect->data(), expect->size());
            assert(expectStr == actualStr);
            INDEXLIB_TEST_EQUAL(actualStr, expectStr);
        }
        SearchSummaryDocumentPtr desDoc(new SearchSummaryDocument(NULL, 
                        mSchema->GetSummarySchema()->GetSummaryCount()));
        INDEXLIB_TEST_TRUE(!reader.GetDocument(docCount, desDoc.get()));
    }

    void MakeSummaryDocStr(uint32_t docCount, uint32_t segCount, 
                           string& docStr, docid_t baseDocId)
    {
        for (uint32_t j = 0; j < docCount; ++j)
        {
            stringstream ss;
            ss << "{[string1: (summary" << baseDocId++ << ")]};";
            docStr.append(ss.str());
        }
    }

private:
    string mRootDir;
    IndexPartitionSchemaPtr mSchema;
    vector<SummaryWriterPtr> mSummaryWriters;
    ReclaimMapPtr mReclaimMap;
};

INDEXLIB_UNIT_TEST_CASE(SummaryMergerTest, TestForMerge);
INDEXLIB_UNIT_TEST_CASE(SummaryMergerTest, TestForSortByWeightMerge);


IE_NAMESPACE_END(index);
