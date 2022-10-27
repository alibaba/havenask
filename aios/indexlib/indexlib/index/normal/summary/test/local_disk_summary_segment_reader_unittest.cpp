#include <autil/mem_pool/Pool.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/summary/test/summary_maker.h"
#include "indexlib/index/normal/summary/local_disk_summary_segment_reader.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/file_system/test/load_config_list_creator.h"

IE_NAMESPACE_BEGIN(index);

using namespace std;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

class LocalDiskSummarySegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(LocalDiskSummarySegmentReaderTest);
public:
    void CaseSetUp() override
    {
        mIndexPartitionSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mIndexPartitionSchema,
                "title:text;user_name:string;user_id:integer;price:float", 
                "", "", "title;user_name;user_id;price");
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForRead()
    {
        // no compress
        TestSegmentReader(false, 2000, false);
        TestSegmentReader(false, 2000, true);
        // using compress
        TestSegmentReader(true, 2000, false);
        TestSegmentReader(true, 2000, true);
    }

private:

    void TestSegmentReader(bool compress, uint32_t docCount, bool isMmap)
    {
        TearDown();
        SetUp();
        mIndexPartitionSchema->GetRegionSchema(DEFAULT_REGIONID)->SetSummaryCompress(compress);
        SummaryMaker::DocumentArray answerDocArray;
        autil::mem_pool::Pool pool;

        file_system::DirectoryPtr segDirectory = GET_SEGMENT_DIRECTORY();
        SummaryMaker::BuildOneSegment(segDirectory, 0, 
                mIndexPartitionSchema, docCount, &pool, answerDocArray);        

        config::LoadConfigList loadConfigList;
        if (!isMmap) 
        {
            config::LoadConfig loadConfig = file_system::LoadConfigListCreator::
                                    MakeBlockLoadConfig(10, 1);
            loadConfigList.PushBack(loadConfig);
        }
        //use local load segment
        RESET_FILE_SYSTEM(loadConfigList);

        LocalDiskSummarySegmentReaderPtr summarySegmentReader(new LocalDiskSummarySegmentReader(
                        mIndexPartitionSchema->GetSummarySchema()->GetSummaryGroupConfig(0)));

        segDirectory = GET_SEGMENT_DIRECTORY();

        SegmentInfo segmentInfo;
        segmentInfo.Load(segDirectory);

        index_base::SegmentData segData = IndexTestUtil::CreateSegmentData(
                segDirectory, segmentInfo, 0, 0);

        bool ret = summarySegmentReader->Open(segData);
        INDEXLIB_TEST_TRUE(ret);

        file_system::FileNodePtr dataFileNodePtr = 
            summarySegmentReader->mDataFileReader->GetFileNode();
        file_system::FileNodePtr offsetFileNodePtr = 
            summarySegmentReader->mOffsetFileReader->GetFileNode();

        if (isMmap)
        {
            INDEXLIB_TEST_EQUAL(file_system::FSFT_MMAP, dataFileNodePtr->GetType());
            INDEXLIB_TEST_EQUAL(file_system::FSFT_MMAP, offsetFileNodePtr->GetType());
            INDEXLIB_TEST_EQUAL(file_system::FSOT_LOAD_CONFIG, 
                    summarySegmentReader->mDataFileReader->GetOpenType());
            INDEXLIB_TEST_EQUAL(file_system::FSOT_LOAD_CONFIG, 
                    summarySegmentReader->mOffsetFileReader->GetOpenType());
        }
        else
        {
            INDEXLIB_TEST_EQUAL(file_system::FSFT_BLOCK, dataFileNodePtr->GetType());
            INDEXLIB_TEST_EQUAL(file_system::FSFT_IN_MEM, offsetFileNodePtr->GetType());
            INDEXLIB_TEST_EQUAL(file_system::FSOT_LOAD_CONFIG, 
                    summarySegmentReader->mDataFileReader->GetOpenType());
            INDEXLIB_TEST_EQUAL(file_system::FSOT_IN_MEM, 
                    summarySegmentReader->mOffsetFileReader->GetOpenType());
        }
        CheckReadSummaryDocument(summarySegmentReader, answerDocArray);
    }

    void CheckReadSummaryDocument(const LocalDiskSummarySegmentReaderPtr& reader, 
                                  const SummaryMaker::DocumentArray& answerDocArray)
    {
        for (size_t i = 0; i < answerDocArray.size(); ++i)
        {
            SummaryDocumentPtr answerDoc = answerDocArray[i];
            const SummarySchemaPtr& summarySchema = mIndexPartitionSchema->GetSummarySchema();
            SearchSummaryDocumentPtr gotDoc(new SearchSummaryDocument(NULL, 
                    summarySchema->GetSummaryCount()));
            reader->GetDocument(i, gotDoc.get());

            INDEXLIB_TEST_EQUAL(answerDoc->GetNotEmptyFieldCount(), gotDoc->GetFieldCount());

            for (uint32_t j = 0; j < answerDoc->GetNotEmptyFieldCount(); ++j)
            {
                const autil::ConstString &expectField = answerDoc->GetField((fieldid_t)j);
                summaryfieldid_t summaryFieldId = summarySchema->GetSummaryFieldId((fieldid_t)j);
                const autil::ConstString* str = gotDoc->GetFieldValue(summaryFieldId);
                INDEXLIB_TEST_TRUE(str != NULL);
                INDEXLIB_TEST_EQUAL(expectField, *str);
            }
        }
    }

private:
    IndexPartitionSchemaPtr mIndexPartitionSchema;
    autil::mem_pool::Pool mPool;
};

INDEXLIB_UNIT_TEST_CASE(LocalDiskSummarySegmentReaderTest, TestCaseForRead);


IE_NAMESPACE_END(index);
