#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/summary/common_disk_summary_segment_reader.h"
#include "indexlib/index/normal/summary/test/summary_maker.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index/test/index_test_util.h"

using namespace std;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

class CommonDiskSummarySegmentReaderMock : public CommonDiskSummarySegmentReader
{
public:
    CommonDiskSummarySegmentReaderMock(const SummaryGroupConfigPtr& summaryGroupConfig,
                                       uint32_t bufferSize)
        : CommonDiskSummarySegmentReader(summaryGroupConfig)
        , mBufferSize(bufferSize)
    {
    }

    virtual uint32_t GetBufferSize() const { return mBufferSize; }    

private:
    uint32_t mBufferSize;
};

class CommonDiskSummarySegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(CommonDiskSummarySegmentReaderTest);

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

    void TestCaseForGetData()
    {
        bool isCompress = false;
        TestSegmentReader(isCompress, 2000);

        isCompress = true;
        TestSegmentReader(isCompress, 2000);
    }

private:
    void TestSegmentReader(bool compress, uint32_t docCount)
    {
        TearDown();
        SetUp();
        mIndexPartitionSchema->GetRegionSchema(DEFAULT_REGIONID)->SetSummaryCompress(compress);
        autil::mem_pool::Pool pool;
        SummaryMaker::DocumentArray answerDocArray;
        
        file_system::DirectoryPtr segDirectory = GET_SEGMENT_DIRECTORY();
        SummaryMaker::BuildOneSegment(segDirectory, 0, mIndexPartitionSchema, 
                docCount, &pool, answerDocArray);

        uint32_t bufferSize [] = {16, 1024 * 4, 1024 * 128, 1024 * 1024};

        for (uint32_t i = 0; i < sizeof(bufferSize) / sizeof(bufferSize[0]); ++i)
        {
            CommonDiskSummarySegmentReaderPtr summarySegmentReader;
            summarySegmentReader.reset(new CommonDiskSummarySegmentReaderMock(
                            mIndexPartitionSchema->GetSummarySchema()->GetSummaryGroupConfig(0),
                            bufferSize[i]));

            index_base::SegmentInfo segmentInfo;
            segmentInfo.Load(segDirectory);

            index_base::SegmentData segData = IndexTestUtil::CreateSegmentData(
                    segDirectory, segmentInfo, 0, 0);
            summarySegmentReader->Open(segData);
            CheckReadRawData(summarySegmentReader, answerDocArray);
        }
    }

    void CheckReadRawData(const CommonDiskSummarySegmentReaderPtr& reader, 
                          const SummaryMaker::DocumentArray& answerDocArray)
    {
        SummaryFormatterPtr formatter(
                new SummaryFormatter(mIndexPartitionSchema->GetSummarySchema()));

        for (size_t i = 0; i < answerDocArray.size(); ++i)
        {
            SummaryDocumentPtr answerDoc = answerDocArray[i];
            size_t expectLen = formatter->GetSerializeLength(answerDoc);
            char* expectBuffer = new char[expectLen];
            formatter->SerializeSummary(answerDoc, expectBuffer, expectLen);

            size_t actualLen = reader->GetRawDataLength(i);
            INDEXLIB_TEST_EQUAL(expectLen, actualLen);

            char* actualBuffer = new char[actualLen];
            reader->GetRawData(i, actualBuffer, actualLen);            
            assert(memcmp(expectBuffer, actualBuffer, actualLen) == 0);
            INDEXLIB_TEST_TRUE(memcmp(expectBuffer, actualBuffer, actualLen) == 0);

            delete[] expectBuffer;
            delete[] actualBuffer;
        }
    }

private:
    IndexPartitionSchemaPtr mIndexPartitionSchema;    
};

INDEXLIB_UNIT_TEST_CASE(CommonDiskSummarySegmentReaderTest, TestCaseForGetData);

IE_NAMESPACE_END(index);
