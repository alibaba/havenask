#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/summary/test/summary_maker.h"
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader_container.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/index/test/partition_schema_maker.h"

using namespace std;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_BEGIN(index);

class InMemSummarySegmentReaderTest : public INDEXLIB_TESTBASE
{
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
        TestSegmentReader(false, 2000);
        // using compress
        TestSegmentReader(true, 2000);
    }

private:

    void TestSegmentReader(bool compress, uint32_t docCount)
    {
        mIndexPartitionSchema->GetRegionSchema(DEFAULT_REGIONID)->SetSummaryCompress(compress);
        SummaryMaker::DocumentArray answerDocArray;
        autil::mem_pool::Pool pool;
        SummaryWriterPtr summaryWriter = SummaryMaker::BuildOneSegmentWithoutDump(
                docCount, mIndexPartitionSchema, &pool, answerDocArray);


        InMemSummarySegmentReaderContainerPtr container = 
            std::tr1::dynamic_pointer_cast<InMemSummarySegmentReaderContainer>
            (summaryWriter->CreateInMemSegmentReader());
     
        CheckReadSummaryDocument(container->GetInMemSummarySegmentReader(0), answerDocArray);
    }

    void CheckReadSummaryDocument(const InMemSummarySegmentReaderPtr& reader, 
                                  const SummaryMaker::DocumentArray& answerDocArray)
    {
        for (size_t i = 0; i < answerDocArray.size(); ++i)
        {
            SummaryDocumentPtr answerDoc = answerDocArray[i];
            SearchSummaryDocument gotDoc(NULL, 
                    mIndexPartitionSchema->GetSummarySchema()->GetSummaryCount());
            reader->GetDocument(i, &gotDoc);

            INDEXLIB_TEST_EQUAL(answerDoc->GetNotEmptyFieldCount(), gotDoc.GetFieldCount());

            for (uint32_t j = 0; j < gotDoc.GetFieldCount(); ++j)
            {
                const autil::ConstString &expectField = answerDoc->GetField((fieldid_t)j);
                const autil::ConstString* str = gotDoc.GetFieldValue((summaryfieldid_t)j);
                INDEXLIB_TEST_EQUAL(expectField, *str);
            }
        }
    }

private:
    IndexPartitionSchemaPtr mIndexPartitionSchema;
};

INDEXLIB_UNIT_TEST_CASE(InMemSummarySegmentReaderTest, TestCaseForRead);


IE_NAMESPACE_END(index);
