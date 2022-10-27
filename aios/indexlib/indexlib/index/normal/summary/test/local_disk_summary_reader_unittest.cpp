#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/summary/local_disk_summary_reader.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index/normal/summary/test/summary_maker.h"
#include "indexlib/file_system/test/load_config_list_creator.h"
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader_container.h"

using namespace std;
using namespace std::tr1;
using namespace autil::legacy;
using namespace autil;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);

class FakeInt32AttributeReader : public Int32AttributeReader
{
public:
    bool Read(docid_t docId, std::string& value,
              autil::mem_pool::Pool* pool = NULL) const override
    {
        stringstream ss;
        ss << docId;
        value = ss.str();
        return true;
    }
};

class FakeFloatAttributeReader : public FloatAttributeReader
{
public:
    bool Read(docid_t docId, std::string& value,
              autil::mem_pool::Pool* pool = NULL) const override
    {
        stringstream ss;
        ss << docId << ".0";
        value = ss.str();
        return true;
    }
};

class LocalDiskSummaryReaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(LocalDiskSummaryReaderTest);

public:
    typedef vector<SummaryDocumentPtr> DocumentArray;
    typedef map<fieldid_t, AttributeReaderPtr> AttrReaderMap;
    typedef tr1::shared_ptr<AttrReaderMap> AttrReaderMapPtr;

    void CaseSetUp() override
    {
        mIndexPartitionSchema.reset(new IndexPartitionSchema);
        PartitionSchemaMaker::MakeSchema(mIndexPartitionSchema,
                "title:text;user_name:string;user_id:integer;price:float", 
                "", "", "title;user_name;user_id;price");

        mUpdatableIntFieldId = 2; // user_id
        mUpdatableFloatFieldId = 3; // price
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForOneSegment()
    {
        // no compress
        TestForOneSegment(false);
        // using compress
        TestForOneSegment(true);
    }

    void TestCaseForMultiSegments()
    {
        // no compress
        TestForMultiSegments(false);
        // using compress
        TestForMultiSegments(true);
    }

    void TestCaseForOpen()
    {
        vector<uint32_t> fullBuildDocCounts;
        fullBuildDocCounts.push_back(10);
        autil::mem_pool::Pool pool;
        DocumentArray answerDocArray;
        FullBuild(fullBuildDocCounts, &pool, answerDocArray);
        AttrReaderMapPtr readerMap;
        {
            //test case for need store summary true
            LocalDiskSummaryReaderPtr summaryReader =
                DYNAMIC_POINTER_CAST(LocalDiskSummaryReader,
                        CreateSummaryReader(fullBuildDocCounts.size(), readerMap));
            INDEXLIB_TEST_EQUAL((size_t)1, summaryReader->mSegmentReaders.size());
            INDEXLIB_TEST_TRUE(summaryReader->mSegmentReaders[0]);
        }
        {
            //test case for need store summary false
            mIndexPartitionSchema->GetSummarySchema()->SetNeedStoreSummary(false);
            LocalDiskSummaryReaderPtr summaryReader =
                DYNAMIC_POINTER_CAST(LocalDiskSummaryReader,
                        CreateSummaryReader(fullBuildDocCounts.size(), readerMap));
            
            INDEXLIB_TEST_EQUAL((size_t)1, summaryReader->mSegmentReaders.size());
            ASSERT_FALSE(summaryReader->mSegmentReaders[0]);
            mIndexPartitionSchema->GetSummarySchema()->SetNeedStoreSummary(true);
        }
    }

    void TestCaseForInvalidDocId()
    {
        vector<uint32_t> fullBuildDocCounts;
        fullBuildDocCounts.push_back(10);   

        DocumentArray answerDocArray;
        autil::mem_pool::Pool pool;
        FullBuild(fullBuildDocCounts, &pool, answerDocArray);

        AttrReaderMapPtr attrReaderMap = MakeAttrReaderMap();

        SummaryReaderPtr summaryReader =
            CreateSummaryReader(fullBuildDocCounts.size(), attrReaderMap, READ_MODE_CACHE);

        SearchSummaryDocument document(NULL, 100);
        bool ret = summaryReader->GetDocument(INVALID_DOCID, &document);
        INDEXLIB_TEST_TRUE(!ret);

        ret = summaryReader->GetDocument(-100, &document);
        INDEXLIB_TEST_TRUE(!ret);

        summaryReader.reset();
        summaryReader = CreateSummaryReader(fullBuildDocCounts.size(), attrReaderMap);
        ret = summaryReader->GetDocument(INVALID_DOCID, &document);        
        INDEXLIB_TEST_TRUE(!ret);

    }

    void TestCaseForGetDocumentFromSummary()
    {
        autil::mem_pool::Pool pool;
        DocumentArray answerDocArray;

        vector<uint32_t> fullBuildDocCounts;
        fullBuildDocCounts.push_back(1);
        fullBuildDocCounts.push_back(2);
        FullBuild(fullBuildDocCounts, &pool, answerDocArray);

        AttrReaderMapPtr readerMap;
        LocalDiskSummaryReaderPtr summaryReader =
            DYNAMIC_POINTER_CAST(LocalDiskSummaryReader,
                    CreateSummaryReader(fullBuildDocCounts.size(), readerMap));
        SearchSummaryDocumentPtr summaryDoc(
                new SearchSummaryDocument(NULL, 
                        mIndexPartitionSchema->GetSummarySchema()->GetSummaryCount()));
        {
            //test for docid < 0
            ASSERT_FALSE(
                    summaryReader->GetDocumentFromSummary(
                            (docid_t)-1, summaryDoc.get()));
        }

        {
            //test for needStoreSummary = false
            summaryReader->mSummarySchema->SetNeedStoreSummary(false);
            INDEXLIB_TEST_TRUE(
                    summaryReader->GetDocumentFromSummary(
                            (docid_t)100, summaryDoc.get()));
            
            summaryReader->mSummarySchema->SetNeedStoreSummary(true);
        }

        {
            //test for get doc from on disk segment reader
            CheckSummaryReader(summaryReader, answerDocArray, AttrReaderMapPtr());
            
        }

        // TODO: add open unit test
        {
            //test for get doc from in memory segment reader
            DocumentArray inMemDocArray;
            SummaryWriterPtr writer = 
                SummaryMaker::BuildOneSegmentWithoutDump((uint32_t)1, 
                    mIndexPartitionSchema, &pool, inMemDocArray);
            InMemSummarySegmentReaderContainerPtr container = DYNAMIC_POINTER_CAST(
                    InMemSummarySegmentReaderContainer, writer->CreateInMemSegmentReader());

            BuildingSummaryReaderPtr buildingReader(new BuildingSummaryReader);
            buildingReader->AddSegmentReader(3, container->GetInMemSummarySegmentReader(0));
            summaryReader->mBuildingSummaryReader = buildingReader;
            summaryReader->mBuildingBaseDocId = 3;
            
            INDEXLIB_TEST_TRUE(summaryReader->GetDocument(0, summaryDoc.get()));
            CheckSearchDocument(summaryDoc, answerDocArray[0]);

            //test in mem doc
            INDEXLIB_TEST_TRUE(summaryReader->GetDocument(3, summaryDoc.get()));
            CheckSearchDocument(summaryDoc, inMemDocArray[0]);

            ASSERT_FALSE(summaryReader->GetDocument(4, summaryDoc.get()));
            //test for docid too large
            ASSERT_FALSE(
                    summaryReader->GetDocumentFromSummary(
                            (docid_t)100, summaryDoc.get()));
        }
        
    }

private:
    void CheckSearchDocument(
            SearchSummaryDocumentPtr gotDoc, SummaryDocumentPtr expectDoc)
    {
        INDEXLIB_TEST_EQUAL(expectDoc->GetNotEmptyFieldCount(), gotDoc->GetFieldCount());

        for (uint32_t j = 0; j < gotDoc->GetFieldCount(); ++j)
        {
            ConstString constStr = expectDoc->GetField((fieldid_t)j);
            string expectField(constStr.data(), constStr.size());

            const autil::ConstString* str = gotDoc->GetFieldValue((summaryfieldid_t)j);
            string field(str->data(), str->size());
            INDEXLIB_TEST_EQUAL(expectField, field);
        }
    }

    void FullBuild(const vector<uint32_t> &docCountInSegs,
                   autil::mem_pool::Pool *pool,
                   DocumentArray& answerDocArray)
    {
        for (size_t i = 0; i < docCountInSegs.size(); ++i)
        {
            file_system::DirectoryPtr segDirectory = MAKE_SEGMENT_DIRECTORY(i);
            SummaryMaker::BuildOneSegment(segDirectory, i, mIndexPartitionSchema, 
                    docCountInSegs[i], pool, answerDocArray);
        }
    }

    void CheckSummaryReader(const SummaryReaderPtr& summaryReader, 
                            const DocumentArray& answerDocArray,
                            AttrReaderMapPtr attrReaderMap)
    {
        for (size_t i = 0; i < answerDocArray.size(); ++i)
        {
            SummaryDocumentPtr answerDoc = answerDocArray[i];

            if (answerDoc.get() == NULL)
            {
                SearchSummaryDocumentPtr nullDoc(new SearchSummaryDocument(NULL, 
                                mIndexPartitionSchema->GetSummarySchema()->GetSummaryCount()));
                bool ret = summaryReader->GetDocument(i, nullDoc.get());
                INDEXLIB_TEST_TRUE(!ret);
                continue;
            }

            SearchSummaryDocumentPtr gotDoc(new SearchSummaryDocument(NULL, 
                    mIndexPartitionSchema->GetSummarySchema()->GetSummaryCount()));
            summaryReader->GetDocument(i, gotDoc.get());

            INDEXLIB_TEST_EQUAL(answerDoc->GetNotEmptyFieldCount(), gotDoc->GetFieldCount());

            for (uint32_t j = 0; j < gotDoc->GetFieldCount(); ++j)
            {
                ConstString constStr = answerDoc->GetField((fieldid_t)j);
                string expectField(constStr.data(), constStr.size());

                if (attrReaderMap)
                {
                    AttrReaderMap::const_iterator it = attrReaderMap->find((fieldid_t)j);
                    if (it != attrReaderMap->end())
                    {
                        it->second->Read(answerDoc->GetDocId(), expectField);
                    }
                }

                const autil::ConstString* str = gotDoc->GetFieldValue((summaryfieldid_t)j);
                string field(str->data(), str->size());
                assert(expectField == field);
                INDEXLIB_TEST_EQUAL(expectField, field);
            }
        }
    }

    bool FloatEqual(float a, float b)
    {
        return fabs(a - b) < 1E-6;
    }

    void TestOpen(const vector<uint32_t>& fullBuildDocCounts,
                  AttrReaderMapPtr attrReaderMap)
    {
        TearDown();
        SetUp();
        autil::mem_pool::Pool pool;
        DocumentArray answerDocArray;
        FullBuild(fullBuildDocCounts, &pool, answerDocArray);
        SummaryReaderPtr summaryReader =
            CreateSummaryReader(fullBuildDocCounts.size(), attrReaderMap);
        CheckSummaryReader(summaryReader, answerDocArray, attrReaderMap);
    }

    void TestForOneSegment(bool compress)
    {
        mIndexPartitionSchema->GetRegionSchema(DEFAULT_REGIONID)->SetSummaryCompress(compress);

        vector<uint32_t> docCountInSegs;
        docCountInSegs.push_back(10);
        TestOpen(docCountInSegs, AttrReaderMapPtr());

        AttrReaderMapPtr attrReaderMap = MakeAttrReaderMap();
        TestOpen(docCountInSegs, attrReaderMap);
    }

    void TestForMultiSegments(bool compress)
    {
        mIndexPartitionSchema->GetRegionSchema(DEFAULT_REGIONID)->SetSummaryCompress(compress);

        vector<uint32_t> docCountInSegs;
        docCountInSegs.push_back(10);
        docCountInSegs.push_back(5);
        docCountInSegs.push_back(15);
        docCountInSegs.push_back(27);
        TestOpen(docCountInSegs, AttrReaderMapPtr());

        AttrReaderMapPtr attrReaderMap = MakeAttrReaderMap();
        TestOpen(docCountInSegs, attrReaderMap);
    }

    SummaryReaderPtr CreateSummaryReader(uint32_t segCount, 
            AttrReaderMapPtr& attrReaderMap,
            string loadStrategyName = READ_MODE_MMAP)
    {
        if (loadStrategyName == READ_MODE_CACHE)
        {
            config::LoadConfig loadConfig = file_system::LoadConfigListCreator::
                                    MakeBlockLoadConfig(1000, 100);
            config::LoadConfigList loadConfigList;
            loadConfigList.PushBack(loadConfig);

            RESET_FILE_SYSTEM(loadConfigList);
        }

        index_base::PartitionDataPtr partitionData = 
            IndexTestUtil::CreatePartitionData(GET_FILE_SYSTEM(), segCount);
        SummaryReaderPtr summaryReader(new LocalDiskSummaryReader(
                        mIndexPartitionSchema->GetSummarySchema(), 0));
        summaryReader->Open(partitionData, PrimaryKeyIndexReaderPtr());
        AddAttrReadersToSummaryReader(attrReaderMap, summaryReader);
        return summaryReader;
    }

    AttrReaderMapPtr MakeAttrReaderMap()
    {
        AttrReaderMapPtr attrReaderMap(new AttrReaderMap());

        FakeInt32AttributeReader* int32AttrReader = new FakeInt32AttributeReader();
        AttributeReaderPtr attrReader(int32AttrReader);
        attrReaderMap->insert(make_pair(mUpdatableIntFieldId, attrReader));

        FakeFloatAttributeReader* floatAttrReader = new FakeFloatAttributeReader();
        attrReader.reset(floatAttrReader);
        attrReaderMap->insert(make_pair(mUpdatableFloatFieldId, attrReader));
        
        return attrReaderMap;
    }

    void AddAttrReadersToSummaryReader(AttrReaderMapPtr attrReaderMap, 
                                       SummaryReaderPtr& summaryReader)
    {
        if (attrReaderMap)
        {
            AttrReaderMap::const_iterator it;
            for (it = attrReaderMap->begin(); it != attrReaderMap->end(); ++it)
            {
                summaryReader->AddAttrReader(it->first, it->second);
            }
        }
    }

private:
    string mDir;
    IndexPartitionSchemaPtr mIndexPartitionSchema;
    fieldid_t mUpdatableIntFieldId;
    fieldid_t mUpdatableFloatFieldId;
};


INDEXLIB_UNIT_TEST_CASE(LocalDiskSummaryReaderTest, TestCaseForOneSegment);
INDEXLIB_UNIT_TEST_CASE(LocalDiskSummaryReaderTest, TestCaseForMultiSegments);
INDEXLIB_UNIT_TEST_CASE(LocalDiskSummaryReaderTest, TestCaseForInvalidDocId);
INDEXLIB_UNIT_TEST_CASE(LocalDiskSummaryReaderTest, TestCaseForGetDocumentFromSummary);
IE_NAMESPACE_END(index);

