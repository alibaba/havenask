#include "indexlib/index/normal/primarykey/test/primary_key_index_merger_typed_unittest.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(index);

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64WithDelete);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128WithDelete);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64PKAttr);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt64WithDeletePKAttr);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128PKAttr);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexMergerTypedTest, TestCaseForMergeUInt128WithDeletePKAttr);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexMergerTypedTest, TestMergeFromIncontinuousSegments);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexMergerTypedTest, TestMergeToSegmentWithoutDoc);

void PrimaryKeyIndexMergerTypedTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void PrimaryKeyIndexMergerTypedTest::CaseTearDown()
{
}

void PrimaryKeyIndexMergerTypedTest::TestCaseForMergeUInt64()
{
    TestMerge<uint64_t>(IndexTestUtil::NoDelete, false);
}

void PrimaryKeyIndexMergerTypedTest::TestCaseForMergeUInt64WithDelete()
{
    TestMerge<uint64_t>(IndexTestUtil::DeleteEven, false);
}

void  PrimaryKeyIndexMergerTypedTest::TestCaseForMergeUInt128()
{
    TestMerge<autil::uint128_t>(IndexTestUtil::NoDelete, false);
}

void  PrimaryKeyIndexMergerTypedTest::TestCaseForMergeUInt128WithDelete()
{
    TestMerge<autil::uint128_t>(IndexTestUtil::DeleteEven, false);
}

void  PrimaryKeyIndexMergerTypedTest::TestCaseForMergeUInt64PKAttr()
{
    TestMerge<uint64_t>(IndexTestUtil::NoDelete, true);
}

void  PrimaryKeyIndexMergerTypedTest::TestCaseForMergeUInt64WithDeletePKAttr()
{
    TestMerge<uint64_t>(IndexTestUtil::DeleteEven, true);
}

void  PrimaryKeyIndexMergerTypedTest::TestCaseForMergeUInt128PKAttr()
{
    TestMerge<autil::uint128_t>(IndexTestUtil::NoDelete, true);
}

void  PrimaryKeyIndexMergerTypedTest::TestCaseForMergeUInt128WithDeletePKAttr()
{
    TestMerge<autil::uint128_t>(IndexTestUtil::DeleteEven, true);
}

void PrimaryKeyIndexMergerTypedTest::TestMergeFromIncontinuousSegments()
{
    string field = "string1:string";
    string index = "pk:primarykey64:string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    PrimaryKeyIndexConfigPtr pkIndexConfig = DYNAMIC_POINTER_CAST(
        PrimaryKeyIndexConfig, indexSchema->GetIndexConfig("pk"));
    pkIndexConfig->SetPrimaryKeyIndexType(GetPrimaryKeyIndexType());

    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetOfflineConfig().mergeConfig.mergeStrategyStr = "optimize";
    options.GetOfflineConfig().mergeConfig.mergeStrategyParameter.SetLegacyString(
            "max-doc-count=2");
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    string docString = "cmd=add,string1=pk1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    docString = "cmd=add,string1=pk2;"
                "cmd=add,string1=pk3;"
                "cmd=add,string1=pk4;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, docString, "", ""));

    docString = "cmd=add,string1=pk5;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1", "docid=3"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk2", "docid=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk3", "docid=1"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk4", "docid=2"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk5", "docid=4"));
}

void PrimaryKeyIndexMergerTypedTest::TestMergeToSegmentWithoutDoc()
{
    string field = "string1:string";
    string index = "pk:primarykey64:string1";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "", "");
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    PrimaryKeyIndexConfigPtr pkIndexConfig = DYNAMIC_POINTER_CAST(
        PrimaryKeyIndexConfig, indexSchema->GetIndexConfig("pk"));
    pkIndexConfig->SetPrimaryKeyIndexType(GetPrimaryKeyIndexType());

    PartitionStateMachine psm;
    IndexPartitionOptions options;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    string docString = "cmd=add,string1=pk1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    docString = "cmd=delete,string1=pk1;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, docString, "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1", ""));
}

IndexConfigPtr PrimaryKeyIndexMergerTypedTest::CreateIndexConfig(
        uint64_t key, const std::string& indexName, bool hasPKAttribute)
{
    return PrimaryKeyTestCaseHelper::CreateIndexConfig<uint64_t>("pk",
            GetPrimaryKeyIndexType(), hasPKAttribute);
}

IndexConfigPtr PrimaryKeyIndexMergerTypedTest::CreateIndexConfig(
        autil::uint128_t key, const std::string& indexName, bool hasPKAttribute)
{
    return PrimaryKeyTestCaseHelper::CreateIndexConfig<autil::uint128_t>(
            "pk", GetPrimaryKeyIndexType(), hasPKAttribute);
}

ReclaimMapPtr PrimaryKeyIndexMergerTypedTest::CreateReclaimMap(
        const SegmentMergeInfos& segMergeInfos, 
        const DeletionMapReaderPtr& deletionMapReader)
{
        
    ReclaimMapPtr reclaimMap(new ReclaimMap());

    OfflineAttributeSegmentReaderContainerPtr readerContainer;
    reclaimMap->Init(segMergeInfos, deletionMapReader, readerContainer, false);
    return reclaimMap;
}

DeletionMapReaderPtr PrimaryKeyIndexMergerTypedTest::CreateDeletionMap(
    const merger::SegmentDirectoryPtr& segDir, const SegmentInfos& segInfos,
    IndexTestUtil::ToDelete toDel)
{
    vector<uint32_t> docCounts;
    for (size_t i = 0; i < segInfos.size(); ++i)
    {
        docCounts.push_back((uint32_t)segInfos[i].docCount);
    }
    return IndexTestUtil::CreateDeletionMap(segDir->GetVersion(),
            docCounts, toDel);
}

void PrimaryKeyIndexMergerTypedTest::CreateIndexDocuments(
        autil::mem_pool::Pool* pool, const string& docsStr,
        vector<IndexDocumentPtr>& docs)
{
    StringTokenizer st(docsStr, ",", 
                       StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);

    for (size_t i = 0; i < st.getNumTokens(); ++i)
    {
        StringTokenizer st2(st[i], ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                            | StringTokenizer::TOKEN_TRIM);
        assert(st2.getNumTokens() == 2);

        IndexDocumentPtr doc(new IndexDocument(pool));
        doc->SetPrimaryKey(st2[0]);

        docid_t docId;
        StringUtil::strToUInt32(st2[1].c_str(), (uint32_t&)docId);
        doc->SetDocId(docId);
        docs.push_back(doc);
    }
}
    
IE_NAMESPACE_END(index);
