#include "indexlib/partition/test/online_partition_reader_intetest.h"

#include "autil/StringUtil.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference_typed.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/config/disable_fields_config.h"
#include "indexlib/config/test/modify_schema_maker.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/index/inverted_index/InDocSectionMeta.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/index/normal/summary/summary_reader_impl.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::index_base;
using namespace indexlib::test;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OnlinePartitionReaderInteTest);

template <typename T>
void CheckAttributeReferenceForSingleValue(const PackAttributeReaderPtr& packAttrReader, const string& attrName,
                                           docid_t docId, const string& expectValueStr)
{
    ASSERT_TRUE(packAttrReader);
    AttributeReferenceTyped<T>* attrRef = packAttrReader->GetSubAttributeReferenceTyped<T>(attrName);
    ASSERT_TRUE(attrRef);
    const char* baseAddr = packAttrReader->GetBaseAddress(docId, nullptr);
    ASSERT_TRUE(baseAddr);
    T value;
    attrRef->GetValue(baseAddr, value);
    T expectValue = StringUtil::numberFromString<T>(expectValueStr);
    ASSERT_EQ(expectValue, value);
}

template <typename T>
void CheckAttributeReferenceForMultiValue(const PackAttributeReaderPtr& packAttrReader, const string& attrName,
                                          docid_t docId, const string& expectValueStr)
{
    ASSERT_TRUE(packAttrReader);
    AttributeReferenceTyped<MultiValueType<T>>* attrRef =
        packAttrReader->GetSubAttributeReferenceTyped<MultiValueType<T>>(attrName);
    ASSERT_TRUE(attrRef);
    const char* baseAddr = packAttrReader->GetBaseAddress(docId, nullptr);
    ASSERT_TRUE(baseAddr);
    MultiValueType<T> value;
    attrRef->GetValue(baseAddr, value);

    vector<T> expectValues;
    StringUtil::fromString(expectValueStr, expectValues, " ");

    ASSERT_EQ(expectValues.size(), value.size());
    for (size_t i = 0; i < expectValues.size(); ++i) {
        ASSERT_EQ(expectValues[i], value[i]);
    }
}

OnlinePartitionReaderInteTest::OnlinePartitionReaderInteTest() {}

OnlinePartitionReaderInteTest::~OnlinePartitionReaderInteTest() {}

void OnlinePartitionReaderInteTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    mSchema = SchemaMaker::MakeSchema("pk:string", "pk:primarykey64:pk;", "", "");
    mOptions = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
}

void OnlinePartitionReaderInteTest::CaseTearDown() {}

void OnlinePartitionReaderInteTest::TestOpenForIndex()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("string1:string", "index1:string:string1;", "", "");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    string fullDoc = "cmd=add,string1=hello,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "index1:hello", "docid=0"));
    string rtDoc = "cmd=add,string1=hello2,ts=2;"
                   "cmd=add,string1=hello,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "index1:hello", "docid=0;docid=2"));

    // check IndexAccessCounters
    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    const IndexPartitionReader::AccessCounterMap& counterMap = indexPart->GetReader()->GetIndexAccessCounters();
    IndexPartitionReader::AccessCounterMap::const_iterator iter = counterMap.find("index1");
    ASSERT_EQ((int)2, iter->second->Get());
}

void OnlinePartitionReaderInteTest::TestOpenForCompressIndexWithSection()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("text1:text", "pack1:pack:text1;", "", "");
    string compressorStr = "compressor:zstd;";
    string indexCompressStr = "pack1:compressor";
    schema = SchemaMaker::EnableFileCompressSchema(schema, compressorStr, indexCompressStr, "");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    string fullDoc = "cmd=add,text1=hello t2,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "pack1:hello", "docid=0"));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    AssertSectionLen(indexPart->GetReader(), "pack1", 0, 3);
    string rtDoc = "cmd=add,text1=hello2,ts=2;"
                   "cmd=add,text1=hello t2 t3,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "pack1:hello", "docid=0;docid=2"));
    AssertSectionLen(indexPart->GetReader(), "pack1", 2, 4);
}

void OnlinePartitionReaderInteTest::TestOpenForIndexWithSection()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("text1:text", "pack1:pack:text1;", "", "");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    string fullDoc = "cmd=add,text1=hello t2,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "pack1:hello", "docid=0"));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    AssertSectionLen(indexPart->GetReader(), "pack1", 0, 3);
    string rtDoc = "cmd=add,text1=hello2,ts=2;"
                   "cmd=add,text1=hello t2 t3,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "pack1:hello", "docid=0;docid=2"));
    AssertSectionLen(indexPart->GetReader(), "pack1", 2, 4);
}

void OnlinePartitionReaderInteTest::TestOpenForPrimaryKey()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("string1:string", "pk:primarykey64:string1;", "", "");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    string fullDoc = "cmd=add,string1=hello,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "pk:hello", "docid=0"));
    string rtDoc = "cmd=add,string1=hello2,ts=2;"
                   "cmd=add,string1=hello,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "pk:hello", "docid=2"));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    ASSERT_TRUE(indexPart->GetReader()->GetPrimaryKeyReader());
    // check partition info
    ASSERT_EQ((docid_t)3, indexPart->GetReader()->GetPartitionInfo()->GetTotalDocCount());
}

void OnlinePartitionReaderInteTest::TestOpenForAttribute()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("string1:string;long1:uint32;", "index1:string:string1;", "long1;", "");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    string fullDoc = "cmd=add,string1=hello,long1=1,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "index1:hello", "long1=1"));
    string rtDoc = "cmd=add,string1=hello,long1=2,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "index1:hello", "long1=1;long1=2"));

    // check AttributeAccessCounters
    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    const IndexPartitionReader::AccessCounterMap& counterMap = indexPart->GetReader()->GetAttributeAccessCounters();
    IndexPartitionReader::AccessCounterMap::const_iterator iter = counterMap.find("long1");
    ASSERT_EQ((int)3, iter->second->Get());
}

void OnlinePartitionReaderInteTest::TestOpenForPackAttribute()
{
    IndexPartitionSchemaPtr schema =
        SchemaAdapter::TEST_LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/main_schema_with_pack.json");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));

    string fullDoc = "cmd=add,int8_single=0,int8_multi=0 1,str_single=pk;"
                     "cmd=add,int8_single=1,int8_multi=1 2,str_single=pk;";

    string rtDoc = "cmd=add,int8_single=2,int8_multi=2 3,str_single=pk1,ts=1;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc, "pk:pk", "int8_single=1,int8_multi=1 2"));
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "pk:pk1", "int8_single=2,int8_multi=2 3"));

    // check AttributeAccessCounters
    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    const IndexPartitionReaderPtr& partReader = indexPart->GetReader();
    ASSERT_TRUE(partReader);
    const IndexPartitionReader::AccessCounterMap& counterMap = partReader->GetAttributeAccessCounters();
    IndexPartitionReader::AccessCounterMap::const_iterator iter1 = counterMap.find("int8_single");
    ASSERT_EQ((int)2, iter1->second->Get());
    IndexPartitionReader::AccessCounterMap::const_iterator iter2 = counterMap.find("int8_multi");
    ASSERT_EQ((int)2, iter2->second->Get());

    const PackAttributeReaderPtr& packAttrReader = partReader->GetPackAttributeReader("pack_attr");

    const PackAttributeReaderPtr& packAttrReaderCmp = partReader->GetPackAttributeReader(packattrid_t(0));

    ASSERT_TRUE(packAttrReader);
    ASSERT_TRUE(packAttrReaderCmp);
    ASSERT_EQ(packAttrReader.get(), packAttrReaderCmp.get());

    CheckAttributeReferenceForSingleValue<int8_t>(packAttrReader, "int8_single", docid_t(0), "0");
    CheckAttributeReferenceForSingleValue<int8_t>(packAttrReader, "int8_single", docid_t(1), "1");
    CheckAttributeReferenceForSingleValue<int8_t>(packAttrReader, "int8_single", docid_t(2), "2");

    // check access counter
    ASSERT_EQ((int)5, iter1->second->Get());
    ASSERT_EQ((int)2, iter2->second->Get());

    CheckAttributeReferenceForMultiValue<int8_t>(packAttrReader, "int8_multi", docid_t(0), "0 1");
    CheckAttributeReferenceForMultiValue<int8_t>(packAttrReader, "int8_multi", docid_t(1), "1 2");
    CheckAttributeReferenceForMultiValue<int8_t>(packAttrReader, "int8_multi", docid_t(2), "2 3");

    // check access counter
    ASSERT_EQ((int)5, iter1->second->Get());
    ASSERT_EQ((int)5, iter2->second->Get());

    CheckReadSubAttribute(packAttrReader, "int8_single", docid_t(0), "0");
    CheckReadSubAttribute(packAttrReader, "int8_single", docid_t(1), "1");
    CheckReadSubAttribute(packAttrReader, "int8_single", docid_t(2), "2");

    CheckReadSubAttribute(packAttrReader, "int8_multi", docid_t(0), "01");
    CheckReadSubAttribute(packAttrReader, "int8_multi", docid_t(1), "12");
    CheckReadSubAttribute(packAttrReader, "int8_multi", docid_t(2), "23");
}

void OnlinePartitionReaderInteTest::CheckReadSubAttribute(const PackAttributeReaderPtr& packAttrReader,
                                                          const string& attrName, docid_t docId,
                                                          const string& expectedValue)
{
    autil::mem_pool::Pool pool;
    string attrValue;
    ASSERT_TRUE(packAttrReader);
    ASSERT_TRUE(packAttrReader->Read(docId, attrName, attrValue, &pool));
    ASSERT_EQ(expectedValue, attrValue);
}

void OnlinePartitionReaderInteTest::TestOpenForSummary()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("string1:string;long1:uint32;", "index1:string:string1;", "", "long1");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    string fullDoc = "cmd=add,string1=hello,long1=1,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "index1:hello", "long1=1"));
    string rtDoc = "cmd=add,string1=hello,long1=2,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "index1:hello", "long1=1;long1=2"));
}

void OnlinePartitionReaderInteTest::TestOpenForSummaryWithPack()
{
    IndexPartitionSchemaPtr schema = SchemaAdapter::TEST_LoadSchema(
        GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/schema_for_online_partition_reader_test.json");

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    string fullDoc = "cmd=add,string1=hello,long1=1,long2=1,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "", ""));
    string rtDoc = "cmd=add,string1=hello,long1=2,long2=1,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    IndexPartitionReaderPtr partReader = indexPart->GetReader();
    ASSERT_TRUE(partReader);
    SummaryReaderPtr summaryReader = partReader->GetSummaryReader();
    ASSERT_TRUE(summaryReader);

    SearchSummaryDocument summaryDoc(NULL, 4096);
    ASSERT_TRUE(summaryReader->GetDocument(docid_t(0), &summaryDoc));
    const StringView* field = summaryDoc.GetFieldValue(0);
    ASSERT_EQ(string("1"), field->to_string());

    ASSERT_TRUE(summaryReader->GetDocument(docid_t(1), &summaryDoc));
    field = summaryDoc.GetFieldValue(0);
    ASSERT_EQ(string("2"), field->to_string());
}

void OnlinePartitionReaderInteTest::TestSubDocOpenForIndex()
{
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        "sub_string:string;sub_pk:string", "sub_index:string:sub_string;sub_pk:primarykey64:sub_pk", "", "");
    mSchema->SetSubIndexPartitionSchema(subSchema);

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string fullDoc = "cmd=add,pk=1,sub_pk=11^12,sub_string=abc^def,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "sub_index:abc", "docid=0"));
    string rtDoc = "cmd=add,pk=2,sub_pk=13^14,sub_string=def^ghi,ts=2;"
                   "cmd=add,pk=3,sub_pk=15^16,sub_string=def^abc,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "sub_index:abc", "docid=0;docid=5"));
}

void OnlinePartitionReaderInteTest::TestSubDocOpenForIndexWithSection()
{
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        "sub_text:text;sub_pk:string", "sub_pack:pack:sub_text;sub_pk:primarykey64:sub_pk", "", "");
    mSchema->SetSubIndexPartitionSchema(subSchema);

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string fullDoc = "cmd=add,pk=1,sub_pk=11^12,sub_text=abc 123^def,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "sub_pack:abc", "docid=0"));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    AssertSectionLen(indexPart->GetReader()->GetSubPartitionReader(), "sub_pack", 0, 3);

    string rtDoc = "cmd=add,pk=2,sub_pk=13^14,sub_text=def^ghi,ts=2;"
                   "cmd=add,pk=3,sub_pk=15^16,sub_text=def^abc 123 def,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "sub_pack:abc", "docid=0;docid=5"));
    AssertSectionLen(indexPart->GetReader()->GetSubPartitionReader(), "sub_pack", 5, 4);
}

void OnlinePartitionReaderInteTest::TestSubDocOpenForPrimaryKey()
{
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema("sub_pk:string", "sub_pk:primarykey64:sub_pk", "", "");
    mSchema->SetSubIndexPartitionSchema(subSchema);

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string fullDoc = "cmd=add,pk=1,sub_pk=11^12,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "sub_pk:11", "docid=0"));
    string rtDoc = "cmd=add,pk=2,sub_pk=13^14,ts=2;"
                   "cmd=add,pk=1,sub_pk=11^12,ts=3;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "sub_pk:11", "docid=4"));
    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    ASSERT_TRUE(indexPart->GetReader()->GetSubPartitionReader()->GetPrimaryKeyReader());
    // check partition info
    ASSERT_EQ((docid_t)6, indexPart->GetReader()->GetPartitionInfo()->GetSubPartitionInfo()->GetTotalDocCount());
}

void OnlinePartitionReaderInteTest::TestSubDocOpenForAttribute()
{
    IndexPartitionSchemaPtr subSchema =
        SchemaMaker::MakeSchema("sub_long:uint32;sub_pk:string", "sub_pk:primarykey64:sub_pk", "sub_long", "");
    mSchema->SetSubIndexPartitionSchema(subSchema);

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));

    string fullDoc = "cmd=add,pk=1,sub_pk=11^12,sub_long=11^12,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "sub_pk:11", "sub_long=11"));
    string rtDoc = "cmd=add,pk=2,sub_pk=13^14,sub_long=13^14,ts=2;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "sub_pk:13", "sub_long=13"));
}

void OnlinePartitionReaderInteTest::AssertSectionLen(const IndexPartitionReaderPtr& reader, const string& indexName,
                                                     docid_t docid, section_len_t expectLen)
{
    const SectionAttributeReader* sectionReader =
        reader->GetInvertedIndexReader(indexName)->GetSectionReader(indexName);
    ASSERT_TRUE(sectionReader);
    InDocSectionMetaPtr meta = sectionReader->GetSection(docid);
    ASSERT_TRUE(meta);
    ASSERT_EQ(expectLen, meta->GetSectionLen(0, 0));
}

void OnlinePartitionReaderInteTest::TestDisableIndex()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("string1:string;long1:uint32;long2:uint32",
                                                             "index1:string:string1;index2:number:long1", "long2", "");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    string fullDoc = "cmd=add,string1=hello,long1=1,long2=11;"
                     "cmd=add,string1=hello2,long1=2,long2=22";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "index1:hello", "docid=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:1", "docid=0"));
    string rtDoc = "cmd=add,string1=hello3,long1=3,long2=33";

    psm.GetSchema()->GetIndexSchema()->DisableIndex("index2");
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "index1:hello3", "docid=2"));

    // reopen
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:1", ""));

    // check IndexAccessCounters
    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    const IndexPartitionReader::AccessCounterMap& counterMap = indexPart->GetReader()->GetIndexAccessCounters();
    IndexPartitionReader::AccessCounterMap::const_iterator iter = counterMap.find("index1");
    ASSERT_EQ((int)2, iter->second->Get());
    iter = counterMap.find("index2");
    ASSERT_EQ((int)1, iter->second->Get());
}

void OnlinePartitionReaderInteTest::TestDisableSubIndex()
{
    string field = "pk:string;long1:int32;long2:int32;multi_int:int32:true:true;";
    string index = "pk:primarykey64:pk;";
    string attr = "long1;packAttr1:long2,multi_int";
    string summary = "";
    auto schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        "sub_pk:string;sub_int:int32;sub_multi_int:int32:true:true",
        "sub_pk:primarykey64:sub_pk;sub_int:number:sub_int", "sub_pk;sub_int;sub_multi_int;", "");
    schema->SetSubIndexPartitionSchema(subSchema);

    string fullDocs = "cmd=add,pk=pk1,long1=1,multi_int=1 2 3,long2=1,"
                      "sub_pk=sub11^sub12,sub_int=1^2,sub_multi_int=11 12^12 13;"
                      "cmd=add,pk=pk2,long1=2,multi_int=2 2 3,long2=2,"
                      "sub_pk=sub21^sub22,sub_int=2^3,sub_multi_int=21 22^22 23;";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "sub_int:2", "sub_pk=sub12;sub_pk=sub21"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1", "long1=1;"));

    // disable fields
    psm.GetSchema()->GetSubIndexPartitionSchema()->GetIndexSchema()->DisableIndex("sub_int");
    psm.GetSchema()->GetSubIndexPartitionSchema()->GetAttributeSchema()->DisableAttribute("long1");

    string incDocs = "cmd=add,pk=pk3,long1=3,multi_int=4 2 3,long2=3,"
                     "sub_pk=sub31^sub32,sub_int=4^5,sub_multi_int=31 32^32 33;";

    // cannot search sub_int
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "sub_int:2", ""));
}

void OnlinePartitionReaderInteTest::TestDisableSubIndexByConfig()
{
    string field = "pk:string;long1:int32;long2:int32;multi_int:int32:true:true;";
    string index = "pk:primarykey64:pk;";
    string attr = "long1;packAttr1:long2,multi_int";
    string summary = "";
    auto schema = SchemaMaker::MakeSchema(field, index, attr, summary);
    IndexPartitionSchemaPtr subSchema = SchemaMaker::MakeSchema(
        "sub_pk:string;sub_int:int32;sub_multi_int:int32:true:true",
        "sub_pk:primarykey64:sub_pk;sub_int:number:sub_int", "sub_pk;sub_int;sub_multi_int;", "");
    schema->SetSubIndexPartitionSchema(subSchema);
    mOptions.GetOnlineConfig().GetDisableFieldsConfig().indexes.push_back("sub_int");

    string fullDocs = "cmd=add,pk=pk1,long1=1,multi_int=1 2 3,long2=1,"
                      "sub_pk=sub11^sub12,sub_int=1^2,sub_multi_int=11 12^12 13;"
                      "cmd=add,pk=pk2,long1=2,multi_int=2 2 3,long2=2,"
                      "sub_pk=sub21^sub22,sub_int=2^3,sub_multi_int=21 22^22 23;";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDocs, "sub_int:2", ""));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "pk:pk1", "long1=1;"));

    string incDocs = "cmd=add,pk=pk3,long1=3,multi_int=4 2 3,long2=3,"
                     "sub_pk=sub31^sub32,sub_int=4^5,sub_multi_int=31 32^32 33;";

    // cannot search sub_int
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDocs, "sub_int:2", ""));
}

void OnlinePartitionReaderInteTest::TestDisableSpatialIndexMode1()
{
    // mode 1 using set OngoingModifyOperationIds
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("string1:string;long1:uint32;long2:uint32",
                                                             "index1:string:string1;index2:number:long1", "long2", "");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    string fullDoc = "cmd=add,string1=hello,long1=1,long2=11;"
                     "cmd=add,string1=hello2,long1=2,long2=22";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "index1:hello", "docid=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:1", "docid=0"));
    IndexPartitionSchemaPtr newSchema;
    newSchema.reset(schema->Clone());
    ModifySchemaMaker::AddModifyOperations(newSchema, "", "coordinate:location", "spatial_index:spatial:coordinate",
                                           "");

    IndexPartitionOptions options = util::BuildTestUtil::GetIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam());
    options.SetOngoingModifyOperationIds({1});
    string incDoc = "cmd=add,string1=world,long1=10,long2=12,coordinate=45 30;";
    PartitionStateMachine newPsm;
    INDEXLIB_TEST_TRUE(newPsm.Init(newSchema, options, mRootDir));
    ASSERT_TRUE(newPsm.Transfer(PE_BUILD_INC, incDoc, "", ""));
    ASSERT_TRUE(newPsm.Transfer(QUERY, "", "spatial_index:rectangle(45 0, 90 40)", ""));
}

void OnlinePartitionReaderInteTest::TestDisableSortField()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema("string1:string;long1:uint32;long2:uint32",
                                                             "index1:string:string1;index2:number:long1", "long2", "");
    PartitionStateMachine psm;
    mOptions.SetIsOnline(true);
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    PartitionMeta partMeta;
    partMeta.AddSortDescription("long2", indexlibv2::config::sp_desc);
    partMeta.Store(GET_PARTITION_DIRECTORY());

    string fullDoc = "cmd=add,string1=hello,long1=1,long2=22;"
                     "cmd=add,string1=hello2,long1=2,long2=11";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "index1:hello", "docid=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "index2:1", "docid=0"));

    psm.GetSchema()->GetAttributeSchema()->DisableAttribute("long2");
    PartitionStateMachine psm2;
    string incDoc = "cmd=add,string1=hello,long1=1,long2=22;"
                    "cmd=add,string1=hello2,long1=2,long2=11";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "", ""));
}

void OnlinePartitionReaderInteTest::TestDisableSpatialIndexMode2()
{
    // mode 2 using disable schema
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        "string1:string;long1:uint32;long2:uint32;coordinate:location",
        "index1:string:string1;index2:number:long1;spatial_index:spatial:coordinate", "long2", "");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, mOptions, mRootDir));
    string fullDoc = "cmd=add,string1=hello,long1=1,long2=11,coordinate=45 30;"
                     "cmd=add,string1=hello2,long1=2,long2=22, coordinate=45 30";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL, fullDoc, "index1:hello", "docid=0"));
    ASSERT_TRUE(psm.Transfer(QUERY, "", "spatial_index:rectangle(45 0, 90 40)", "docid=0;docid=1"));

    psm.GetSchema()->GetIndexSchema()->DisableIndex("spatial_index");
    string incDoc = "cmd=add,string1=world,long1=10,long2=12,coordinate=45 30;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC, incDoc, "spatial_index:rectangle(45 0, 90 40)", ""));
}
}} // namespace indexlib::partition
