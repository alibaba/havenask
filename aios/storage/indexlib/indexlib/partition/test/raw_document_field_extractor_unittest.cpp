#include "indexlib/partition/test/raw_document_field_extractor_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;

using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::index;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, RawDocumentFieldExtractorTest);

RawDocumentFieldExtractorTest::RawDocumentFieldExtractorTest() {}

RawDocumentFieldExtractorTest::~RawDocumentFieldExtractorTest() {}

void RawDocumentFieldExtractorTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    mSchema = SchemaMaker::MakeSchema(
        // Field schema
        "string0:string:false:true;string1:string;long1:uint32;long2:uint16:true;location:location",
        // Primary key index schema
        "pk:primarykey64:string1",
        // Attribute schema
        "long1;string0;string1;location",
        // Summary schema
        "long1;long2;string1");
}

void RawDocumentFieldExtractorTest::CaseTearDown() {}

void RawDocumentFieldExtractorTest::TestSimpleProcess()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
    string docString = "cmd=add,string0=hi0,string1=ha0,long1=10,long2=20 30,location=51.1 52.251.1 52.1;"
                       "cmd=add,string0=hi1,string1=ha1,long1=11,long2=21 31,location=51.2 52.2;"
                       "cmd=add,string0=hi2,string1=ha2,long1=12,long2=22 32,location=51.3 52.351.1 52.1;"
                       "cmd=add,string0=hi3,string1=ha3,long1=13,long2=23 33,location=51.4 52.4;"
                       "cmd=delete,string1=ha3;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    IndexPartitionReaderPtr indexReader = indexPart->GetReader();
    ASSERT_TRUE(indexReader);

    RawDocumentFieldExtractor extractor;
    vector<string> fieldNames;
    fieldNames.push_back("string0");
    fieldNames.push_back("string1");
    fieldNames.push_back("long1");
    fieldNames.push_back("long2");
    fieldNames.push_back("location");
    extractor.Init(indexReader, fieldNames);
    InnerCheckIterator(extractor, 0, fieldNames, "hi0,ha0,10,2030,51.1 52.251.1 52.1");
    InnerCheckIterator(extractor, 1, fieldNames, "hi1,ha1,11,2131,51.2 52.2");
    InnerCheckIterator(extractor, 2, fieldNames, "hi2,ha2,12,2232,51.3 52.351.1 52.1");
    InnerCheckIterator(extractor, 3, fieldNames, "");
}

void RawDocumentFieldExtractorTest::TestExtractFromOfflinePartition()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    ASSERT_TRUE(psm.Init(mSchema, options, mRootDir));
    options.GetBuildConfig(false).maxDocCount = 1;
    string docString = "cmd=add,string0=hi0,string1=ha0,long1=10,long2=20 30;"
                       "cmd=add,string0=hi1,string1=ha1,long1=11,long2=21 31;"
                       "cmd=add,string0=hi2,string1=ha2,long1=12,long2=22 32;"
                       "cmd=add,string0=hi3,string1=ha3,long1=13,long2=23 33;"
                       "cmd=delete,string1=ha3;"
                       "cmd=update_field,string0=hi1_new,string1=ha1,long1=111";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    OfflinePartition offlinePartition;
    options.SetIsOnline(false);
    offlinePartition.Open(mRootDir, "", mSchema, options);

    IndexPartitionReaderPtr indexReader = offlinePartition.GetReader();
    ASSERT_TRUE(indexReader);

    RawDocumentFieldExtractor extractor;
    vector<string> fieldNames;
    fieldNames.push_back("string0");
    fieldNames.push_back("string1");
    fieldNames.push_back("long1");
    fieldNames.push_back("long2");
    extractor.Init(indexReader, fieldNames);
    InnerCheckIterator(extractor, 0, fieldNames, "hi0,ha0,10,2030");
    InnerCheckIterator(extractor, 1, fieldNames, "hi1_new,ha1,111,2131");
    InnerCheckIterator(extractor, 2, fieldNames, "hi2,ha2,12,2232");
    InnerCheckIterator(extractor, 3, fieldNames, "");
}

void RawDocumentFieldExtractorTest::TestExtractFromSummary()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        // Field schema
        "string0:string;string1:string;long1:uint32;long2:uint16:true;location:location",
        // Primary key index schema
        "pk:primarykey64:string0",
        // Attribute schema
        "string0;string1",
        // Summary schema
        "long1;long2;string1;location");
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;
    ASSERT_TRUE(psm.Init(schema, options, mRootDir));
    string docString = "cmd=add,string0=hi0,string1=ha0,long1=10,long2=20 30,location=51.1 52.251.1 52.1;"
                       "cmd=add,string0=hi1,string1=ha1,long1=11,long2=21 31,location=51.2 52.251.1 52.1;"
                       "cmd=add,string0=hi2,string1=ha2,long1=12,long2=22 32,location=51.3 52.251.1 52.1;"
                       "cmd=add,string0=hi3,string1=ha3,long1=13,long2=23 33,location=51.4 52.251.1 52.1;"
                       "cmd=delete,string0=hi3;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    IndexPartitionReaderPtr indexReader = indexPart->GetReader();
    ASSERT_TRUE(indexReader);

    RawDocumentFieldExtractor extractor;
    vector<string> fieldNames;
    fieldNames.push_back("string0");
    fieldNames.push_back("string1");
    fieldNames.push_back("long1");
    fieldNames.push_back("long2");
    fieldNames.push_back("location");
    ASSERT_TRUE(extractor.Init(indexReader, fieldNames));

    typedef RawDocumentFieldExtractor::FieldInfo FieldInfo;
    vector<FieldInfo>& fieldInfos = extractor.mFieldInfos;

    ASSERT_EQ(size_t(5), fieldInfos.size());
    ASSERT_TRUE(fieldInfos[0].attrReader);  // string0 load from attribute
    ASSERT_FALSE(fieldInfos[1].attrReader); // string1 load from attribute
    ASSERT_FALSE(fieldInfos[2].attrReader); // long1 load from attribute
    ASSERT_FALSE(fieldInfos[3].attrReader); // long2 load from attribute
    ASSERT_FALSE(fieldInfos[4].attrReader);

    InnerCheckIterator(extractor, 0, fieldNames, "hi0,ha0,10,2030,51.1 52.251.1 52.1");
    InnerCheckIterator(extractor, 1, fieldNames, "hi1,ha1,11,2131,51.2 52.251.1 52.1");
    InnerCheckIterator(extractor, 2, fieldNames, "hi2,ha2,12,2232,51.3 52.251.1 52.1");
    InnerCheckIterator(extractor, 3, fieldNames, "");
}

void RawDocumentFieldExtractorTest::TestExtractFromSourceIndex()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        // Field schema
        "string0:string;string1:string;long1:uint32;long2:uint16:true;location:location",
        // Primary key index schema
        "pk:primarykey64:string0",
        // Attribute schema
        "string0;string1",
        // Summary schema
        "long1;long2;string1;location", "",
        // source schema
        "string0:string1#long1:long2:location");

    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;
    ASSERT_TRUE(psm.Init(schema, options, mRootDir));
    string docString = "cmd=add,string0=hi0,string1=ha0,long1=10,long2=20 30,location=51.1 52.251.1 52.1;"
                       "cmd=add,string0=hi1,string1=ha1,long1=11,long2=21 31,location=51.2 52.251.1 52.1;"
                       "cmd=add,string0=hi2,string1=ha2,long1=12,long2=22 32,location=51.3 52.251.1 52.1;"
                       "cmd=add,string0=hi3,string1=ha3,long1=13,long2=23 33,location=51.4 52.251.1 52.1;"
                       "cmd=delete,string0=hi3;";

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));

    OfflinePartition offlinePartition;
    options.SetIsOnline(false);
    offlinePartition.Open(mRootDir, "", schema, options);
    IndexPartitionReaderPtr indexReader = offlinePartition.GetReader();
    ASSERT_TRUE(indexReader);

    vector<string> fieldNames;
    fieldNames.push_back("string0");
    fieldNames.push_back("string1");
    fieldNames.push_back("long1");
    fieldNames.push_back("long2");
    fieldNames.push_back("location");
    RawDocumentFieldExtractor extractor;
    ASSERT_TRUE(extractor.Init(indexReader));

    typedef RawDocumentFieldExtractor::FieldInfo FieldInfo;
    vector<FieldInfo>& fieldInfos = extractor.mFieldInfos;
    ASSERT_TRUE(fieldInfos.empty());

    InnerCheckIterator(extractor, 0, fieldNames, "hi0,ha0,10,20 30,51.1 52.251.1 52.1");
    InnerCheckIterator(extractor, 1, fieldNames, "hi1,ha1,11,21 31,51.2 52.251.1 52.1");
    InnerCheckIterator(extractor, 2, fieldNames, "hi2,ha2,12,22 32,51.3 52.251.1 52.1");
    InnerCheckIterator(extractor, 3, fieldNames, "");
}

void RawDocumentFieldExtractorTest::TestExtractNullField()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        // Field schema
        "pk:string;string0:string:false:true;string1:string:true:true;long1:uint32;long2:uint16:true;",
        // Primary key index schema
        "pk:primarykey64:pk",
        // Attribute schema
        "long1;string0;string1",
        // Summary schema
        "long1;long2;string1");
    SchemaMaker::EnableNullFields(schema, "long1;long2;string0;string1");

    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;
    options.GetBuildConfig(false).enablePackageFile = false;
    ASSERT_TRUE(psm.Init(schema, options, mRootDir));
    string docString = "cmd=add,pk=1,string1=ha0,long1=10;"
                       "cmd=add,pk=2,string0=hi1,long2=21 31;"
                       "cmd=add,pk=3,string0=hi2,string1=ha2,long1=12,long2=22 32;"
                       "cmd=add,pk=4,string0=hi3,string1=ha3,long1=13,long2=23 33;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    string incStr = "cmd=update_field,pk=3,string1=__NULL__;"
                    "cmd=update_field,pk=4,long1=__NULL__;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incStr, "", ""));

    OfflinePartition offlinePartition;
    options.SetIsOnline(false);
    offlinePartition.Open(mRootDir, "", schema, options);
    IndexPartitionReaderPtr indexReader = offlinePartition.GetReader();
    ASSERT_TRUE(indexReader);

    RawDocumentFieldExtractor extractor;
    vector<string> fieldNames = {"string0", "string1", "long1", "long2"};
    ASSERT_TRUE(extractor.Init(indexReader, fieldNames));

    typedef RawDocumentFieldExtractor::FieldInfo FieldInfo;
    vector<FieldInfo>& fieldInfos = extractor.mFieldInfos;
    ASSERT_EQ(size_t(4), fieldInfos.size());
    ASSERT_TRUE(fieldInfos[0].attrReader);  // string0 load from attribute
    ASSERT_FALSE(fieldInfos[1].attrReader); // string1 load from attribute
    ASSERT_FALSE(fieldInfos[2].attrReader); // long1 load from attribute
    ASSERT_FALSE(fieldInfos[3].attrReader); // long2 load from summary

    InnerCheckIterator(extractor, 0, fieldNames, "__NULL__,ha0,10,__NULL__");
    InnerCheckIterator(extractor, 1, fieldNames, "hi1,__NULL__,__NULL__,2131");
    InnerCheckIterator(extractor, 2, fieldNames, "hi2,__NULL__,12,2232");
    InnerCheckIterator(extractor, 3, fieldNames, "hi3,ha3,__NULL__,2333");
}

void RawDocumentFieldExtractorTest::TestExtractTimeAttribute()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        // Field schema
        "pk:string;date:date;time:time;timestamp:timestamp;",
        // Primary key index schema
        "pk:primarykey64:pk",
        // Attribute schema
        "date;time;timestamp",
        // Summary schema
        "");
    SchemaMaker::EnableNullFields(schema, "date;time;timestamp");

    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig(false).maxDocCount = 1;
    options.GetBuildConfig(false).enablePackageFile = false;
    ASSERT_TRUE(psm.Init(schema, options, mRootDir));
    string docString = "cmd=add,pk=1,time=08:00:00,timestamp=1990-01-01 08:00:00;"
                       "cmd=add,pk=2,date=2000-01-01;"
                       "cmd=add,pk=3,date=2010-01-01,time=09:00:00,timestamp=2010-01-01 09:00:00;"
                       "cmd=add,pk=4,date=2020-01-01,time=09:00:00,timestamp=2020-01-01 09:00:00;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));

    string incStr = "cmd=update_field,pk=3,time=__NULL__;";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incStr, "", ""));

    OfflinePartition offlinePartition;
    options.SetIsOnline(false);
    offlinePartition.Open(mRootDir, "", schema, options);
    IndexPartitionReaderPtr indexReader = offlinePartition.GetReader();
    ASSERT_TRUE(indexReader);

    RawDocumentFieldExtractor extractor;
    vector<string> fieldNames = {"date", "time", "timestamp"};
    ASSERT_TRUE(extractor.Init(indexReader, fieldNames));

    typedef RawDocumentFieldExtractor::FieldInfo FieldInfo;
    vector<FieldInfo>& fieldInfos = extractor.mFieldInfos;
    ASSERT_EQ(size_t(3), fieldInfos.size());
    ASSERT_TRUE(fieldInfos[0].attrReader); // string0 load from attribute
    ASSERT_TRUE(fieldInfos[1].attrReader); // time load from attribute
    ASSERT_TRUE(fieldInfos[2].attrReader); // timestamp load from attribute

    InnerCheckIterator(extractor, 0, fieldNames, "__NULL__,08:00:00.000,1990-01-01 08:00:00.000");
    InnerCheckIterator(extractor, 1, fieldNames, "2000-01-01,__NULL__,__NULL__");
    InnerCheckIterator(extractor, 2, fieldNames, "2010-01-01,__NULL__,2010-01-01 09:00:00.000");
    InnerCheckIterator(extractor, 3, fieldNames, "2020-01-01,09:00:00.000,2020-01-01 09:00:00.000");
}

void RawDocumentFieldExtractorTest::TestValidateFields()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
        // Field schema
        "long0:uint32;long1:uint32;long2:uint32;long3:uint32;long4:uint32;",
        // Primary key index schema
        "pk:primarykey64:long0;long0:number:long0;long1:number:long1",
        // Attribute schema
        "long1;packAttr:long2,long3",
        // Summary schema
        "long1;long4");

    vector<string> emptyFields;
    RawDocumentFieldExtractor extractor;
    ASSERT_FALSE(extractor.ValidateFields(schema, emptyFields));

    vector<string> fieldsWithPackAttr;
    fieldsWithPackAttr.push_back("long1");
    fieldsWithPackAttr.push_back("long2");
    ASSERT_FALSE(extractor.ValidateFields(schema, fieldsWithPackAttr));

    vector<string> fieldsWithSomeInIndexOnly;
    fieldsWithSomeInIndexOnly.push_back("long1");
    fieldsWithSomeInIndexOnly.push_back("long0");
    fieldsWithSomeInIndexOnly.push_back("long4");
    ASSERT_FALSE(extractor.ValidateFields(schema, fieldsWithSomeInIndexOnly));

    vector<string> goodFields;
    goodFields.push_back("long1");
    goodFields.push_back("long4");
    ASSERT_TRUE(extractor.ValidateFields(schema, goodFields));
}

void RawDocumentFieldExtractorTest::InnerCheckIterator(RawDocumentFieldExtractor& extractor, docid_t docId,
                                                       const vector<string>& fieldNames, const string& fieldValues)
{
    if (fieldValues.empty()) {
        ASSERT_EQ(SS_DELETED, extractor.Seek(docId));
        return;
    }
    ASSERT_EQ(SS_OK, extractor.Seek(docId));

    FieldIterator iter = extractor.CreateIterator();
    vector<string> expectValues;
    StringUtil::fromString(fieldValues, expectValues, ",");

    for (size_t i = 0; i < expectValues.size(); ++i) {
        string name;
        string value;
        iter.Next(name, value);
        ASSERT_EQ(fieldNames[i], name);
        ASSERT_EQ(expectValues[i], value);
    }
    ASSERT_FALSE(iter.HasNext());
}
}} // namespace indexlib::partition
