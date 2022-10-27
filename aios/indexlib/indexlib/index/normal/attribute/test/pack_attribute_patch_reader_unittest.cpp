#include "indexlib/index/normal/attribute/test/pack_attribute_patch_reader_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PackAttributePatchReaderTest);

PackAttributePatchReaderTest::PackAttributePatchReaderTest()
{
    mBufferLen = 256 * 1024;
    mBuffer = new uint8_t[mBufferLen];
}

PackAttributePatchReaderTest::~PackAttributePatchReaderTest()
{
    ARRAY_DELETE_AND_SET_NULL(mBuffer);
}

void PackAttributePatchReaderTest::CaseSetUp()
{
    string field = "string0:string;price:int32;count:int32;pid:int32:true:true;";
    string index = "pk:primarykey64:string0";
    string attribute = "pack_attr:price,count,pid";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void PackAttributePatchReaderTest::CaseTearDown()
{
}

void PackAttributePatchReaderTest::TestSimpleProcess()
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, GET_TEST_DATA_PATH()));

    string fullDocStr = "cmd=add,string0=pk0;"
        "cmd=add,string0=pk1;"
        "cmd=add,string0=pk2;";
    
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocStr, "", ""));

    string incDocStr1 = "cmd=update_field,string0=pk0,price=10,pid=11 12;"
        "cmd=update_field,string0=pk0,price=20,count=31";
    string incDocStr2 = "cmd=update_field,string0=pk2,price=15,count=6;"
        "cmd=update_field,string0=pk1,count=5,pid=13 14";

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr1, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocStr2, "", ""));
    
    PartitionDataPtr partitionData = 
        PartitionDataMaker::CreateSimplePartitionData(GET_FILE_SYSTEM());

    PackAttributeConfigPtr packAttrConfig =
        mSchema->GetAttributeSchema()->GetPackAttributeConfig("pack_attr");
    ASSERT_TRUE(packAttrConfig);
    PackAttributePatchReaderPtr packAttrPatchReader(
            new PackAttributePatchReader(packAttrConfig));
    packAttrPatchReader->Init(partitionData, 0);
    CheckReader(packAttrPatchReader, packAttrConfig, 0, "0:20;1:31;2:11 12");
    CheckReader(packAttrPatchReader, packAttrConfig, 1, "1:5;2:13 14");
    CheckReader(packAttrPatchReader, packAttrConfig, 2, "0:15;1:6");
    ASSERT_FALSE(packAttrPatchReader->HasNext());

    CheckReaderForSeek(partitionData, packAttrConfig, 2, "0:15;1:6");
    CheckReaderForSeek(partitionData, packAttrConfig, 1, "1:5;2:13 14");
    CheckReaderForSeek(partitionData, packAttrConfig, 0, "0:20;1:31;2:11 12");
}

void PackAttributePatchReaderTest::CheckReader(
    PackAttributePatchReaderPtr& packAttrPatchReader,
    const PackAttributeConfigPtr& packAttrConfig,
    docid_t expectedDocId, const string& expectStr)
{
    docid_t docId;
    ASSERT_TRUE(packAttrPatchReader->HasNext());
    size_t dataLen = packAttrPatchReader->Next(docId, mBuffer, mBufferLen);
    ASSERT_EQ(expectedDocId, docId);
    InnerCheckPatchValue(mBuffer, dataLen, expectStr);
}

void PackAttributePatchReaderTest::CheckReaderForSeek(
    const PartitionDataPtr& partitionData,
    const PackAttributeConfigPtr& packAttrConfig,
    docid_t seekDocId, const string& expectStr)
{
    PackAttributePatchReaderPtr packAttrPatchReader(new PackAttributePatchReader(packAttrConfig));
    packAttrPatchReader->Init(partitionData, 0);
    size_t dataLen = packAttrPatchReader->Seek(seekDocId, mBuffer, mBufferLen);
    InnerCheckPatchValue(mBuffer, dataLen, expectStr);
}

void PackAttributePatchReaderTest::InnerCheckPatchValue(
    uint8_t* buffer, size_t dataLen, const string& expectStr)
{
    PackAttributeFormatter::PackAttributeFields packFields;
    ASSERT_TRUE(PackAttributeFormatter::DecodePatchValues(
                    buffer, dataLen, packFields));

    vector<vector<string> > expectValues;
    StringUtil::fromString(expectStr, expectValues, ":", ";");

    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    ASSERT_TRUE(attrSchema);

    ASSERT_EQ(expectValues.size(), packFields.size());
    for (size_t i = 0; i < expectValues.size(); ++i)
    {
        assert(expectValues[i].size() == 2);
        attrid_t attrId = StringUtil::fromString<attrid_t>(expectValues[i][0]);
        vector<int> values;
        StringUtil::fromString(expectValues[i][1], values, " ");
        const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(attrId);
        ASSERT_TRUE(attrConfig);
        if (attrConfig->IsMultiValue())
        {
            char* cursor = (char*)packFields[i].second.data();
            
            size_t encodeCountLen = 0;
            uint32_t valueCount = 
                VarNumAttributeFormatter::DecodeCount(cursor, encodeCountLen);
            cursor += encodeCountLen;
            ASSERT_EQ(values.size(), valueCount);
            for (size_t j = 0; j < valueCount; j++)
            {
                int32_t value = *(int32_t*)cursor;
                cursor += sizeof(int32_t);
                ASSERT_EQ(values[j], value);
            }
        }
        else
        {
            assert(values.size() == 1);
            ASSERT_EQ(values[0], *(int32_t*)packFields[i].second.data());
        }
    }
}


IE_NAMESPACE_END(index);

