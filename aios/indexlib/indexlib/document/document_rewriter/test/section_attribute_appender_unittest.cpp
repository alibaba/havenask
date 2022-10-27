#include "indexlib/document/document_rewriter/test/section_attribute_appender_unittest.h"
#include "indexlib/common/field_format/section_attribute/section_attribute_formatter.h"
#include "indexlib/common/field_format/attribute/string_attribute_convertor.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, SectionAttributeAppenderTest);

SectionAttributeAppenderTest::SectionAttributeAppenderTest()
{
}

SectionAttributeAppenderTest::~SectionAttributeAppenderTest()
{
}

void SectionAttributeAppenderTest::CaseSetUp()
{
    mPool = new Pool;
    ResetSchema();
}

void SectionAttributeAppenderTest::CaseTearDown()
{
    delete mPool;
    mPool = NULL;
}

void SectionAttributeAppenderTest::TestAppendSectionAttrWithDifferentParam()
{
    SCOPED_TRACE("Failed");
    InnerTestAppendSectionAttribute("3,4,5", "", false, true, true);
    InnerTestAppendSectionAttribute("3,4,5", "", true, false, true);
    InnerTestAppendSectionAttribute("3,4,5", "", true, true, false);
    InnerTestAppendSectionAttribute("3,4,5", "", false, false, false);
    InnerTestAppendSectionAttribute("3,4,0", "", true, true, true);
    InnerTestAppendSectionAttribute("3,4,5", "index0", true, true, true);
    InnerTestAppendSectionAttribute("3,4,5", "index0,index1", true, true, true);
}

void SectionAttributeAppenderTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    // test append section attribute with default schema
    string despStr = "3,4,5";
    ResetSchema();
    OverwriteSchema(mSchema, "", true, true, true);
    IndexDocumentPtr indexDocument = CreateIndexDocument(despStr);
    SectionAttributeAppender sectionAttrAppender;
    ASSERT_TRUE(sectionAttrAppender.Init(mSchema));
    ASSERT_TRUE(sectionAttrAppender.AppendSectionAttribute(indexDocument));
    InnerCheckDocument(indexDocument, despStr);
    
    // test append section attribute to a document already has it
    ASSERT_FALSE(sectionAttrAppender.AppendSectionAttribute(indexDocument));

    // test cloned SectionAttributeAppender
    despStr = "5,6,7";
    OverwriteSchema(mSchema, "", true, true, true);
    indexDocument = CreateIndexDocument(despStr);
    SectionAttributeAppenderPtr cloneAppender(sectionAttrAppender.Clone());
    ASSERT_TRUE(cloneAppender->AppendSectionAttribute(indexDocument));
    InnerCheckDocument(indexDocument, despStr);
}

// despStr: section_count0,section_count1,section_count2
IndexDocumentPtr SectionAttributeAppenderTest::CreateIndexDocument(const string& despStr)
{
    IndexDocumentPtr indexDocument(new IndexDocument(mPool));
    vector<int> sectionCounts;
    StringUtil::fromString(despStr, sectionCounts, ",");
    size_t fieldCount = mSchema->GetFieldSchema()->GetFieldCount();
    assert(sectionCounts.size() == fieldCount);
    
    for (size_t i = 0; i < fieldCount; ++i)
    {
	IndexTokenizeField* pField = dynamic_cast<IndexTokenizeField*>(
                indexDocument->CreateField((fieldid_t)i, Field::FieldTag::TOKEN_FIELD));
	for (int j = 0; j < sectionCounts[i]; ++j)
	{
	    Section* pSection = pField->CreateSection();
	    pSection->SetLength(i + j + 1);
	    pSection->SetWeight((section_weight_t)(i + j +1));
	}
    }
    return indexDocument;
}

// despStr: section_count0,section_count1,section_count2
void SectionAttributeAppenderTest::InnerCheckDocument(const IndexDocumentPtr& indexDocument,
						      const string& despStr)
{
    IndexSchemaPtr indexSchema = mSchema->GetIndexSchema();
    IndexSchema::Iterator it;
    for (it = indexSchema->Begin(); it != indexSchema->End(); ++it)
    {
	if ((*it)->GetIndexType() == it_expack || (*it)->GetIndexType() == it_pack)
	{
	    PackageIndexConfigPtr packIndexConfig = DYNAMIC_POINTER_CAST(PackageIndexConfig, *it);
	    const ConstString& sectionAttr = indexDocument->GetSectionAttribute(
		packIndexConfig->GetIndexId());

	    if (packIndexConfig->HasSectionAttribute())
	    {
		CheckSectionAttribute(sectionAttr, packIndexConfig, despStr);
	    }
	    else
	    {
		ASSERT_EQ(ConstString::EMPTY_STRING, sectionAttr);
	    }
	}
    }
}

void SectionAttributeAppenderTest::ResetSchema()
{
    string field = "field0:text;field1:text;field2:text";
    string index = "index0:pack:field0,field2;"
	           "index1:expack:field1,field2;";
    mSchema = SchemaMaker::MakeSchema(field, index, "", "");    
}

// indexFilter: indexName0,indexName1
void SectionAttributeAppenderTest::OverwriteSchema(
    const IndexPartitionSchemaPtr& schema,
    const string& indexFilter,
    bool isUniqCompress, bool hasSectionWeight, bool hasFieldId)
{
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    vector<string> IndexWithNoSectionAttrConfig;
    StringUtil::fromString(indexFilter, IndexWithNoSectionAttrConfig, ",");
    
    IndexSchema::Iterator it;
    for (it = indexSchema->Begin(); it != indexSchema->End(); ++it)
    {
	IndexType indexType = (*it)->GetIndexType();
	string indexName = (*it)->GetIndexName();
	if (indexType != it_expack || indexType != it_pack)
	{
	    continue;
	}
	PackageIndexConfigPtr packIndexConfig =
	    DYNAMIC_POINTER_CAST(PackageIndexConfig, *it);

	bool hideSectionAttribute = false;
	for (size_t i = 0; i < IndexWithNoSectionAttrConfig.size(); ++i)
	{
	    if (indexName == IndexWithNoSectionAttrConfig[i])
	    {
		hideSectionAttribute = true;
	    }
	}
	if (hideSectionAttribute)
	{
	    packIndexConfig->SetHasSectionAttributeFlag(false);
	}
	    
	if (packIndexConfig->HasSectionAttribute())
	{
	    string compressType = isUniqCompress ? "uniq" : "";
	    SectionAttributeConfigPtr newConfig(
		new SectionAttributeConfig(
		    compressType, hasSectionWeight, hasFieldId));
	    packIndexConfig->SetSectionAttributeConfig(newConfig);
	}
    }
}

// despStr: section_count0,section_count1,section_count2
void  SectionAttributeAppenderTest::InnerTestAppendSectionAttribute(
    const string& despStr, const string& indexFilter,
    bool isUniqCompress, bool hasSectionWeight, bool hasFieldId)
{
    ResetSchema();
    OverwriteSchema(mSchema, indexFilter, isUniqCompress, hasSectionWeight, hasFieldId);
    IndexDocumentPtr indexDocument = CreateIndexDocument(despStr);
    SectionAttributeAppender sectionAttrAppender;
    ASSERT_TRUE(sectionAttrAppender.Init(mSchema));
    ASSERT_TRUE(sectionAttrAppender.AppendSectionAttribute(indexDocument));
    InnerCheckDocument(indexDocument, despStr);
}

void SectionAttributeAppenderTest::CheckSectionAttribute(const ConstString& sectionAttr,
							 const PackageIndexConfigPtr& packIndexConfig,
							 const string& despStr)
{
    uint8_t *buffer = new uint8_t[MAX_SECTION_BUFFER_LEN];
    SectionAttributeConfigPtr sectionAttrConf = packIndexConfig->GetSectionAttributeConfig();
    assert(sectionAttrConf);
    AttributeConfigPtr attrConfig = sectionAttrConf->CreateAttributeConfig(
            packIndexConfig->GetIndexName());
    
    SectionAttributeFormatter formatter(sectionAttrConf);
    StringAttributeConvertor convertor(attrConfig->IsUniqEncode(), attrConfig->GetAttrName());
    AttrValueMeta attrValueMeta = convertor.Decode(sectionAttr);
    char* pData = attrValueMeta.data.data();
    uint32_t dataLen = attrValueMeta.data.size();

    size_t encodeCountLen = 0;
    VarNumAttributeFormatter::DecodeCount(pData, encodeCountLen);
    pData += encodeCountLen;
    dataLen -= encodeCountLen;
    
    formatter.Decode(pData, dataLen, buffer, MAX_SECTION_BUFFER_LEN);
    section_len_t* lengthBuf;
    section_fid_t* fidBuf;
    section_weight_t* weightBuf;
    
    uint32_t sectionCount = formatter.UnpackBuffer(buffer, sectionAttrConf->HasFieldId(),
						   sectionAttrConf->HasSectionWeight(),
						   lengthBuf, fidBuf, weightBuf);
    
    vector<int> sectionCountsPerField;
    StringUtil::fromString(despStr, sectionCountsPerField, ",");
    size_t fieldCount = mSchema->GetFieldSchema()->GetFieldCount();
    ASSERT_EQ(sectionCountsPerField.size(), fieldCount);

    CheckUnpackSectionAttribute(lengthBuf, fidBuf, weightBuf, sectionCount,
				sectionCountsPerField, packIndexConfig);
    delete [] buffer;
}

void SectionAttributeAppenderTest::CheckUnpackSectionAttribute(
    section_len_t* lengthBuf, section_fid_t* fidBuf,
    section_weight_t* weightBuf, uint32_t sectionCount,
    const vector<int>& sectionCountsPerField,
    const PackageIndexConfigPtr& packIndexConfig)
{
    SectionAttributeConfigPtr sectionAttrConf = packIndexConfig->GetSectionAttributeConfig();
    uint32_t sectionCursor = 0;
    section_fid_t lastFieldIdx = 0;
    for (size_t i = 0; i < sectionCountsPerField.size(); ++i)
    {
	if (!packIndexConfig->IsInIndex((fieldid_t)i))
	{
	    continue;
	}

	section_fid_t curFieldIdx = packIndexConfig->GetFieldIdxInPack((fieldid_t)i);
	for (int j = 0; j < sectionCountsPerField[i]; ++j)
	{
	    section_len_t len = i + j + 1;
	    section_weight_t weight = i + j + 1;
	    if (sectionAttrConf->HasFieldId())
	    {
		EXPECT_EQ(curFieldIdx - lastFieldIdx, fidBuf[sectionCursor]);
	    }
	    else
	    {
		ASSERT_TRUE(fidBuf == NULL);
	    }

	    if (sectionAttrConf->HasSectionWeight())
	    {
		EXPECT_EQ(weight, weightBuf[sectionCursor]);
	    }
	    else
	    {
		ASSERT_TRUE(weightBuf == NULL);
	    }
	    EXPECT_EQ(len, lengthBuf[sectionCursor]);
	    ++sectionCursor;
	}
	lastFieldIdx = curFieldIdx;
    }
    EXPECT_EQ(sectionCursor, sectionCount);
}

IE_NAMESPACE_END(document);

