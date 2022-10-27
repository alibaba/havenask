#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/key_hasher_typed.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/document/index_document/normal_document/test/document_serialize_test_helper.h"
#include "indexlib/document/index_document/normal_document/test/index_document_unittest.h"
#include <autil/StringUtil.h>
#include <sstream>
#include <vector>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(document);


void IndexDocumentTest::CaseSetUp()
{
    mPool = new autil::mem_pool::Pool;
}

void IndexDocumentTest::CaseTearDown()
{
    delete mPool;
    mPool = NULL;
}

void IndexDocumentTest::TestCaseForCreateField()
{
    IndexDocumentPtr doc(new IndexDocument(mPool));
    Field* field = doc->CreateField((fieldid_t)1, Field::FieldTag::TOKEN_FIELD);
    field = doc->CreateField((fieldid_t)1);
    field = doc->CreateField((fieldid_t)3);

    INDEXLIB_TEST_TRUE(doc->GetField(1) != NULL);
    INDEXLIB_TEST_TRUE(doc->GetField(3) != NULL);
    INDEXLIB_TEST_TRUE(doc->GetField(0) == NULL);
    INDEXLIB_TEST_TRUE(doc->GetField(2) == NULL);
    INDEXLIB_TEST_TRUE(doc->GetField(1)->GetFieldId() == 1);

    INDEXLIB_TEST_EQUAL((uint32_t)2, doc->GetFieldCount());
        
    doc->SetSectionAttribute((indexid_t)2, ConstString("abc"));
    INDEXLIB_TEST_EQUAL((indexid_t)2, 
                        doc->GetMaxIndexIdInSectionAttribute());

    field = doc->CreateField((fieldid_t)1);
    INDEXLIB_TEST_EQUAL(field->GetFieldId(), 1);

    field = doc->CreateField((fieldid_t)2);
    INDEXLIB_TEST_EQUAL(field->GetFieldId(), 2);

    doc->CreateField((fieldid_t)3);
    INDEXLIB_TEST_EQUAL((uint32_t)3, doc->GetFieldCount());

}

void IndexDocumentTest::TestCaseForAddField()
{
    const uint32_t fieldCount = 3;
    vector<fieldid_t> fieldIdVec;
    for (uint32_t i = 0; i < fieldCount; ++i)
    {
	fieldIdVec.push_back((fieldid_t)(i * 2));
    }

    vector<Field*> fieldVec;
    CreateFields(fieldIdVec, fieldVec, 10 ,5);        
    IndexDocumentPtr doc(new IndexDocument(mPool));
    for (uint32_t i = 0; i < fieldVec.size(); ++i)
    {
	doc->AddField(fieldVec[i]);
    }

    INDEXLIB_TEST_EQUAL(fieldVec.size(), doc->GetFieldCount());
    for (uint32_t i = 0; i < fieldVec.size(); ++i)
    {
	Field* field = doc->GetField(i * 2);
	INDEXLIB_TEST_TRUE(field != NULL);
	INDEXLIB_TEST_EQUAL((fieldid_t)(i * 2), field->GetFieldId());
    }
    //no such field
    Field* field = doc->GetField(1);
    INDEXLIB_TEST_TRUE(field == NULL);

    field = doc->GetField(5);
    INDEXLIB_TEST_TRUE(field == NULL);

}

void IndexDocumentTest::TestCaseForSetField()
{
    const uint32_t fieldCount = 3;
    vector<fieldid_t> fieldIdVec;
    for (uint32_t i = 0; i < fieldCount; ++i)
    {
	fieldIdVec.push_back((fieldid_t)(i * 2));
    }

    vector<Field*> fieldVec;
    CreateFields(fieldIdVec, fieldVec, 10, 5); 
    IndexDocumentPtr doc(new IndexDocument(mPool));
        
    for (size_t i = 0; i < fieldVec.size(); ++i)
    {
	Field* field = fieldVec[i];
	doc->SetField(2 * i + 1, field);
	INDEXLIB_TEST_EQUAL((uint32_t)(i + 1), doc->GetFieldCount());
	INDEXLIB_TEST_TRUE(doc->GetField(2 * i + 1) != NULL);
	INDEXLIB_TEST_EQUAL((fieldid_t)(2 * i + 1), field->GetFieldId());
    }
}
    
void IndexDocumentTest::TestCaseForIterator()
{
    const uint32_t fieldCount = 3;
    vector<fieldid_t> fieldIdVec;
    for (uint32_t i = 0; i < fieldCount; ++i)
    {
	fieldIdVec.push_back((fieldid_t)(i * 2));
    }

    vector<Field*> fieldVec;
    CreateFields(fieldIdVec, fieldVec, 10, 5);        
    IndexDocumentPtr doc(new IndexDocument(mPool));
    for (uint32_t i = 0; i < fieldVec.size(); ++i)
    {
	doc->AddField(fieldVec[i]);
    }
    INDEXLIB_TEST_EQUAL(fieldVec.size(), doc->GetFieldCount());
        
    IndexDocument::Iterator it = doc->CreateIterator();
    INDEXLIB_TEST_TRUE(it.HasNext());

    size_t i = 0;
    while (it.HasNext())
    {
	Field* field = it.Next();
	INDEXLIB_TEST_TRUE(field != NULL);
	INDEXLIB_TEST_EQUAL(fieldVec[i]->GetFieldId(), field->GetFieldId());
	i++;
    }
        
    INDEXLIB_TEST_EQUAL(fieldVec.size(), (size_t)i);


    fieldid_t fieldIds[] = {1,4};
    for (i = 0; i < sizeof(fieldIds) / sizeof(fieldid_t); ++i)
    {
	Field* field1 = doc->CreateField(fieldIds[i]);
	INDEXLIB_TEST_TRUE(field1 != NULL);
    }
}

void IndexDocumentTest::TestCaseForNextInIterator()
{
    const uint32_t fieldCount = 3;
    vector<fieldid_t> fieldIdVec;
    for (uint32_t i = 0; i < fieldCount; ++i)
    {
	fieldIdVec.push_back((fieldid_t)(i * 2));
    }

    vector<Field*> fieldVec;
    CreateFields(fieldIdVec, fieldVec, 10, 5);        
    IndexDocumentPtr doc(new IndexDocument(mPool));
    for (uint32_t i = 0; i < fieldVec.size(); ++i)
    {
	doc->AddField(fieldVec[i]);
    }

    IndexDocument::Iterator it = doc->CreateIterator();
    int32_t i = 0;
    Field* field;
    while ((field = it.Next()))
    {
	INDEXLIB_TEST_EQUAL(fieldVec[i]->GetFieldId(), field->GetFieldId());
	i++;
    }
        
    INDEXLIB_TEST_EQUAL(fieldVec.size(), (size_t)i);
}

void IndexDocumentTest::TestCaseForHasNextInIterator()
{
    IndexDocumentPtr doc(new IndexDocument(mPool));
    IndexDocument::Iterator it = doc->CreateIterator();
    INDEXLIB_TEST_TRUE(!it.HasNext());

    const uint32_t fieldCount = 3;
    vector<fieldid_t> fieldIdVec;
    for (uint32_t i = 0; i < fieldCount; ++i)
    {
	fieldIdVec.push_back((fieldid_t)(i * 2));
    }

    vector<Field*> fieldVec;
    CreateFields(fieldIdVec, fieldVec,10, 5);        

    for (uint32_t i = 0; i < fieldVec.size(); ++i)
    {
	doc->AddField(fieldVec[i]);
    }
    INDEXLIB_TEST_TRUE(it.HasNext());
    INDEXLIB_TEST_TRUE(it.HasNext());
    INDEXLIB_TEST_TRUE(it.HasNext());
}

void IndexDocumentTest::TestCaseForOperatorEqual()
{
    IndexDocumentPtr document(new IndexDocument(mPool));
    int fieldCount = 2;
    int sectionCount = 3;
    int tokenCount = 4;
    CreateDocument(document, 1, fieldCount, sectionCount, tokenCount);
    int size = 5;
    for(int i = 0; i < size; i++)
    {
	for(int k = 0; k < size; k++)
	{
	    for(int j = 0 ; j < size; j++)
	    {
		IndexDocumentPtr newDocument(new IndexDocument(mPool));
		CreateDocument(newDocument, 1, i, k ,j);
		if(i == fieldCount && k == sectionCount && j == tokenCount)
		{
		    INDEXLIB_TEST_TRUE(*document == *newDocument);
		}
		else
		{
		    bool ret= (*document != *newDocument);
		    INDEXLIB_TEST_TRUE(ret);
		}
                    
	    }
	}
    }

    IndexDocumentPtr document1(new IndexDocument(mPool));
    IndexDocumentPtr document2(new IndexDocument(mPool));
    CreateDocument(document1, 1, 1, 0, 1);
    CreateDocument(document2, 1, 1, 0, 0);
    INDEXLIB_TEST_TRUE(*document1 == *document2);
    CreateDocument(document1, 1, 0, 2, 10);
    CreateDocument(document2, 1, 0, 3, 11);
    INDEXLIB_TEST_TRUE(*document1 == *document2);
}

void IndexDocumentTest::TestCaseForSetTermPayload()
{
    IndexDocument indexDoc(mPool);
    indexDoc.SetTermPayload(123456, 11);
    termpayload_t termPayload = indexDoc.GetTermPayload(123456);
    INDEXLIB_TEST_EQUAL((termpayload_t)11, termPayload);

    indexDoc.SetTermPayload(123456, 22);
    termPayload = indexDoc.GetTermPayload(123456);
    INDEXLIB_TEST_EQUAL((termpayload_t)22, termPayload);
}

void IndexDocumentTest::TestCaseForSetDocPayload()
{
    IndexDocument indexDoc(mPool);
    for (uint32_t i = 0; i < 256; ++i)
    {
	uint64_t key = i;
	indexDoc.SetDocPayload(key, 11);
	docpayload_t termPayload = indexDoc.GetDocPayload(key);
	INDEXLIB_TEST_EQUAL((termpayload_t)11, termPayload);

	indexDoc.SetDocPayload(key, 22);
	termPayload = indexDoc.GetDocPayload(key);
	INDEXLIB_TEST_EQUAL((termpayload_t)22, termPayload);
    }
}

void IndexDocumentTest::TestCaseSerializeIndexDocument()
{
    IndexDocumentPtr document1(new IndexDocument(mPool));
    IndexDocumentPtr document2(new IndexDocument(mPool));
    CreateDocument(document1, 1, 1, 2, 10);
    document1->SetTermPayload(1, 2);
    document1->SetTermPayload(3, 4);
    document1->SetDocPayload(5, 6);
    document1->SetDocPayload(7, 8);

    autil::DataBuffer dataBuffer;
    autil::mem_pool::Pool pool;
    dataBuffer.write(*document1);
    dataBuffer.read(*document2, &pool);

    INDEXLIB_TEST_EQUAL(termpayload_t(2), document2->GetTermPayload(1));
    INDEXLIB_TEST_EQUAL(termpayload_t(4), document2->GetTermPayload(3));
    INDEXLIB_TEST_EQUAL(docpayload_t(6), document2->GetDocPayload(5));
    INDEXLIB_TEST_EQUAL(docpayload_t(8), document2->GetDocPayload(7));
    TEST_INDEX_DOCUMENT_SERIALIZE_EQUAL((*document1), (*document2));
}

void IndexDocumentTest::TestCaseForSerializeSectionAttribute()
{
    IndexDocumentPtr document1(new IndexDocument(mPool));
    IndexDocumentPtr document2(new IndexDocument(mPool));
    document1->CreateSectionAttribute(0, "00000");
    document1->CreateSectionAttribute(1, "11111");
    document1->CreateSectionAttribute(3, "33333");
    autil::DataBuffer dataBuffer;
    autil::mem_pool::Pool pool;
    dataBuffer.write(*document1);
    dataBuffer.read(*document2, &pool);

    const ConstString& sectionAttr0 = document2->GetSectionAttribute(0);
    const ConstString& sectionAttr1 = document2->GetSectionAttribute(1);
    const ConstString& sectionAttr3 = document2->GetSectionAttribute(3);
    const ConstString& nonExistAttr2 = document2->GetSectionAttribute(2);
    const ConstString& nonExistAttr4 = document2->GetSectionAttribute(4);

    INDEXLIB_TEST_EQUAL(ConstString("00000"), sectionAttr0);
    INDEXLIB_TEST_EQUAL(ConstString("11111"), sectionAttr1);
    INDEXLIB_TEST_EQUAL(ConstString("33333"), sectionAttr3);
    INDEXLIB_TEST_EQUAL(ConstString::EMPTY_STRING, nonExistAttr2);
    INDEXLIB_TEST_EQUAL(ConstString::EMPTY_STRING, nonExistAttr4);
    TEST_INDEX_DOCUMENT_SERIALIZE_EQUAL((*document1), (*document2));
}

void IndexDocumentTest::CreateDocument(IndexDocumentPtr& indexDoc, 
				       docid_t docId,
				       int fieldCount,
				       int sectionCount, 
				       int termCount)
{
    stringstream ss;
    ss << docId;
    indexDoc->SetPrimaryKey(ss.str());

    vector<fieldid_t> fieldIdVec;
    for (int j = 0; j < fieldCount; ++j)
    {
	fieldIdVec.push_back((fieldid_t)(j * 2));
    }

    vector<Field*> fieldVec;
    CreateFields(fieldIdVec, fieldVec, sectionCount, termCount);

    indexDoc->SetDocId(docId);
    for (uint32_t j = 0; j < fieldVec.size(); ++j)
    {
	indexDoc->AddField(fieldVec[j]);
    }

    if(fieldCount > 0)
    {
	for (int j = 0; j < sectionCount * termCount; ++j)
	{
	    stringstream ss;
	    ss << "text" << j;
	    DefaultHasher hasher;
	    uint64_t hashKey;
	    hasher.GetHashKey(ss.str().c_str(), hashKey);
	    indexDoc->SetDocPayload(hashKey, j);
	}

	for (int j = 0; j < sectionCount * termCount; j++ )
	{
	    indexDoc->SetTermPayload(j,j+1);
	}
    }
}
  
void IndexDocumentTest::CreateFields(const vector<fieldid_t>& fieldIds, 
				     vector<Field*>& fieldVec,
				     uint32_t sectionsPerField ,
				     uint32_t termCountPerSection)
{
    uint32_t termCount = termCountPerSection;
    vector<Section*> sectionVec;
    for (uint32_t i = 0; i < fieldIds.size() * sectionsPerField; i++)
    {
	Section* section = IE_POOL_COMPATIBLE_NEW_CLASS(mPool, Section, 8, mPool);
	for (size_t j = 0; j < termCount; ++j)
	{
	    stringstream ss;
	    ss << "field" << i << "section" << j 
	       << "hello";
	    DefaultHasher hasher;
	    uint64_t hashKey;
	    hasher.GetHashKey(ss.str().c_str(), hashKey);
	    section->CreateToken(hashKey);
	}
	sectionVec.push_back(section);
    }

    for (uint32_t i = 0; i < (uint32_t)fieldIds.size(); ++i)
    {
	IndexTokenizeField* field = IE_POOL_COMPATIBLE_NEW_CLASS(mPool, IndexTokenizeField, mPool);
	if(sectionsPerField > 0)
	{
	    for (uint32_t j = 0; j < sectionsPerField; j++)
	    {
		field->AddSection(sectionVec[i * sectionsPerField + j]);
	    }
	}
	field->SetFieldId(fieldIds[i]);
	fieldVec.push_back(field);
    }
}

void IndexDocumentTest::TestCaseForSerializeCompatibility()
{
    string testDir = TEST_DATA_PATH;
    IndexDocumentPtr document(new IndexDocument(mPool));
    CreateDocument(document, 1, 3, 6, 18);
    cout << document->mFieldCount << endl;

    // 3_0_index_document is generated by this code in branch 3.0
    // IndexDocumentPtr document(new IndexDocument);
    // CreateDocument(document, 1, 3, 6, 18);
    // autil::DataBuffer dataBuffer;
    // dataBuffer.write(*document);
    // storage::FileSystemWrapper::AtomicStore(testDir + "/3_0_index_document",
    //         string(dataBuffer.getStart(), dataBuffer.getDataLen()));

    string docStr;
    storage::FileSystemWrapper::AtomicLoad(testDir + "/3_0_index_document", docStr);
    autil::DataBuffer dataBuffer2((void*)docStr.c_str(), docStr.length());
    IndexDocumentPtr document2(new IndexDocument(mPool));
    document2->deserialize(dataBuffer2, mPool, 4);

    // mDocId and mFieldCount does not serialized
    document->mDocId = document2->mDocId = INVALID_DOCID;
    document->mFieldCount = document2->mFieldCount = 0;

    ASSERT_TRUE(*document == *document2);
}

IE_NAMESPACE_END(document);
