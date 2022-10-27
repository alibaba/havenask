#include "indexlib/document/index_document/normal_document/test/attribute_document_unittest.h"
#include <autil/mem_pool/Pool.h>
#include <autil/ConstString.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, AttributeDocumentTest);

AttributeDocumentTest::AttributeDocumentTest()
{
}

AttributeDocumentTest::~AttributeDocumentTest()
{
}

void AttributeDocumentTest::CaseSetUp()
{
}

void AttributeDocumentTest::CaseTearDown()
{
}

void AttributeDocumentTest::TestSimpleProcess()
{
    Pool pool;
    AttributeDocument attrDoc;
    attrDoc.SetDocId(23);
    vector<string> normalFieldVec;
    for (size_t i = 0; i < 10; i++)
    {
        stringstream ss;
        ss << "field_" << i;
        normalFieldVec.push_back(ss.str());
        attrDoc.SetField(i, ConstString(ss.str(), &pool));
    }

    vector<string> packFieldVec;
    for (size_t i = 0; i < 5; i++)
    {
        stringstream ss;
        ss << "pack_field_" << i;
        packFieldVec.push_back(ss.str());
        attrDoc.SetPackField(i, ConstString(ss.str(), &pool));
    }

    DataBuffer buffer;
    attrDoc.serialize(buffer);

    AttributeDocument attrDocOther;
    attrDocOther.deserialize(buffer, &pool);
    attrDocOther.SetDocId(23);
    
    AttributeDocument::Iterator iter = attrDocOther.CreateIterator();
    int idx = 0;
    while (iter.HasNext())
    {
        fieldid_t fid;
        const ConstString& field = iter.Next(fid);
        ASSERT_EQ(ConstString(normalFieldVec[idx]), field);
        ASSERT_EQ(fieldid_t(idx), fid);
        idx++;
    }

    ASSERT_EQ(packFieldVec.size(), attrDocOther.GetPackFieldCount());
    for (size_t i = 0; i < packFieldVec.size(); ++i)
    {
        ASSERT_EQ(ConstString(packFieldVec[i]), attrDocOther.GetPackField(i));
    }

    ASSERT_EQ(ConstString::EMPTY_STRING, attrDocOther.GetPackField(INVALID_ATTRID));
    ASSERT_EQ(ConstString::EMPTY_STRING, attrDocOther.GetPackField(packFieldVec.size()));

    ASSERT_EQ(attrDoc, attrDocOther);
}

IE_NAMESPACE_END(document);

