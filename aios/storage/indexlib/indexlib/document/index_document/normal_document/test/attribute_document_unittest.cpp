#include "indexlib/document/index_document/normal_document/test/attribute_document_unittest.h"

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, AttributeDocumentTest);

AttributeDocumentTest::AttributeDocumentTest() {}

AttributeDocumentTest::~AttributeDocumentTest() {}

void AttributeDocumentTest::CaseSetUp() {}

void AttributeDocumentTest::CaseTearDown() {}

void AttributeDocumentTest::TestSimpleProcess()
{
    Pool pool;
    AttributeDocument attrDoc;
    attrDoc.SetDocId(23);
    vector<string> normalFieldVec;
    for (size_t i = 0; i < 10; i++) {
        stringstream ss;
        ss << "field_" << i;
        normalFieldVec.push_back(ss.str());
        attrDoc.SetField(i, autil::MakeCString(ss.str(), &pool));
    }

    vector<string> packFieldVec;
    for (size_t i = 0; i < 5; i++) {
        stringstream ss;
        ss << "pack_field_" << i;
        packFieldVec.push_back(ss.str());
        attrDoc.SetPackField(i, autil::MakeCString(ss.str(), &pool));
    }

    DataBuffer buffer;
    attrDoc.serialize(buffer);

    AttributeDocument attrDocOther;
    attrDocOther.deserialize(buffer, &pool);
    attrDocOther.SetDocId(23);

    AttributeDocument::Iterator iter = attrDocOther.CreateIterator();
    int idx = 0;
    while (iter.HasNext()) {
        fieldid_t fid;
        const StringView& field = iter.Next(fid);
        ASSERT_EQ(StringView(normalFieldVec[idx]), field);
        ASSERT_EQ(fieldid_t(idx), fid);
        idx++;
    }

    ASSERT_EQ(packFieldVec.size(), attrDocOther.GetPackFieldCount());
    for (size_t i = 0; i < packFieldVec.size(); ++i) {
        ASSERT_EQ(StringView(packFieldVec[i]), attrDocOther.GetPackField(i));
    }

    ASSERT_EQ(StringView::empty_instance(), attrDocOther.GetPackField(INVALID_ATTRID));
    ASSERT_EQ(StringView::empty_instance(), attrDocOther.GetPackField(packFieldVec.size()));

    ASSERT_EQ(attrDoc, attrDocOther);
}

void AttributeDocumentTest::TestNullField()
{
    AttributeDocument attrDoc;
    StringView field4("f4");
    attrDoc.SetField(4, field4, true);
    ASSERT_FALSE(attrDoc.HasField(4));
    bool isNull = false;
    StringView ret = attrDoc.GetField(4, isNull);
    ASSERT_TRUE(isNull);
    ASSERT_EQ(StringView::empty_instance(), ret);

    StringView field5("f5");
    attrDoc.SetField(5, field5);
    ASSERT_TRUE(attrDoc.HasField(5));
    ret = attrDoc.GetField(5, isNull);
    ASSERT_FALSE(isNull);
    ASSERT_EQ(ret, field5);
    attrDoc.SetNullField(5);
    ASSERT_TRUE(attrDoc.IsNullField(5));
    ASSERT_TRUE(attrDoc.HasField(5));

    StringView field10("f10");
    attrDoc.SetField(10, field10);
    attrDoc.SetField(10, field10);

    // test serialize and deserialize
    DataBuffer dataBuffer;
    attrDoc.serialize(dataBuffer);
    attrDoc.serializeVersion8(dataBuffer);

    autil::mem_pool::Pool pool;
    AttributeDocument newAttrDoc;
    newAttrDoc.deserialize(dataBuffer, &pool, LEGACY_DOCUMENT_BINARY_VERSION);
    newAttrDoc.deserializeVersion8(dataBuffer);
    ASSERT_EQ(attrDoc._packFields, newAttrDoc._packFields);
    ASSERT_EQ(attrDoc._normalAttrDoc, newAttrDoc._normalAttrDoc);
    ASSERT_EQ(attrDoc._nullFields, newAttrDoc._nullFields);
}
}} // namespace indexlib::document
