#include "indexlib/document/normal/NormalDocument.h"

#include "unittest/unittest.h"
using indexlib::document::SerializedSourceDocument;

namespace indexlibv2::document {

class NormalDocumentTest : public TESTBASE
{
public:
    NormalDocumentTest() = default;
    ~NormalDocumentTest() = default;

private:
public:
    void setUp() override;
    void tearDown() override;
};

void NormalDocumentTest::setUp() {}

void NormalDocumentTest::tearDown() {}

TEST_F(NormalDocumentTest, TestSerializeSourceDocument)
{
    std::shared_ptr<NormalDocument> doc1(new NormalDocument);
    doc1->SetSchemaId(1);
    std::shared_ptr<indexlib::document::AttributeDocument> attDoc(new indexlib::document::AttributeDocument());
    doc1->SetAttributeDocument(attDoc);

    std::string tmp1 = "f1,f2";
    autil::StringView meta(tmp1);
    std::shared_ptr<SerializedSourceDocument> srcDoc(new SerializedSourceDocument(NULL));
    srcDoc->SetMeta(meta);
    std::string tmp2 = "v1,v2";
    autil::StringView groupData(tmp2);
    srcDoc->SetGroupValue(0, groupData);
    doc1->SetSourceDocument(srcDoc);

    {
        autil::DataBuffer dataBuffer;
        dataBuffer.write(doc1);

        std::shared_ptr<NormalDocument> doc2(new NormalDocument);
        dataBuffer.read(doc2);
        ASSERT_EQ(1u, doc2->GetSchemaId());
        ASSERT_TRUE(doc2->_sourceDocument);
        ASSERT_EQ(meta, doc2->_sourceDocument->GetMeta());
        ASSERT_EQ(groupData, doc2->_sourceDocument->GetGroupValue(0));
    }
    {
        // old binary read new version doc
        autil::DataBuffer dataBuffer;
        doc1->DoSerialize(dataBuffer, 9);

        std::shared_ptr<NormalDocument> doc2(new NormalDocument);
        doc2->DoDeserialize(dataBuffer, 8);
        ASSERT_EQ(1u, doc2->GetSchemaId());
        ASSERT_FALSE(doc2->_sourceDocument);
    }
}

TEST_F(NormalDocumentTest, TestSerializeFieldMetaDocument)
{
    std::shared_ptr<NormalDocument> doc1(new NormalDocument);
    std::shared_ptr<indexlib::document::AttributeDocument> attDoc(new indexlib::document::AttributeDocument());
    doc1->SetAttributeDocument(attDoc);

    auto fieldMetaDoc = std::make_shared<indexlib::document::FieldMetaDocument>();
    std::string tmp1 = "f1,f2";
    autil::StringView meta(tmp1);
    fieldMetaDoc->SetField(0, meta, false);
    doc1->SetFieldMetaDocument(fieldMetaDoc);

    {
        autil::DataBuffer dataBuffer;
        dataBuffer.write(doc1);

        std::shared_ptr<NormalDocument> doc2(new NormalDocument);
        dataBuffer.read(doc2);

        ASSERT_TRUE(doc2->_fieldMetaDocument);
        bool isNull;
        ASSERT_EQ(meta, doc2->_fieldMetaDocument->GetField(0, isNull));
        ASSERT_FALSE(isNull);
    }

    // old binary read new version doc with out field meta
    {
        auto normalDoc = std::make_shared<NormalDocument>();
        auto attrDoc = std::make_shared<indexlib::document::AttributeDocument>();
        std::string attr1 = "hello";
        autil::StringView tmp(attr1);
        attrDoc->SetField(0, tmp);
        normalDoc->SetAttributeDocument(attrDoc);
        ASSERT_EQ(tmp, attrDoc->GetField(0));
        ASSERT_EQ(tmp, normalDoc->GetAttributeDocument()->GetField(0));

        // currnet doc version is 12 with field meta doc
        autil::DataBuffer dataBuffer;
        normalDoc->DoSerialize(dataBuffer, 12);

        auto oldDoc = std::make_shared<NormalDocument>();
        oldDoc->DoDeserialize(dataBuffer, 11);
        ASSERT_TRUE(oldDoc->GetAttributeDocument());
        ASSERT_EQ(tmp, oldDoc->GetAttributeDocument()->GetField(0));
        ASSERT_FALSE(oldDoc->GetFieldMetaDocument());
    }
}

} // namespace indexlibv2::document
