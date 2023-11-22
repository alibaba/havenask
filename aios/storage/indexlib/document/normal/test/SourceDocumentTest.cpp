#include "indexlib/document/normal/SourceDocument.h"

#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "unittest/unittest.h"
using indexlibv2::document::DefaultRawDocument;
using indexlibv2::document::RawDocument;
using namespace autil;

namespace indexlib::document {

class SourceDocumentTest : public TESTBASE
{
public:
    SourceDocumentTest() = default;
    ~SourceDocumentTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void SourceDocumentTest::setUp() {}

void SourceDocumentTest::tearDown() {}

TEST_F(SourceDocumentTest, TestSimpleProcess)
{
    std::shared_ptr<RawDocument> rawDoc(new DefaultRawDocument);
    mem_pool::Pool* pool = rawDoc->getPool();
    std::shared_ptr<SourceDocument> doc1(new SourceDocument(pool));
    // group1 f1,f2,f3
    // group2 f2,f4,f5
    // group3 f6
    doc1->Append(1, "f1", StringView("v1"), true);
    doc1->Append(2, "f4", StringView("v4"), true);
    doc1->Append(1, "f2", StringView("v2"), true);
    doc1->AppendNonExistField(1, "no_exist1");
    doc1->Append(1, "f3", StringView("v3"), true);
    doc1->Append(3, "f6", StringView("v6"), true);
    doc1->AppendNonExistField(3, "no_exist3");
    doc1->Append(2, "f5", StringView("v5"), true);
    doc1->Append(1, "f1", StringView("V11"), true);
    doc1->Append(2, "f2", StringView("V22"), true);

    ASSERT_EQ(StringView("V11"), doc1->GetField(1, "f1"));
    ASSERT_EQ(StringView("v4"), doc1->GetField(2, "f4"));
    ASSERT_EQ(StringView("V22"), doc1->GetField(2, "f2"));
    ASSERT_EQ(StringView("v2"), doc1->GetField(1, "f2"));
    ASSERT_EQ(StringView("v6"), doc1->GetField(3, "f6"));

    ASSERT_TRUE(doc1->HasField(1, "no_exist1"));
    ASSERT_TRUE(doc1->IsNonExistField(1, "no_exist1"));
    ASSERT_TRUE(doc1->HasField(3, "no_exist3"));
    ASSERT_TRUE(doc1->IsNonExistField(3, "no_exist3"));

    // test to rawDoc
    doc1->ToRawDocument(*rawDoc);
    ASSERT_EQ("V11", rawDoc->getField("f1"));
    ASSERT_EQ("v4", rawDoc->getField("f4"));
    ASSERT_FALSE(rawDoc->exist("no_exist1"));
    ASSERT_FALSE(rawDoc->exist("no_exist2"));

    // for same field name, value use the group with largest group id
    ASSERT_EQ("V22", rawDoc->getField("f2"));
    ASSERT_EQ("v6", rawDoc->getField("f6"));
}

TEST_F(SourceDocumentTest, TestMerge)
{
    mem_pool::Pool* pool = new mem_pool::Pool();

    std::shared_ptr<SourceDocument> doc1(new SourceDocument(pool));
    doc1->Append(0, "f0", StringView("v0"), true);
    doc1->Append(1, "f1", StringView("v1"), true);
    doc1->Append(1, "f2", StringView("v2"), true);
    doc1->Append(2, "f3", StringView("v3"), true);
    doc1->Append(2, "f4", StringView("v4"), true);

    std::shared_ptr<SourceDocument> doc2(new SourceDocument(pool));
    doc2->Append(0, "f0", StringView("newValue0"), true);
    doc2->AppendNonExistField(1, "f1");
    doc2->Append(1, "f2", StringView("newValue2"), true);
    doc2->Append(2, "f3", StringView("newValue3"), true);
    doc2->Append(2, "f4", StringView::empty_instance(), true);

    ASSERT_TRUE(doc1->Merge(doc2));
    ASSERT_EQ(StringView("newValue0"), doc1->GetField(0, "f0"));
    ASSERT_EQ(StringView("v1"), doc1->GetField(1, "f1"));
    ASSERT_EQ(StringView("newValue2"), doc1->GetField(1, "f2"));
    ASSERT_EQ(StringView("newValue3"), doc1->GetField(2, "f3"));
    ASSERT_EQ(StringView::empty_instance(), doc1->GetField(2, "f4"));

    // different src schema, merge failed, and doc1 value not affected
    std::shared_ptr<SourceDocument> doc3(new SourceDocument(pool));
    doc3->Append(0, "f0", StringView("q0"), true);
    doc3->Append(1, "f1", StringView::empty_instance(), true);
    doc3->Append(1, "f2", StringView("q2"), true);
    ASSERT_FALSE(doc1->Merge(doc3));
    ASSERT_EQ(StringView("newValue0"), doc1->GetField(0, "f0"));
    ASSERT_EQ(StringView("v1"), doc1->GetField(1, "f1"));
    ASSERT_EQ(StringView("newValue2"), doc1->GetField(1, "f2"));
    ASSERT_EQ(StringView("newValue3"), doc1->GetField(2, "f3"));
    ASSERT_EQ(StringView::empty_instance(), doc1->GetField(2, "f4"));

    // doc1 merge doc4 with different pool
    std::shared_ptr<SourceDocument> doc4(new SourceDocument(NULL));
    doc4->Append(0, "f0", StringView("b0"), true);
    doc4->Append(1, "f1", StringView::empty_instance(), true);
    doc4->Append(1, "f2", StringView("b2"), true);
    doc4->AppendNonExistField(2, "f3");
    doc4->AppendNonExistField(2, "f4");
    ASSERT_TRUE(doc1->Merge(doc4));

    // doc4 delete pool, and check doc1 getfield
    doc4.reset();
    ASSERT_EQ(StringView("b0"), doc1->GetField(0, "f0"));
    ASSERT_EQ(StringView::empty_instance(), doc1->GetField(1, "f1"));
    ASSERT_EQ(StringView("b2"), doc1->GetField(1, "f2"));
    ASSERT_EQ(StringView("newValue3"), doc1->GetField(2, "f3"));
    ASSERT_EQ(StringView::empty_instance(), doc1->GetField(2, "f4"));

    delete pool;
}

TEST_F(SourceDocumentTest, TestMergeAccessaryField)
{
    autil::mem_pool::Pool pool;
    std::shared_ptr<SourceDocument> srcDoc(new SourceDocument(&pool));
    srcDoc->Append(0, "f1", StringView("v1"), true);
    srcDoc->Append(0, "f2", StringView("v2"), true);
    srcDoc->Append(1, "f1", StringView("v1"), true);
    srcDoc->Append(1, "f3", StringView("v3"), true);

    std::shared_ptr<SourceDocument> other(new SourceDocument(&pool));
    other->Append(0, "f1", StringView("nv1"), true);
    other->Append(0, "f2", StringView("v2"), true);
    other->Append(1, "f1", StringView("v1"), true);
    other->Append(1, "f3", StringView("nv3"), true);
    other->AppendAccessaryField("a1", StringView("a1"), true);
    other->AppendAccessaryField("a2", StringView("a2"), true);

    srcDoc->Merge(other);
    ASSERT_EQ(StringView("nv1"), srcDoc->GetField(0, "f1"));
    ASSERT_EQ(StringView("v2"), srcDoc->GetField(0, "f2"));
    ASSERT_EQ(StringView("v1"), srcDoc->GetField(1, "f1"));
    ASSERT_EQ(StringView("nv3"), srcDoc->GetField(1, "f3"));
    ASSERT_EQ(StringView("a2"), srcDoc->GetAccessaryField("a2"));
    ASSERT_EQ(StringView("a1"), srcDoc->GetAccessaryField("a1"));
}

} // namespace indexlib::document
