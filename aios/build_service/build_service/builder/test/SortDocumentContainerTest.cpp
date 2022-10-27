#include "build_service/test/unittest.h"
#include "build_service/builder/SortDocumentContainer.h"
#include "build_service/builder/NormalSortDocSorter.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include <indexlib/config/attribute_schema.h>
#include <indexlib/config/index_schema.h>
#include <indexlib/indexlib.h>
#include <indexlib/config/primary_key_index_config.h>
#include <indexlib/config/region_schema.h>
#include <indexlib/testlib/schema_maker.h>
#include <indexlib/testlib/document_creator.h>

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);

using namespace heavenask::indexlib;
using namespace autil;
using namespace build_service::document;
namespace build_service {
namespace builder {

using namespace std;
using namespace testing;

class SortDocumentContainerTest : public  BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    IE_NAMESPACE(document)::DocumentPtr createDocument(
            docid_t docId, const std::string& pkStr = "",
            DocOperateType opType = ADD_DOC, uint32_t sortField = 0, int64_t locator=0);
    void SetPKToSchema(const IndexPartitionSchemaPtr& schema) {
        PrimaryKeyIndexConfigPtr pkConfig(new PrimaryKeyIndexConfig("pk", it_primarykey64));
        auto regionSchema = schema->GetRegionSchema(DEFAULT_REGIONID);
        regionSchema->ResetIndexSchema();
        regionSchema->GetIndexSchema()->SetPrimaryKeyIndexConfig(pkConfig);
    }
protected:
    AttributeSchemaPtr _attrSchema;
    IndexPartitionSchemaPtr _schema;
    SortDescriptions _sortDescs;
};

void SortDocumentContainerTest::setUp() {
    SortDescription sortDesc;
    sortDesc.sortFieldName = "field";
    sortDesc.sortPattern = sp_asc;
    _sortDescs.push_back(sortDesc);
    _schema.reset(new IndexPartitionSchema());
    RegionSchemaPtr regionSchema;
    regionSchema.reset(new RegionSchema(_schema->mImpl.get()));
    {
        FieldConfigPtr fieldConfig(new FieldConfig("field", ft_uint32, false));
        fieldConfig->SetFieldId(0);
        AttributeConfigPtr attrConfig(new AttributeConfig);
        attrConfig->Init(fieldConfig);
        regionSchema->AddAttributeConfig(attrConfig);
    }
    {
        FieldConfigPtr fieldConfig(new FieldConfig("uint1", ft_uint32, false));
        fieldConfig->SetFieldId(1);
        AttributeConfigPtr attrConfig(new AttributeConfig);
        attrConfig->Init(fieldConfig);
        regionSchema->AddAttributeConfig(attrConfig);
    }
    {
        FieldConfigPtr fieldConfig(new FieldConfig("uint2", ft_uint32, false));
        fieldConfig->SetFieldId(2);
        AttributeConfigPtr attrConfig(new AttributeConfig);
        attrConfig->Init(fieldConfig);
        regionSchema->AddAttributeConfig(attrConfig);
    }

    _attrSchema = regionSchema->GetAttributeSchema();
    _schema->AddRegionSchema(regionSchema);
}

void SortDocumentContainerTest::tearDown() {
}

// use docid to check docs' order

TEST_F(SortDocumentContainerTest, testPushWithoutPk) {
    SortDocumentContainer container;
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));

    container.pushDocument(createDocument(0, "1", ADD_DOC, 0));
    container.pushDocument(createDocument(1, "1", ADD_DOC, 1));
    container.pushDocument(createDocument(2, "1", UPDATE_FIELD, 1));
    EXPECT_EQ(size_t(3), container.allDocCount());
    container._orderVec[0] = 0;
    container._orderVec[1] = 1;
    container._orderVec[2] = 2;
    container.sortDocument();
    EXPECT_EQ(int64_t(0), container[0]->GetTimestamp());
    EXPECT_EQ(int64_t(1), container[1]->GetTimestamp());
    EXPECT_EQ(int64_t(2), container[2]->GetTimestamp());
    NormalSortDocSorter* sorter = dynamic_cast<NormalSortDocSorter*>(container._sortDocumentSorter.get());
    ASSERT_TRUE(sorter);
    EXPECT_TRUE(sorter->_documentPkPosMap->empty());
    EXPECT_TRUE(sorter->_mergePosVector->empty());
}

TEST_F(SortDocumentContainerTest, testPushwithPk) {
    // 1. same pk with different operate type
    // 2. the recent ADD doc overwrite ADD before
    // 3. UPDATE, DELETE comes without ADD
    SortDocumentContainer container;
    SetPKToSchema(_schema);
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));

    container.pushDocument(createDocument(0, "1", ADD_DOC, 0));
    container.pushDocument(createDocument(1, "2", ADD_DOC, 1));
    container.pushDocument(createDocument(2, "1", UPDATE_FIELD, 0));
    container.pushDocument(createDocument(3, "1", DELETE_DOC, 0));
    container.pushDocument(createDocument(4, "1", ADD_DOC, 0));
    container.pushDocument(createDocument(5, "3", UPDATE_FIELD, 0));

    EXPECT_EQ(size_t(6), container.allDocCount());
    NormalSortDocSorter* sorter = dynamic_cast<NormalSortDocSorter*>(container._sortDocumentSorter.get());
    ASSERT_TRUE(sorter);

    EXPECT_THAT((*(sorter->_documentPkPosMap)), UnorderedElementsAre(
                    Pair(ConstString("1"), pair<int, int>(4, -1)),
                    Pair(ConstString("2"), pair<int, int>(1, -1)),
                    Pair(ConstString("3"), pair<int, int>(5, -1))));
    ASSERT_EQ(size_t(1), sorter->_mergePosVector->size());
    EXPECT_THAT((*(sorter->_mergePosVector))[0], ElementsAre(2, 3));
}

TEST_F(SortDocumentContainerTest, testSortWithoutPk) {
    // 1. sort stable with equal sort field
    // 2. ignore same pk with different operate type
    SortDocumentContainer container;
    _sortDescs[0].sortPattern = sp_desc;
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));

    container.pushDocument(createDocument(0, "1", ADD_DOC, 0));
    container.pushDocument(createDocument(1, "2", ADD_DOC, 1));
    container.pushDocument(createDocument(2, "2", ADD_DOC, 1));
    container.pushDocument(createDocument(3, "3", ADD_DOC, 2));
    container.sortDocument();
    ASSERT_EQ(size_t(4), container.allDocCount());
    ASSERT_EQ(size_t(4), container.sortedDocCount());
    EXPECT_EQ(int64_t(3), container[0]->GetTimestamp());
    EXPECT_EQ(int64_t(1), container[1]->GetTimestamp());
    EXPECT_EQ(int64_t(2), container[2]->GetTimestamp());
    EXPECT_EQ(int64_t(0), container[3]->GetTimestamp());
}

TEST_F(SortDocumentContainerTest, testSortWithPk) {
    {
        // 1. multi operate  with same pk
        SortDocumentContainer container;
        SetPKToSchema(_schema);
        _sortDescs[0].sortPattern = sp_desc;
        common::Locator locator;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
        container.pushDocument(createDocument(0, "1", ADD_DOC, 0));
        container.pushDocument(createDocument(1, "1", UPDATE_FIELD, 0));
        container.pushDocument(createDocument(2, "1", ADD_DOC, 0));
        container.pushDocument(createDocument(3, "1", UPDATE_FIELD, 0));
        container.pushDocument(createDocument(4, "1", DELETE_DOC, 0));
        container.sortDocument();
        EXPECT_EQ(size_t(5), container.allDocCount());
        EXPECT_EQ(size_t(3), container.sortedDocCount());
        int64_t expectedTs[] = {2, 3, 4};
        for (size_t i = 0; i < container.sortedDocCount(); i++) {
            EXPECT_EQ(expectedTs[i], container[i]->GetTimestamp());
        }
    }
    {
        // 1. with equal sort field
        SortDocumentContainer container;
        SetPKToSchema(_schema);
        _sortDescs[0].sortPattern = sp_desc;
        common::Locator locator;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
        container.pushDocument(createDocument(0, "0", ADD_DOC, 0));
        container.pushDocument(createDocument(1, "1", ADD_DOC, 1));
        container.pushDocument(createDocument(2, "2", ADD_DOC, 1));
        container.pushDocument(createDocument(3, "3", ADD_DOC, 2));
        container.sortDocument();
        EXPECT_EQ(size_t(4), container.allDocCount());
        EXPECT_EQ(size_t(4), container.sortedDocCount());
        EXPECT_EQ(int64_t(3), container[0]->GetTimestamp());
        EXPECT_EQ(int64_t(1), container[1]->GetTimestamp());
        EXPECT_EQ(int64_t(2), container[2]->GetTimestamp());
        EXPECT_EQ(int64_t(0), container[3]->GetTimestamp());
    }
}

TEST_F(SortDocumentContainerTest, testUnprocessedCount) {
    SortDocumentContainer container;
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
    EXPECT_EQ((uint32_t)0, container.getUnprocessedCount());
    container.pushDocument(createDocument(0, "",  ADD_DOC));
    container.pushDocument(createDocument(1, "", ADD_DOC));
    container.sortDocument();
    EXPECT_EQ((uint32_t)2, container.getUnprocessedCount());
    container.clear();
    EXPECT_EQ((uint32_t)0, container.getUnprocessedCount());
}

TEST_F(SortDocumentContainerTest, testMergedLocator) {
    SortDocumentContainer container;
    SetPKToSchema(_schema);
    _sortDescs[0].sortPattern = sp_desc;
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
    container.pushDocument(createDocument(0, "0", UPDATE_FIELD, 0, 0));
    container.pushDocument(createDocument(1, "0", UPDATE_FIELD, 1, 1));
    container.pushDocument(createDocument(2, "0", UPDATE_FIELD, 1, 2));
    container.pushDocument(createDocument(3, "1", ADD_DOC, 1, 3));
    container.pushDocument(createDocument(4, "2", ADD_DOC, 1, 4));
    container.sortDocument();
    EXPECT_EQ(int64_t(-1), container._firstLocator.getOffset());
    EXPECT_EQ(int64_t(4), container._lastLocator.getOffset());
    EXPECT_EQ(size_t(3), container.sortedDocCount());
    EXPECT_EQ(size_t(6), container.allDocCount());

    for (size_t i = 0; i < 2; i++) {
        ASSERT_TRUE(locator.fromString(container[i]->GetLocator().ToString()));
        ASSERT_EQ(int64_t(-1), locator.getOffset());
    }
    ASSERT_TRUE(locator.fromString(container[2]->GetLocator().ToString()));
    ASSERT_EQ(int64_t(4), locator.getOffset());
}

TEST_F(SortDocumentContainerTest, testLocator) {
    SortDocumentContainer container;
    SetPKToSchema(_schema);
    _sortDescs[0].sortPattern = sp_desc;
    common::Locator locator;
    ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
    container.pushDocument(createDocument(0, "0", ADD_DOC, 0, 0));
    container.pushDocument(createDocument(1, "1", ADD_DOC, 1, 1));
    container.pushDocument(createDocument(2, "2", ADD_DOC, 1, 2));
    container.pushDocument(createDocument(3, "3", ADD_DOC, 2, 3));
    container.sortDocument();
    EXPECT_EQ(int64_t(-1), container._firstLocator.getOffset());
    EXPECT_EQ(int64_t(3), container._lastLocator.getOffset());
    EXPECT_EQ(size_t(4), container.sortedDocCount());
    EXPECT_EQ(size_t(4), container.allDocCount());

    for (size_t i = 0; i < 3; i++) {
        ASSERT_TRUE(locator.fromString(container[i]->GetLocator().ToString()));
        ASSERT_EQ(int64_t(-1), locator.getOffset());
    }
    ASSERT_TRUE(locator.fromString(container[3]->GetLocator().ToString()));
    ASSERT_EQ(int64_t(3), locator.getOffset());

    // swap
    SortDocumentContainer container2;
    container2.swap(container);
    EXPECT_EQ(int64_t(-1), container2._firstLocator.getOffset());
    EXPECT_EQ(int64_t(3), container2._lastLocator.getOffset());

    // clear
    container2.clear();
    ASSERT_EQ(int64_t(3), container2._firstLocator.getOffset());
    ASSERT_EQ(int64_t(3), container2._lastLocator.getOffset());
}

DocumentPtr newAttrDoc(const string &pk, const string &sortfield,
                       const string &uint1, const string &uint2,
                       DocOperateType opType)
{
    return DocumentTestHelper::createDocument(
            FakeDocument::constructWithAttributes(pk,
                    "field#uint32=" + sortfield +
                    (uint1.empty() ? ";uint1" : ";uint1#uint32=" + uint1) +
                    (uint2.empty() ? ";uint2" : ";uint2#uint32=" + uint2),
                    opType));
}

TEST_F(SortDocumentContainerTest, testMergeUpdateDocWithSwap) {
    SetPKToSchema(_schema);
    common::Locator locator;
    SortDocumentContainer container1;
    SortDocumentContainer container2;
    _sortDescs[0].sortPattern = sp_desc;
    ASSERT_TRUE(container1.init(_sortDescs, _schema, locator));
    ASSERT_TRUE(container2.init(_sortDescs, _schema, locator));
    container2.swap(container1);

    container1.pushDocument(newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
    container1.pushDocument(newAttrDoc("1", "0", "1", "", UPDATE_FIELD));
    container1.pushDocument(newAttrDoc("1", "0", "", "2", UPDATE_FIELD));
    container1.sortDocument();

    container2.clear();
    container2.pushDocument(newAttrDoc("1", "0", "1", "", UPDATE_FIELD));
    container2.pushDocument(newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
    container2.pushDocument(newAttrDoc("1", "0", "", "2", UPDATE_FIELD));
    container2.sortDocument();

    uint32_t value0 = 0;
    ConstString field0(reinterpret_cast<char*>(&value0), sizeof(value0));
    uint32_t value1 = 1;
    ConstString field1(reinterpret_cast<char*>(&value1), sizeof(value1));
    uint32_t value2 = 2;
    ConstString field2(reinterpret_cast<char*>(&value2), sizeof(value2));

    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, container1[0]);
    EXPECT_EQ(field0, doc->GetAttributeDocument()->GetField(0));
    EXPECT_EQ(field1, doc->GetAttributeDocument()->GetField(1));
    EXPECT_EQ(field2, doc->GetAttributeDocument()->GetField(2));
}

TEST_F(SortDocumentContainerTest, testMergeUpdateDoc) {
    SetPKToSchema(_schema);
    common::Locator locator;
    {
        // only update
        SortDocumentContainer container;
        _sortDescs[0].sortPattern = sp_desc;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
        container.pushDocument(newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "1", "", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "", "2", UPDATE_FIELD));
        container.sortDocument();
        EXPECT_EQ(size_t(1), container.sortedDocCount());
        EXPECT_EQ(size_t(4), container.allDocCount());
        uint32_t value0 = 0;
        ConstString field0(reinterpret_cast<char*>(&value0), sizeof(value0));
        uint32_t value1 = 1;
        ConstString field1(reinterpret_cast<char*>(&value1), sizeof(value1));
        uint32_t value2 = 2;
        ConstString field2(reinterpret_cast<char*>(&value2), sizeof(value2));

        NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, container[0]);
        EXPECT_EQ(field0, doc->GetAttributeDocument()->GetField(0));
        EXPECT_EQ(field1, doc->GetAttributeDocument()->GetField(1));
        EXPECT_EQ(field2, doc->GetAttributeDocument()->GetField(2));
    }
    {
        // update mix
        SortDocumentContainer container;
        _sortDescs[0].sortPattern = sp_desc;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));
        container.pushDocument(newAttrDoc("1", "0", "3", "3", ADD_DOC));
        container.pushDocument(newAttrDoc("1", "0", "3", "3", DELETE_DOC));

        container.pushDocument(newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "1", "", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "", "2", UPDATE_FIELD));

        container.pushDocument(newAttrDoc("1", "0", "3", "3", DELETE_DOC));

        container.pushDocument(newAttrDoc("1", "0", "6", "7", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "4", "9", UPDATE_FIELD));
        container.pushDocument(newAttrDoc("1", "0", "8", "5", UPDATE_FIELD));

        container.pushDocument(newAttrDoc("1", "0", "3", "3", DELETE_DOC));
        container.pushDocument(newAttrDoc("1", "0", "3", "3", DELETE_SUB_DOC));

        container.sortDocument();
        EXPECT_EQ(size_t(7), container.sortedDocCount());
        EXPECT_EQ(size_t(13), container.allDocCount());
        EXPECT_EQ(ADD_DOC, container[0]->GetDocOperateType());
        EXPECT_EQ(DELETE_DOC, container[1]->GetDocOperateType());
        EXPECT_EQ(UPDATE_FIELD, container[2]->GetDocOperateType());
        EXPECT_EQ(DELETE_DOC, container[3]->GetDocOperateType());
        EXPECT_EQ(UPDATE_FIELD, container[4]->GetDocOperateType());
        EXPECT_EQ(DELETE_DOC, container[5]->GetDocOperateType());
        EXPECT_EQ(DELETE_SUB_DOC, container[6]->GetDocOperateType());
        uint32_t value0 = 0;
        ConstString field0(reinterpret_cast<char*>(&value0), sizeof(value0));
        uint32_t value1 = 1;
        ConstString field1(reinterpret_cast<char*>(&value1), sizeof(value1));
        uint32_t value2 = 2;
        ConstString field2(reinterpret_cast<char*>(&value2), sizeof(value2));

        NormalDocumentPtr doc2 = DYNAMIC_POINTER_CAST(NormalDocument, container[2]);
        EXPECT_EQ(field0, doc2->GetAttributeDocument()->GetField(0));
        EXPECT_EQ(field1, doc2->GetAttributeDocument()->GetField(1));
        EXPECT_EQ(field2, doc2->GetAttributeDocument()->GetField(2));

        uint32_t value8 = 8;
        ConstString field8(reinterpret_cast<char*>(&value8), sizeof(value8));
        uint32_t value5 = 5;
        ConstString field5(reinterpret_cast<char*>(&value5), sizeof(value5));

        NormalDocumentPtr doc4 = DYNAMIC_POINTER_CAST(NormalDocument, container[4]);
        EXPECT_EQ(field0, doc4->GetAttributeDocument()->GetField(0));
        EXPECT_EQ(field8, doc4->GetAttributeDocument()->GetField(1));
        EXPECT_EQ(field5, doc4->GetAttributeDocument()->GetField(2));
    }
    {
        // sub doc do not merge
        SortDocumentContainer container;
        _schema->SetSubIndexPartitionSchema(IndexPartitionSchemaPtr(new IndexPartitionSchema()));
        _sortDescs[0].sortPattern = sp_desc;
        ASSERT_TRUE(container.init(_sortDescs, _schema, locator));

        NormalDocumentPtr doc1 = DYNAMIC_POINTER_CAST(NormalDocument,
                newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
        NormalDocumentPtr subdoc1 = DYNAMIC_POINTER_CAST(NormalDocument,
                newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));

        doc1->AddSubDocument(subdoc1);
        container.pushDocument(doc1);

        NormalDocumentPtr doc2 = DYNAMIC_POINTER_CAST(NormalDocument,
                newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
        NormalDocumentPtr subdoc2 = DYNAMIC_POINTER_CAST(NormalDocument,
                newAttrDoc("1", "0", "3", "3", UPDATE_FIELD));
        doc2->AddSubDocument(subdoc2);
        container.pushDocument(doc2);
        container.sortDocument();
        EXPECT_EQ(size_t(2), container.sortedDocCount());
        EXPECT_EQ(size_t(2), container.allDocCount());
    }
}

DocumentPtr SortDocumentContainerTest::createDocument(
        docid_t docId, const string& pkStr, DocOperateType opType,
        uint32_t sortField, int64_t locator)
{
    document::FakeDocument fakeDoc(pkStr, opType, true, true, docId, sortField, 0, locator);
    DocumentPtr doc = DocumentTestHelper::createDocument(fakeDoc);
    doc->SetTimestamp((int64_t)docId);
    return doc;
}

}
}
