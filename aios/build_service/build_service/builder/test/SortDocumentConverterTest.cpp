#include "build_service/test/unittest.h"
#include "build_service/builder/SortDocumentConverter.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include "build_service/builder/BinaryStringUtil.h"
#include <autil/DataBuffer.h>
#include "build_service/builder/NormalSortDocConvertor.h"
#include <indexlib/config/region_schema.h>

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::document;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);

namespace build_service {
namespace builder {

class SortDocumentConverterTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    IndexPartitionSchemaPtr _schema;
};

void SortDocumentConverterTest::setUp() {
    AttributeConfigPtr _field1;
    AttributeConfigPtr _field2;
    AttributeConfigPtr _field3;

    FieldConfigPtr fieldConfig(new FieldConfig("field1", ft_uint32, false));
    fieldConfig->SetFieldId(0);
    _field1.reset(new AttributeConfig);
    _field1->Init(fieldConfig);

    fieldConfig.reset(new FieldConfig("field2", ft_double, false));
    fieldConfig->SetFieldId(1);
    _field2.reset(new AttributeConfig);
    _field2->Init(fieldConfig);

    fieldConfig.reset(new FieldConfig("field3", ft_string, false));
    fieldConfig->SetFieldId(2);
    _field3.reset(new AttributeConfig);
    _field3->Init(fieldConfig);

    _schema.reset(new IndexPartitionSchema());
    RegionSchemaPtr regionSchema;
    regionSchema.reset(new RegionSchema(_schema->mImpl.get()));
    regionSchema->AddAttributeConfig(_field1);
    regionSchema->AddAttributeConfig(_field2);
    regionSchema->AddAttributeConfig(_field3);
    _schema->AddRegionSchema(regionSchema);
}

void SortDocumentConverterTest::tearDown() {
}

TEST_F(SortDocumentConverterTest, testInit) {
    SortDescriptions sortDescs;
    sortDescs.resize(2);

    sortDescs[0].sortFieldName = "field1";
    sortDescs[0].sortPattern = sp_desc;
    sortDescs[1].sortFieldName = "field2";
    sortDescs[1].sortPattern = sp_asc;

    NormalSortDocConvertor convertor;
    ASSERT_TRUE(convertor.init(sortDescs, _schema));

    ASSERT_EQ(size_t(2), convertor._emitKeyFuncs.size());
    ASSERT_TRUE(convertor._emitKeyFuncs[0]);
    ASSERT_TRUE(convertor._emitKeyFuncs[1]);

    ASSERT_EQ(size_t(2), convertor._fieldIds.size());
    ASSERT_EQ(0, convertor._fieldIds[0]);
    ASSERT_EQ(1, convertor._fieldIds[1]);
}

TEST_F(SortDocumentConverterTest, testInitFailed) {
    {
        // not exist field
        SortDescriptions sortDescs;
        sortDescs.resize(2);

        sortDescs[0].sortFieldName = "field1";
        sortDescs[0].sortPattern = sp_desc;
        sortDescs[1].sortFieldName = "unknown";
        sortDescs[1].sortPattern = sp_asc;

        NormalSortDocConvertor converter;
        ASSERT_FALSE(converter.init(sortDescs, _schema));
    }
    {
        // unsortable field
        SortDescriptions sortDescs;
        sortDescs.resize(2);

        sortDescs[0].sortFieldName = "field1";
        sortDescs[0].sortPattern = sp_desc;
        sortDescs[1].sortFieldName = "field3";
        sortDescs[1].sortPattern = sp_asc;

        NormalSortDocConvertor converter;
        ASSERT_FALSE(converter.init(sortDescs, _schema));
    }
}

TEST_F(SortDocumentConverterTest, testConvert) {
    SortDescriptions sortDescs;
    sortDescs.resize(2);

    sortDescs[0].sortFieldName = "field1";
    sortDescs[0].sortPattern = sp_desc;
    sortDescs[1].sortFieldName = "field2";
    sortDescs[1].sortPattern = sp_asc;

    NormalSortDocConvertor converter;
    ASSERT_TRUE(converter.init(sortDescs, _schema));
    FakeDocument fakeDoc = FakeDocument::constructWithAttributes("1", "field1#uint32=10;field2#double=5.4");
    DocumentPtr document = DocumentTestHelper::createDocument(fakeDoc);
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);

    ASSERT_TRUE(doc != NULL);

    SortDocument sortDoc;
    ASSERT_TRUE(converter.convert(doc, sortDoc));

    EXPECT_EQ(ConstString("1"), sortDoc._pk);
    EXPECT_EQ(ADD_DOC, sortDoc._docType);
    EXPECT_EQ(size_t(13), sortDoc._sortKey.length()); // 5 + 8
    
    string expectedKey, str;
    BinaryStringUtil::toInvertString<uint32_t>(10, str);
    expectedKey += str;
    BinaryStringUtil::toString<double>(5.4, str);
    expectedKey += str;
    EXPECT_EQ(ConstString(expectedKey), sortDoc._sortKey);

    NormalDocumentPtr doc2;
    DataBuffer dataBuffer(const_cast<char*>(sortDoc._docStr.data()), sortDoc._docStr.length());
    dataBuffer.read(doc2);
    doc2->SetDocId(0);
    EXPECT_EQ(doc->GetPrimaryKey(), doc2->GetPrimaryKey());
    EXPECT_EQ(doc->GetAttributeDocument()->GetField(0), doc2->GetAttributeDocument()->GetField(0));
    EXPECT_EQ(doc->GetAttributeDocument()->GetField(1), doc2->GetAttributeDocument()->GetField(1));
}

}
}
