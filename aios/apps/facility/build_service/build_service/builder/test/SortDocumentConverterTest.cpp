#include "build_service/builder/SortDocumentConverter.h"

#include <memory>
#include <ostream>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/LongHashValue.h"
#include "autil/Span.h"
#include "build_service/builder/NormalSortDocConvertor.h"
#include "build_service/document/DocumentDefine.h"
#include "build_service/document/test/DocumentTestHelper.h"
#include "build_service/test/unittest.h"
#include "indexlib/base/BinaryStringUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/region_schema.h"
#include "indexlib/config/schema_modify_operation.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/attribute_document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace build_service::document;

using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::index_base;

namespace build_service { namespace builder {

class SortDocumentConverterTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();

protected:
    IndexPartitionSchemaPtr _schema;
};

void SortDocumentConverterTest::setUp()
{
    AttributeConfigPtr _field1;
    AttributeConfigPtr _field2;
    AttributeConfigPtr _field3;
    AttributeConfigPtr _field4;
    AttributeConfigPtr _field5;

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

    fieldConfig.reset(new FieldConfig("field4", ft_float, false));
    fieldConfig->SetFieldId(3);
    _field4.reset(new AttributeConfig);
    _field4->Init(fieldConfig);

    fieldConfig.reset(new FieldConfig("field5", ft_uint8, false));
    fieldConfig->SetFieldId(4);
    _field5.reset(new AttributeConfig);
    _field5->Init(fieldConfig);

    _schema.reset(new IndexPartitionSchema());
    RegionSchemaPtr regionSchema;
    regionSchema.reset(new RegionSchema(_schema->mImpl.get()));
    regionSchema->AddAttributeConfig(_field1);
    regionSchema->AddAttributeConfig(_field2);
    regionSchema->AddAttributeConfig(_field3);
    regionSchema->AddAttributeConfig(_field4);
    regionSchema->AddAttributeConfig(_field5);
    _schema->AddRegionSchema(regionSchema);
}

void SortDocumentConverterTest::tearDown() {}

TEST_F(SortDocumentConverterTest, testInit)
{
    indexlibv2::config::SortDescriptions sortDescs;
    sortDescs.push_back(indexlibv2::config::SortDescription("field1", indexlibv2::config::sp_desc));
    sortDescs.push_back(indexlibv2::config::SortDescription("field2", indexlibv2::config::sp_asc));

    NormalSortDocConvertor convertor;
    ASSERT_TRUE(convertor.init(sortDescs, _schema));

    ASSERT_EQ(size_t(2), convertor._convertFuncs.size());
    ASSERT_TRUE(convertor._convertFuncs[0]);
    ASSERT_TRUE(convertor._convertFuncs[1]);

    ASSERT_EQ(size_t(2), convertor._fieldIds.size());
    ASSERT_EQ(0, convertor._fieldIds[0]);
    ASSERT_EQ(1, convertor._fieldIds[1]);
}

TEST_F(SortDocumentConverterTest, testInitFailed)
{
    {
        // not exist field
        indexlibv2::config::SortDescriptions sortDescs;
        sortDescs.push_back(indexlibv2::config::SortDescription("field1", indexlibv2::config::sp_desc));
        sortDescs.push_back(indexlibv2::config::SortDescription("unknown", indexlibv2::config::sp_asc));

        NormalSortDocConvertor converter;
        ASSERT_FALSE(converter.init(sortDescs, _schema));
    }
    {
        // unsortable field
        indexlibv2::config::SortDescriptions sortDescs;
        sortDescs.push_back(indexlibv2::config::SortDescription("field1", indexlibv2::config::sp_desc));
        sortDescs.push_back(indexlibv2::config::SortDescription("field3", indexlibv2::config::sp_asc));

        NormalSortDocConvertor converter;
        ASSERT_FALSE(converter.init(sortDescs, _schema));
    }
}

TEST_F(SortDocumentConverterTest, testConvert)
{
    indexlibv2::config::SortDescriptions sortDescs;
    sortDescs.push_back(indexlibv2::config::SortDescription("field1", indexlibv2::config::sp_desc));
    sortDescs.push_back(indexlibv2::config::SortDescription("field2", indexlibv2::config::sp_asc));
    sortDescs.push_back(indexlibv2::config::SortDescription("field4", indexlibv2::config::sp_asc));
    sortDescs.push_back(indexlibv2::config::SortDescription("field5", indexlibv2::config::sp_desc));

    NormalSortDocConvertor converter;
    ASSERT_TRUE(converter.init(sortDescs, _schema));
    stringstream fields;
    fields << "field1#uint32=10;field2#double=5.4;"
           << "field3#string=abc;"
           << "field4#float=" << DEFAULT_NULL_FIELD_STRING << ";"
           << "field5#uint8=" << DEFAULT_NULL_FIELD_STRING << ";";
    FakeDocument fakeDoc = FakeDocument::constructWithAttributes("1", fields.str());
    DocumentPtr document = DocumentTestHelper::createDocument(fakeDoc);
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);

    ASSERT_TRUE(doc != NULL);

    SortDocument sortDoc;
    ASSERT_TRUE(converter.convert(doc, sortDoc));

    EXPECT_EQ(StringView("1"), sortDoc._pk);
    EXPECT_EQ(ADD_DOC, sortDoc._docType);

    string expectedKey;
    expectedKey += indexlibv2::BinaryStringUtil::toInvertString<uint32_t>(10);
    expectedKey += indexlibv2::BinaryStringUtil::toString<double>(5.4);
    expectedKey += indexlibv2::BinaryStringUtil::toString<float>(0.0, true);
    expectedKey += indexlibv2::BinaryStringUtil::toInvertString<uint8_t>(0, true);

    EXPECT_EQ(StringView(expectedKey), sortDoc._sortKey);
    EXPECT_EQ(size_t(21), sortDoc._sortKey.length()); // 5 + 9 + 5 + 2
    EXPECT_EQ(expectedKey.length(), sortDoc._sortKey.length());

    NormalDocumentPtr doc2;
    DataBuffer dataBuffer(const_cast<char*>(sortDoc._docStr.data()), sortDoc._docStr.length());
    dataBuffer.read(doc2);
    doc2->SetDocId(0);
    EXPECT_EQ(doc->GetPrimaryKey(), doc2->GetPrimaryKey());
    EXPECT_EQ(doc->GetAttributeDocument()->GetField(0), doc2->GetAttributeDocument()->GetField(0));
    EXPECT_EQ(doc->GetAttributeDocument()->GetField(1), doc2->GetAttributeDocument()->GetField(1));
    EXPECT_EQ(doc->GetAttributeDocument()->GetField(2), doc2->GetAttributeDocument()->GetField(2));
    EXPECT_EQ(doc->GetAttributeDocument()->GetField(3), doc2->GetAttributeDocument()->GetField(3));
}

}} // namespace build_service::builder
