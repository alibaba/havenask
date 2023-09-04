#include "indexlib/document/document_rewriter/TTLSetter.h"

#include "autil/TimeUtility.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "unittest/unittest.h"

namespace indexlibv2::document {

class TTLSetterTest : public TESTBASE
{
public:
    TTLSetterTest() = default;
    ~TTLSetterTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    std::shared_ptr<indexlibv2::document::NormalDocument>
    MakeDocument(DocOperateType docType, const std::shared_ptr<indexlibv2::index::AttributeConfig>& attrConfig,
                 int64_t timestampInUs, std::optional<uint32_t> ttlValue)
    {
        auto attrDocument = std::make_shared<indexlib::document::AttributeDocument>();
        auto normalDocument = std::make_shared<indexlibv2::document::NormalDocument>();
        if (ttlValue != std::nullopt) {
            std::shared_ptr<index::AttributeConvertor> convertor(
                index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
            attrDocument->SetField(attrConfig->GetFieldId(),
                                   convertor->Encode(std::to_string(ttlValue.value()), normalDocument->GetPool()));
        }

        normalDocument->SetDocOperateType(docType);
        auto docInfo = normalDocument->GetDocInfo();
        docInfo.timestamp = timestampInUs;
        normalDocument->SetDocInfo(docInfo);
        normalDocument->SetAttributeDocument(attrDocument);
        return normalDocument;
    }
};

void TTLSetterTest::setUp() {}

void TTLSetterTest::tearDown() {}

TEST_F(TTLSetterTest, testUsage)
{
    // user not configure ttl_field, set default value for ttl
    const uint32_t CURRENT_TS_IN_SECOND = 1234;
    const int64_t CURRENT_TS_IN_US = autil::TimeUtility::sec2us(CURRENT_TS_IN_SECOND);
    const uint32_t DEFAULT_TTL = 60;

    auto fieldConfig = std::make_shared<config::FieldConfig>(DOC_TIME_TO_LIVE_IN_SECONDS, FieldType::ft_uint32, false);
    fieldConfig->SetFieldId(0);
    auto attrConfig = std::make_shared<index::AttributeConfig>();
    ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());

    DocumentBatch documentBatch;

    documentBatch.AddDocument(MakeDocument(ADD_DOC, attrConfig, CURRENT_TS_IN_US, std::nullopt));
    documentBatch.AddDocument(MakeDocument(UPDATE_FIELD, attrConfig, CURRENT_TS_IN_US, std::nullopt));
    documentBatch.AddDocument(MakeDocument(ADD_DOC, attrConfig, CURRENT_TS_IN_US, 123));
    documentBatch.AddDocument(MakeDocument(UPDATE_FIELD, attrConfig, CURRENT_TS_IN_US, 123));
    documentBatch.AddDocument(
        MakeDocument(ADD_DOC, attrConfig, CURRENT_TS_IN_US, std::numeric_limits<uint32_t>::max()));
    documentBatch.AddDocument(
        MakeDocument(UPDATE_FIELD, attrConfig, CURRENT_TS_IN_US, std::numeric_limits<uint32_t>::max()));
    documentBatch.AddDocument(MakeDocument(DELETE_DOC, attrConfig, CURRENT_TS_IN_US, std::nullopt));

    auto ttlSetter = std::make_shared<TTLSetter>(attrConfig, DEFAULT_TTL);
    auto status = ttlSetter->Rewrite(&documentBatch);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(CURRENT_TS_IN_SECOND + DEFAULT_TTL, documentBatch.TEST_GetDocument(0)->GetTTL());
    ASSERT_EQ(0, documentBatch.TEST_GetDocument(1)->GetTTL());
    ASSERT_EQ(123, documentBatch.TEST_GetDocument(2)->GetTTL());
    ASSERT_EQ(0, documentBatch.TEST_GetDocument(3)->GetTTL());
    ASSERT_EQ(std::numeric_limits<uint32_t>::max(), documentBatch.TEST_GetDocument(4)->GetTTL());
    ASSERT_EQ(0, documentBatch.TEST_GetDocument(5)->GetTTL());
    ASSERT_EQ(0, documentBatch.TEST_GetDocument(6)->GetTTL());
    ASSERT_EQ(std::numeric_limits<uint32_t>::max(), documentBatch.GetMaxTTL());
}

TEST_F(TTLSetterTest, testUserDefinedTTLField)
{
    // user not configure ttl_field, set default value for ttl
    const uint32_t CURRENT_TS_IN_SECOND = 1234;
    const int64_t CURRENT_TS_IN_US = autil::TimeUtility::sec2us(CURRENT_TS_IN_SECOND);
    const uint32_t DEFAULT_TTL = 60;

    auto fieldConfig =
        std::make_shared<config::FieldConfig>("user_defined_ttl_field_name", FieldType::ft_uint32, false);
    fieldConfig->SetFieldId(0);
    auto attrConfig = std::make_shared<index::AttributeConfig>();
    ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());

    DocumentBatch documentBatch;

    documentBatch.AddDocument(MakeDocument(ADD_DOC, attrConfig, CURRENT_TS_IN_US, std::nullopt));
    documentBatch.AddDocument(MakeDocument(UPDATE_FIELD, attrConfig, CURRENT_TS_IN_US, std::nullopt));
    documentBatch.AddDocument(MakeDocument(ADD_DOC, attrConfig, CURRENT_TS_IN_US, 123));
    documentBatch.AddDocument(MakeDocument(UPDATE_FIELD, attrConfig, CURRENT_TS_IN_US, 123));
    documentBatch.AddDocument(
        MakeDocument(ADD_DOC, attrConfig, CURRENT_TS_IN_US, std::numeric_limits<uint32_t>::max()));
    documentBatch.AddDocument(
        MakeDocument(UPDATE_FIELD, attrConfig, CURRENT_TS_IN_US, std::numeric_limits<uint32_t>::max()));

    auto ttlSetter = std::make_shared<TTLSetter>(attrConfig, DEFAULT_TTL);
    auto status = ttlSetter->Rewrite(&documentBatch);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(0, documentBatch.TEST_GetDocument(0)->GetTTL());
    ASSERT_EQ(0, documentBatch.TEST_GetDocument(1)->GetTTL());
    ASSERT_EQ(123, documentBatch.TEST_GetDocument(2)->GetTTL());
    ASSERT_EQ(0, documentBatch.TEST_GetDocument(3)->GetTTL());
    ASSERT_EQ(std::numeric_limits<uint32_t>::max(), documentBatch.TEST_GetDocument(4)->GetTTL());
    ASSERT_EQ(0, documentBatch.TEST_GetDocument(5)->GetTTL());
    ASSERT_EQ(std::numeric_limits<uint32_t>::max(), documentBatch.GetMaxTTL());
}

} // namespace indexlibv2::document
