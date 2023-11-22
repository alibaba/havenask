#include "indexlib/document/normal/SingleDocumentParser.h"

#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/document/normal/SummaryFormatter.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/table/normal_table/NormalSchemaResolver.h"
#include "unittest/unittest.h"

namespace indexlibv2::document {

class SingleDocumentParserTest : public TESTBASE
{
public:
    SingleDocumentParserTest() = default;
    ~SingleDocumentParserTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void SingleDocumentParserTest::setUp() {}

void SingleDocumentParserTest::tearDown() {}

TEST_F(SingleDocumentParserTest, testUsage)
{
    // prepare schema
    std::shared_ptr<config::ITabletSchema> schema(
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/schema_summary_reuse_source.json")
            .release());
    ASSERT_TRUE(schema);
    auto option = std::make_shared<config::TabletOptions>();
    table::NormalSchemaResolver resolver(option.get());
    auto tabletSchema = std::dynamic_pointer_cast<config::TabletSchema>(schema);
    ASSERT_TRUE(resolver.Resolve("", tabletSchema.get()).IsOK());

    // prepare raw doc
    std::shared_ptr<RawDocument> rawDoc(new DefaultRawDocument);
    rawDoc->setField("string1", "string1_value");
    rawDoc->setField("price", "price_value");
    rawDoc->setDocOperateType(ADD_DOC);

    // prepare extend doc
    NormalExtendDocument extendDoc;
    extendDoc.SetRawDocument(rawDoc);
    extendDoc.getClassifiedDocument()->setOriginalSnapshot(
        std::shared_ptr<RawDocument::Snapshot>(rawDoc->GetSnapshot()));

    // init parser
    SingleDocumentParser parser;
    std::shared_ptr<indexlib::util::AccumulativeCounter> accumulativeCounter;
    ASSERT_TRUE(parser.Init(schema, accumulativeCounter));
    auto normalDoc = parser.Parse(&extendDoc);
    ASSERT_TRUE(normalDoc);

    auto summaryDoc = normalDoc->GetSummaryDocument();
    auto summaryConfig =
        std::dynamic_pointer_cast<config::SummaryIndexConfig>(schema->GetIndexConfig("summary", "summary"));
    SummaryFormatter formatter(summaryConfig);
    indexlib::document::SearchSummaryDocument searchSummaryDoc(nullptr, 10);
    formatter.DeserializeSummaryDoc(summaryDoc, &searchSummaryDoc);
    auto toString = [](const autil::StringView* str) -> std::string {
        if (!str) {
            return "";
        }
        return std::string(str->data(), str->size());
    };
    auto string1Value = toString(searchSummaryDoc.GetFieldValue(0));
    auto priceValue = toString(searchSummaryDoc.GetFieldValue(1));
    ASSERT_EQ("string1_value", string1Value);
    ASSERT_EQ("", priceValue);
}

} // namespace indexlibv2::document
