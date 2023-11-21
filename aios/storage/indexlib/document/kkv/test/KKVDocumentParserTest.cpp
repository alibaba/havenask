#include "indexlib/document/kkv/KKVDocumentParser.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/kkv/KKVDocument.h"
#include "indexlib/document/kkv/KKVKeysExtractor.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace document {

class KKVDocumentParserTest : public TESTBASE
{
public:
    void setUp() override
    {
        _schema.reset(
            framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "kkv_schema.json").release());
        ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schema.get()).IsOK());
    }

protected:
    const std::shared_ptr<ExtendDocument> InitExtendDoc(const std::map<std::string, std::string>& field2value,
                                                        DocOperateType type) const
    {
        auto extendDoc = std::make_shared<ExtendDocument>();
        std::shared_ptr<RawDocument> rawDoc(new DefaultRawDocument());
        rawDoc->setDocOperateType(type);
        for (auto& [field, value] : field2value) {
            rawDoc->setField(field, value);
        }
        extendDoc->SetRawDocument(rawDoc);
        return extendDoc;
    }

protected:
    std::shared_ptr<config::ITabletSchema> _schema;
};

TEST_F(KKVDocumentParserTest, testDeleteDocWithoutSKey)
{
    auto config = std::dynamic_pointer_cast<config::KKVIndexConfig>(_schema->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(config);
    config->SetDenyEmptySuffixKey(true);

    auto extendDoc = InitExtendDoc({{"pkey", "1"}, {"value", "111"}}, DELETE_DOC);
    auto parser = std::make_unique<KKVDocumentParser>();
    ASSERT_TRUE(parser->Init(_schema, nullptr).IsOK());
    auto [status, batch] = parser->Parse(*extendDoc);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(batch);
    ASSERT_EQ(1, batch->GetBatchSize());

    auto document = std::dynamic_pointer_cast<KKVDocument>((*batch)[0]);
    ASSERT_TRUE(document);
    auto kkvKeysExtractor = std::make_shared<KKVKeysExtractor>(config);
    uint64_t pkeyHash = 0;
    kkvKeysExtractor->HashPrefixKey("1", pkeyHash);
    ASSERT_EQ(pkeyHash, document->GetPKeyHash());
    ASSERT_EQ(false, document->HasSKey());
}

}} // namespace indexlibv2::document
