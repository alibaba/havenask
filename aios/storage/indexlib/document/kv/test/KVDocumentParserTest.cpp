#include "indexlib/document/kv/KVDocumentParser.h"

#include "autil/DataBuffer.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/kv/KVDocument.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/document/kv/KVKeyExtractor.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace document {

class KvDocumentParserTest : public TESTBASE
{
public:
    void setUp() override;

private:
    std::shared_ptr<ExtendDocument> CreateExtendDoc(const std::string& fieldMap);
    void FillRawDoc(const std::shared_ptr<RawDocument>& rawDoc);

protected:
    std::shared_ptr<ExtendDocument> _extendDoc;
    std::shared_ptr<config::ITabletSchema> _schemaPtr;
};

void KvDocumentParserTest::setUp()
{
    _extendDoc.reset(new ExtendDocument());
    std::shared_ptr<RawDocument> rawDoc(new DefaultRawDocument());
    _extendDoc->SetRawDocument(rawDoc);
}

TEST_F(KvDocumentParserTest, testKVAdd)
{
    _extendDoc->GetRawDocument()->setDocOperateType(ADD_DOC);
    _schemaPtr.reset(
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "document_parser_kv_schema.json")
            .release());
    ASSERT_TRUE(_schemaPtr);
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());

    FillRawDoc(_extendDoc->GetRawDocument());
    auto parser = std::make_unique<KVDocumentParser>();
    ASSERT_TRUE(parser->Init(_schemaPtr, nullptr).IsOK());
    auto [status, batch] = parser->Parse(*_extendDoc);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(batch);
    ASSERT_EQ(1, batch->GetBatchSize());
    auto document = std::dynamic_pointer_cast<KVDocument>((*batch)[0]);
    ASSERT_TRUE(document);
    ASSERT_EQ(123, document->GetSchemaId());
    auto kvConfig = std::dynamic_pointer_cast<config::KVIndexConfig>(_schemaPtr->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kvConfig);

    auto kvKeyExtractor =
        std::make_shared<KVKeyExtractor>(kvConfig->GetFieldConfig()->GetFieldType(), kvConfig->UseNumberHash());
    uint64_t keyHash;
    kvKeyExtractor->HashKey("jack", keyHash);

    ASSERT_EQ(keyHash, document->GetPKeyHash());
}

TEST_F(KvDocumentParserTest, testKVUpdate)
{
    _extendDoc->GetRawDocument()->setDocOperateType(UPDATE_FIELD);
    _schemaPtr.reset(
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "document_parser_kv_schema.json")
            .release());
    ASSERT_TRUE(_schemaPtr);
    ASSERT_TRUE(framework::TabletSchemaLoader::ResolveSchema(nullptr, "", _schemaPtr.get()).IsOK());

    FillRawDoc(_extendDoc->GetRawDocument());
    auto parser = std::make_unique<KVDocumentParser>();
    ASSERT_TRUE(parser->Init(_schemaPtr, nullptr).IsOK());
    auto [status, batch] = parser->Parse(*_extendDoc);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(batch);
    ASSERT_EQ(1, batch->GetBatchSize());
    auto document = std::dynamic_pointer_cast<KVDocument>((*batch)[0]);
    ASSERT_TRUE(document);

    auto kvConfig = std::dynamic_pointer_cast<config::KVIndexConfig>(_schemaPtr->GetPrimaryKeyIndexConfig());
    ASSERT_TRUE(kvConfig);
    auto kvKeyExtractor =
        std::make_shared<KVKeyExtractor>(kvConfig->GetFieldConfig()->GetFieldType(), kvConfig->UseNumberHash());
    uint64_t keyHash;
    kvKeyExtractor->HashKey("jack", keyHash);

    ASSERT_EQ(keyHash, document->GetPKeyHash());
}

TEST_F(KvDocumentParserTest, TestSkipTypeRawDocumentParse)
{
    _extendDoc->GetRawDocument()->setDocOperateType(SKIP_DOC);
    _schemaPtr.reset(
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/document_parser_kv_schema.json")
            .release());

    FillRawDoc(_extendDoc->GetRawDocument());
    auto parser = std::make_unique<KVDocumentParser>();
    ASSERT_TRUE(parser->Init(_schemaPtr, nullptr).IsOK());
    auto [status, batch] = parser->Parse(*_extendDoc);
    ASSERT_TRUE(status.IsOK());
    ASSERT_FALSE(batch);
}

TEST_F(KvDocumentParserTest, TestParseFromSerializedData)
{
    _schemaPtr.reset(
        framework::TabletSchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "/document_parser_kv_schema.json")
            .release());

    autil::mem_pool::Pool pool;
    std::shared_ptr<KVDocument> doc = std::make_shared<KVDocument>(&pool);
    doc->_indexNameHash = 5;
    doc->_schemaId = 6;
    autil::DataBuffer buffer;

    auto batch = std::make_unique<KVDocumentBatch>();
    batch->AddDocument(doc);
    buffer.write(batch);

    auto parser = std::make_unique<KVDocumentParser>();
    ASSERT_TRUE(parser->Init(_schemaPtr, nullptr).IsOK());
    auto [status, IBatch] = parser->Parse(std::string(buffer.getData(), buffer.getDataLen()));
    ASSERT_TRUE(status.IsOK());
    auto batch1 = dynamic_cast<KVDocumentBatch*>(IBatch.get());
    ASSERT_TRUE(batch1);
    ASSERT_EQ(1, batch1->GetAddedDocCount());
    std::shared_ptr<KVDocument> doc1 = std::dynamic_pointer_cast<KVDocument>((*batch1)[0]);
    ASSERT_TRUE(doc1);
    ASSERT_EQ(5, doc1->_indexNameHash);
    ASSERT_EQ(6, doc1->_schemaId);
}

std::shared_ptr<ExtendDocument> KvDocumentParserTest::CreateExtendDoc(const string& fieldMap)
{
    std::shared_ptr<ExtendDocument> extendDoc(new ExtendDocument());
    std::shared_ptr<RawDocument> rawDoc(new DefaultRawDocument);
    vector<string> fields = autil::StringTokenizer::tokenize(autil::StringView(fieldMap), ";");
    for (size_t i = 0; i < fields.size(); ++i) {
        vector<string> kv = autil::StringTokenizer::tokenize(autil::StringView(fields[i]), ":");
        rawDoc->setField(kv[0], kv[1]);
    }
    rawDoc->setDocOperateType(ADD_DOC);
    extendDoc->SetRawDocument(rawDoc);
    return extendDoc;
}

void KvDocumentParserTest::FillRawDoc(const std::shared_ptr<RawDocument>& rawDoc)
{
    rawDoc->setField("usernick", "jack");
    rawDoc->setField("userid", "1");
    rawDoc->setField("username", "zhangsan");
    rawDoc->setField("userage", "20");
    rawDoc->setField("usernation", "china");
}

}} // namespace indexlibv2::document
