#include "autil/DataBuffer.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/IIndexFields.h"
#include "indexlib/document/IIndexFieldsParser.h"
#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/inverted_index/InvertedIndexFactory.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"
#include "indexlib/index/inverted_index/config/test/InvertedIndexConfigCreator.h"
#include "indexlib/util/PooledUniquePtr.h"
#include "unittest/unittest.h"
using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlibv2::config;

namespace indexlib::index {

class InvertedIndexInteTest : public TESTBASE
{
public:
    InvertedIndexInteTest() = default;
    ~InvertedIndexInteTest() = default;

private:
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig>
    CreateSingleFieldIndexConfig(InvertedIndexType indexType, const std::string& indexName, FieldType fieldType);

public:
    void setUp() override;
    void tearDown() override;
};

void InvertedIndexInteTest::setUp() {}

void InvertedIndexInteTest::tearDown() {}

std::shared_ptr<indexlibv2::config::InvertedIndexConfig>
InvertedIndexInteTest::CreateSingleFieldIndexConfig(InvertedIndexType indexType, const std::string& indexName,
                                                    FieldType fieldType)
{
    auto indexConfig = indexlib::index::InvertedIndexConfigCreator::CreateSingleFieldIndexConfig(
        indexName, indexType, fieldType, OPTION_FLAG_ALL);
    return dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
}

TEST_F(InvertedIndexInteTest, testUsage)
{
    auto indexFactory = std::make_shared<InvertedIndexFactory>();
    auto config = CreateSingleFieldIndexConfig(InvertedIndexType::it_number, "nid", ft_int32);

    ASSERT_TRUE(config);
    std::shared_ptr<docid64_t> currentBuildDocId(new docid64_t(0));

    // mem indexer
    indexlibv2::index::MemIndexerParameter indexParam;
    indexParam.currentBuildDocId = currentBuildDocId;
    auto memIndexer = indexFactory->CreateMemIndexer(config, indexParam);
    ASSERT_TRUE(memIndexer);
    ASSERT_TRUE(memIndexer->Init(config, nullptr).IsOK());

    // parser
    auto parser = indexFactory->CreateIndexFieldsParser();
    ASSERT_TRUE(parser);
    ASSERT_TRUE(parser->Init({config}, nullptr).IsOK());

    // parse
    indexlibv2::document::ExtendDocument extendDoc;
    std::shared_ptr<indexlibv2::document::RawDocument> rawDoc(
        indexlibv2::document::RawDocumentMaker::Make("cmd=add,nid=100").release());
    ASSERT_TRUE(rawDoc);
    extendDoc.SetRawDocument(rawDoc);
    bool hasFormatError = false;
    autil::mem_pool::Pool pool;
    auto fields = parser->Parse(extendDoc, &pool, hasFormatError);
    ASSERT_TRUE(fields);
    ASSERT_FALSE(hasFormatError);

    // serialize
    autil::DataBuffer dataBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &pool);
    fields->serialize(dataBuffer);
    autil::StringView serializedBuffer(dataBuffer.getData(), dataBuffer.getDataLen());
    auto newFields = parser->Parse(serializedBuffer, &pool);
    ASSERT_TRUE(newFields);

    // build
    ASSERT_TRUE(memIndexer->Build(newFields.get(), 1).IsOK());

    // dump

    // disk indexer
}

} // namespace indexlib::index
