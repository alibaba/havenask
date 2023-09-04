#include "indexlib/document/kv/KVDocumentBatch.h"

#include "autil/DataBuffer.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/document/kv/KVDocument.h"
#include "unittest/unittest.h"

namespace indexlibv2::document {

class KVDocumentBatchTest : public TESTBASE
{
public:
    KVDocumentBatchTest() = default;
    ~KVDocumentBatchTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(KVDocumentBatchTest, TestSimple)
{
    autil::mem_pool::Pool pool;
    std::shared_ptr<KVDocument> doc = std::make_shared<KVDocument>(&pool);
    doc->_indexNameHash = 5;
    doc->_schemaId = 6;
    autil::DataBuffer buffer;

    KVDocumentBatch batch;
    batch.AddDocument(doc);
    batch.serialize(buffer);

    KVDocumentBatch batch1;
    batch1.deserialize(buffer);
    ASSERT_EQ(1, batch1.GetAddedDocCount());
    std::shared_ptr<KVDocument> doc1 = std::dynamic_pointer_cast<KVDocument>(batch1[0]);
    ASSERT_EQ(5, doc1->_indexNameHash);
    ASSERT_EQ(6, doc1->_schemaId);
}

} // namespace indexlibv2::document
