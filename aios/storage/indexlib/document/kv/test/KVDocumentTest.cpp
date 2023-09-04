#include "indexlib/document/kv/KVDocument.h"

#include "autil/DataBuffer.h"
#include "autil/mem_pool/Pool.h"
#include "unittest/unittest.h"

namespace indexlibv2::document {

class KVDocumentTest : public TESTBASE
{
public:
    KVDocumentTest() = default;
    ~KVDocumentTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(KVDocumentTest, TestSimple)
{
    autil::mem_pool::Pool pool;
    KVDocument doc(&pool);
    doc._indexNameHash = 5;
    doc._schemaId = 6;
    autil::DataBuffer buffer;

    KVDocument doc1(&pool);
    doc1 = std::move(doc);
    ASSERT_EQ(doc._schemaId, doc1._schemaId);
    ASSERT_EQ(doc._indexNameHash, doc1._indexNameHash);

    KVDocument doc2(std::move(doc));
    ASSERT_EQ(doc._schemaId, doc2._schemaId);
    ASSERT_EQ(doc._indexNameHash, doc2._indexNameHash);

    auto doc3 = doc.Clone(&pool);
    ASSERT_EQ(doc._schemaId, doc3->_schemaId);
    ASSERT_EQ(doc._indexNameHash, doc3->_indexNameHash);
}

} // namespace indexlibv2::document
