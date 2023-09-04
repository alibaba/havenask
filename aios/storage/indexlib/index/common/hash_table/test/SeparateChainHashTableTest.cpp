#include "indexlib/index/common/hash_table/SeparateChainHashTable.h"

#include "autil/Log.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlibv2::index {

class SeparateChainHashTableTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    SeparateChainHashTableTest();
    ~SeparateChainHashTableTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, SeparateChainHashTableTest);

SeparateChainHashTableTest::SeparateChainHashTableTest() {}

SeparateChainHashTableTest::~SeparateChainHashTableTest() {}

void SeparateChainHashTableTest::CaseSetUp() {}

void SeparateChainHashTableTest::CaseTearDown() {}

TEST_F(SeparateChainHashTableTest, TestSimpleProcess)
{
    typedef SeparateChainHashTable<int64_t, uint32_t> HashTable;
    HashTable table;
    table.Init(4, sizeof(HashTable::KeyNode) * 1000);
    table.Insert(8, 0);

    HashTable::KeyNode* node = table.Find(8);
    ASSERT_TRUE(node);
    ASSERT_EQ(uint32_t(0), node->value);
    ASSERT_EQ(int64_t(8), node->key);

    node = table.Find(4);
    ASSERT_FALSE(node);

    table.Insert(4, 1);
    node = table.Find(4);
    ASSERT_TRUE(node);
    ASSERT_EQ(int64_t(4), node->key);
    ASSERT_EQ(uint32_t(1), node->value);
}
} // namespace indexlibv2::index