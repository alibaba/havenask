#include "indexlib/common/hash_table/test/separate_chain_hash_table_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, SeparateChainHashTableTest);

SeparateChainHashTableTest::SeparateChainHashTableTest()
{
}

SeparateChainHashTableTest::~SeparateChainHashTableTest()
{
}

void SeparateChainHashTableTest::CaseSetUp()
{
}

void SeparateChainHashTableTest::CaseTearDown()
{
}

void SeparateChainHashTableTest::TestSimpleProcess()
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

IE_NAMESPACE_END(common);

