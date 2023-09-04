#include "indexlib/index/kkv/test/separate_chain_prefix_key_table_unittest.h"

#include "indexlib/index/kkv/prefix_key_table_seeker.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SeparateChainPrefixKeyTableTest);

SeparateChainPrefixKeyTableTest::SeparateChainPrefixKeyTableTest() {}

SeparateChainPrefixKeyTableTest::~SeparateChainPrefixKeyTableTest() {}

void SeparateChainPrefixKeyTableTest::CaseSetUp() {}

void SeparateChainPrefixKeyTableTest::CaseTearDown() {}

void SeparateChainPrefixKeyTableTest::TestReadAndWriteTable()
{
    // write
    SeparateChainPrefixKeyTable<uint32_t> writePKeyTable(10, 10240);
    writePKeyTable.Open(GET_SEGMENT_DIRECTORY(), PKOT_WRITE);
    ASSERT_TRUE(writePKeyTable.Insert(1, 10));
    ASSERT_TRUE(writePKeyTable.Insert(2, 20));
    ASSERT_TRUE(writePKeyTable.Insert(3, 30));
    writePKeyTable.Close();

    ASSERT_TRUE(GET_SEGMENT_DIRECTORY()->IsExist(PREFIX_KEY_FILE_NAME));

    // read
    SeparateChainPrefixKeyTable<uint32_t> readPKeyTable;
    readPKeyTable.Open(GET_SEGMENT_DIRECTORY(), PKOT_READ);

    uint32_t* value = NULL;
    value = readPKeyTable.Find(1);
    ASSERT_TRUE(value != NULL);
    ASSERT_EQ((uint32_t)10, *value);

    value = readPKeyTable.Find(2);
    ASSERT_TRUE(value != NULL);
    ASSERT_EQ((uint32_t)20, *value);

    value = readPKeyTable.Find(3);
    ASSERT_TRUE(value != NULL);
    ASSERT_EQ((uint32_t)30, *value);

    value = readPKeyTable.Find(4);
    ASSERT_TRUE(value == NULL);

    value = PrefixKeyTableSeeker::Find(&readPKeyTable, 1);
    ASSERT_TRUE(value != NULL);
    ASSERT_EQ((uint32_t)10, *value);

    value = PrefixKeyTableSeeker::Find(&readPKeyTable, 4);
    ASSERT_TRUE(value == NULL);
}

void SeparateChainPrefixKeyTableTest::TestRWTable()
{
    // write
    SeparateChainPrefixKeyTable<uint32_t> rwTable(10, 10240);
    rwTable.Open(GET_SEGMENT_DIRECTORY(), PKOT_RW);
    ASSERT_TRUE(rwTable.Insert(1, 10));
    ASSERT_TRUE(rwTable.Insert(2, 20));
    ASSERT_TRUE(rwTable.Insert(3, 30));

    // read
    uint32_t* valuePtr = NULL;
    valuePtr = rwTable.Find(1);
    ASSERT_TRUE(valuePtr != NULL);
    ASSERT_EQ((uint32_t)10, *valuePtr);

    valuePtr = rwTable.Find(2);
    ASSERT_TRUE(valuePtr != NULL);
    ASSERT_EQ((uint32_t)20, *valuePtr);

    valuePtr = rwTable.Find(3);
    ASSERT_TRUE(valuePtr != NULL);
    ASSERT_EQ((uint32_t)30, *valuePtr);

    valuePtr = rwTable.Find(4);
    ASSERT_TRUE(valuePtr == NULL);

    // check iterator
    PrefixKeyTableBase<uint32_t>::IteratorPtr iter = rwTable.CreateIterator();
    uint64_t key;
    uint32_t value;
    ASSERT_TRUE(iter->IsValid());
    iter->Get(key, value);
    ASSERT_EQ((uint64_t)1, key);
    ASSERT_EQ((uint32_t)10, value);
    iter->MoveToNext();

    ASSERT_TRUE(iter->IsValid());
    iter->Get(key, value);
    ASSERT_EQ((uint64_t)2, key);
    ASSERT_EQ((uint32_t)20, value);
    iter->MoveToNext();

    ASSERT_TRUE(iter->IsValid());
    iter->Get(key, value);
    ASSERT_EQ((uint64_t)3, key);
    ASSERT_EQ((uint32_t)30, value);
    iter->MoveToNext();

    ASSERT_FALSE(iter->IsValid());
}
}} // namespace indexlib::index
