#include "indexlib/index/kkv/test/closed_hash_prefix_key_table_unittest.h"

#include "indexlib/index/kkv/prefix_key_table_seeker.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, ClosedHashPrefixKeyTableTest);

ClosedHashPrefixKeyTableTest::ClosedHashPrefixKeyTableTest() {}

ClosedHashPrefixKeyTableTest::~ClosedHashPrefixKeyTableTest() {}

void ClosedHashPrefixKeyTableTest::CaseSetUp() {}

void ClosedHashPrefixKeyTableTest::CaseTearDown() {}

template <PKeyTableType Type>
void ClosedHashPrefixKeyTableTest::CheckReadAndWriteTable()
{
    testDataPathTearDown();
    testDataPathSetup();

    // write
    ClosedHashPrefixKeyTable<uint32_t, Type> writePKeyTable(10240, 60);
    writePKeyTable.Open(GET_SEGMENT_DIRECTORY(), PKOT_WRITE);
    ASSERT_TRUE(writePKeyTable.Insert(1, 10));
    ASSERT_TRUE(writePKeyTable.Insert(2, 20));
    ASSERT_TRUE(writePKeyTable.Insert(3, 30));
    writePKeyTable.Close();

    ASSERT_TRUE(GET_SEGMENT_DIRECTORY()->IsExist(PREFIX_KEY_FILE_NAME));

    // read
    ClosedHashPrefixKeyTable<uint32_t, Type> readPKeyTable(60);
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

void ClosedHashPrefixKeyTableTest::TestReadAndWriteTable()
{
    CheckReadAndWriteTable<PKT_DENSE>();
    CheckReadAndWriteTable<PKT_CUCKOO>();
}
void ClosedHashPrefixKeyTableTest::TestRWTable()
{
    CheckRWTable<PKT_DENSE>();
    CheckRWTable<PKT_CUCKOO>();
}

template <PKeyTableType Type>
void ClosedHashPrefixKeyTableTest::CheckRWTable()
{
    // write
    ClosedHashPrefixKeyTable<uint32_t, Type> rwTable(10240, 60);
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
    iter->SortByKey();
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
