#include "indexlib/index/kkv/pkey_table/ClosedHashPrefixKeyTable.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableSeeker.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::index {

class ClosedHashPrefixKeyTableTest : public TESTBASE
{
public:
    ClosedHashPrefixKeyTableTest() {}
    ~ClosedHashPrefixKeyTableTest() {}

public:
    void setUp() override
    {
        // prepare fs
        auto testRoot = GET_TEMP_DATA_PATH();
        auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
        _rootDir = indexlib::file_system::IDirectory::Get(fs);
    }
    void tearDown() override {}

    void TestReadAndWriteTable();
    void TestRWTable();

private:
    template <PKeyTableType Type>
    void CheckReadAndWriteTable();
    template <PKeyTableType Type>
    void CheckRWTable();

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, ClosedHashPrefixKeyTableTest);

template <PKeyTableType Type>
void ClosedHashPrefixKeyTableTest::CheckReadAndWriteTable()
{
    testDataPathTearDown();
    testDataPathSetup();

    // write
    ClosedHashPrefixKeyTable<uint32_t, Type> writePKeyTable(10240, 60);
    writePKeyTable.Open(_rootDir, PKeyTableOpenType::WRITE);
    ASSERT_TRUE(writePKeyTable.Insert(1, 10));
    ASSERT_TRUE(writePKeyTable.Insert(2, 20));
    ASSERT_TRUE(writePKeyTable.Insert(3, 30));
    writePKeyTable.Close();

    ASSERT_TRUE(_rootDir->IsExist(PREFIX_KEY_FILE_NAME).GetOrThrow());

    // read
    ClosedHashPrefixKeyTable<uint32_t, Type> readPKeyTable(60);
    readPKeyTable.Open(_rootDir, PKeyTableOpenType::READ);

    uint32_t* value = nullptr;
    value = readPKeyTable.Find(1);
    ASSERT_NE(nullptr, value);
    ASSERT_EQ((uint32_t)10, *value);

    value = readPKeyTable.Find(2);
    ASSERT_NE(nullptr, value);
    ASSERT_EQ((uint32_t)20, *value);

    value = readPKeyTable.Find(3);
    ASSERT_NE(nullptr, value);
    ASSERT_EQ((uint32_t)30, *value);

    value = readPKeyTable.Find(4);
    ASSERT_EQ(nullptr, value);

    value = PrefixKeyTableSeeker::Find(&readPKeyTable, 1);
    ASSERT_NE(nullptr, value);
    ASSERT_EQ((uint32_t)10, *value);

    value = PrefixKeyTableSeeker::Find(&readPKeyTable, 4);
    ASSERT_EQ(nullptr, value);
}

template <PKeyTableType Type>
void ClosedHashPrefixKeyTableTest::CheckRWTable()
{
    // write
    ClosedHashPrefixKeyTable<uint32_t, Type> rwTable(10240, 60);
    rwTable.Open(_rootDir, PKeyTableOpenType::RW);
    ASSERT_TRUE(rwTable.Insert(1, 10));
    ASSERT_TRUE(rwTable.Insert(2, 20));
    ASSERT_TRUE(rwTable.Insert(3, 30));

    // read
    uint32_t* valuePtr = nullptr;
    valuePtr = rwTable.Find(1);
    ASSERT_NE(nullptr, valuePtr);
    ASSERT_EQ((uint32_t)10, *valuePtr);

    valuePtr = rwTable.Find(2);
    ASSERT_NE(nullptr, valuePtr);
    ASSERT_EQ((uint32_t)20, *valuePtr);

    valuePtr = rwTable.Find(3);
    ASSERT_NE(nullptr, valuePtr);
    ASSERT_EQ((uint32_t)30, *valuePtr);

    valuePtr = rwTable.Find(4);
    ASSERT_EQ(nullptr, valuePtr);

    // check iterator
    auto iter = rwTable.CreateIterator();
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

TEST_F(ClosedHashPrefixKeyTableTest, TestReadAndWriteTable)
{
    CheckReadAndWriteTable<PKeyTableType::DENSE>();
    CheckReadAndWriteTable<PKeyTableType::CUCKOO>();
}

TEST_F(ClosedHashPrefixKeyTableTest, TestRWTable)
{
    CheckRWTable<PKeyTableType::DENSE>();
    CheckRWTable<PKeyTableType::CUCKOO>();
}

} // namespace indexlibv2::index
