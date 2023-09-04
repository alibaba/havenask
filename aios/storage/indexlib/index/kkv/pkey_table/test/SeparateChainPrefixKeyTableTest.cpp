#include "indexlib/index/kkv/pkey_table/SeparateChainPrefixKeyTable.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableSeeker.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::index {

class SeparateChainPrefixKeyTableTest : public TESTBASE
{
public:
    void setUp() override;

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, SeparateChainPrefixKeyTableTest);

void SeparateChainPrefixKeyTableTest::setUp()
{
    auto tempDir = GET_TEMP_DATA_PATH();
    auto testFs = indexlib::file_system::FileSystemCreator::Create("test", tempDir).GetOrThrow();
    _rootDir = indexlib::file_system::IDirectory::Get(testFs);
}

TEST_F(SeparateChainPrefixKeyTableTest, TestReadAndWriteTable)
{
    // write
    SeparateChainPrefixKeyTable<uint32_t> writePKeyTable(10, 10240);
    writePKeyTable.Open(_rootDir, PKeyTableOpenType::WRITE);
    ASSERT_TRUE(writePKeyTable.Insert(1, 10));
    ASSERT_TRUE(writePKeyTable.Insert(2, 20));
    ASSERT_TRUE(writePKeyTable.Insert(3, 30));
    writePKeyTable.Close();

    ASSERT_TRUE(_rootDir->IsExist(PREFIX_KEY_FILE_NAME).GetOrThrow());

    // read
    SeparateChainPrefixKeyTable<uint32_t> readPKeyTable;
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

TEST_F(SeparateChainPrefixKeyTableTest, TestRWTable)
{
    // write
    SeparateChainPrefixKeyTable<uint32_t> rwTable(10, 10240);
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

} // namespace indexlibv2::index
