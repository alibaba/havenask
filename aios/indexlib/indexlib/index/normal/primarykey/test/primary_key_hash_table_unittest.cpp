#include "indexlib/index/normal/primarykey/test/primary_key_hash_table_unittest.h"
using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PrimaryKeyHashTableTest);

PrimaryKeyHashTableTest::PrimaryKeyHashTableTest()
{
}

PrimaryKeyHashTableTest::~PrimaryKeyHashTableTest()
{
}

void PrimaryKeyHashTableTest::CaseSetUp()
{
}

void PrimaryKeyHashTableTest::CaseTearDown()
{
}

void PrimaryKeyHashTableTest::TestInsertAndLookUpPk()
{
    // normal case
    InnerTestInsertAndLookUp("100:0,101:1,102:2,103:3,104:4",
                             "99:-1,100:0,101:1,102:2,103:3,104:4,105:-1");
    // test insert doc with duplicate pk
    InnerTestInsertAndLookUp("100:0,101:1,101:2,101:3,104:4",
                             "99:-1,100:0,101:3,104:4,105:-1");
}

void PrimaryKeyHashTableTest::InnerTestInsertAndLookUp(const string& kvStr,
                                                       const string& queryStr)
{
    InnerTestInsertAndLookUpTyped<uint64_t>(kvStr, queryStr);
    InnerTestInsertAndLookUpTyped<autil::uint128_t>(kvStr, queryStr);
}

void PrimaryKeyHashTableTest::TestGetBucketCount()
{
    ASSERT_EQ((size_t)7, PrimaryKeyHashTable<uint64_t>::GetBucketCount(1));
    ASSERT_EQ((size_t)1999, PrimaryKeyHashTable<uint64_t>::GetBucketCount(6));

    uint64_t docCount = numeric_limits<uint32_t>::max();
    ASSERT_TRUE(PrimaryKeyHashTable<uint64_t>::GetBucketCount(docCount) >
                docCount * PrimaryKeyHashTable<uint64_t>::BUCKET_COUNT_FACTOR);
    ASSERT_TRUE(PrimaryKeyHashTable<uint64_t>::GetBucketCount(docCount) < 
                2 * docCount);

    //check max docid
    docCount = numeric_limits<docid_t>::max();
    ASSERT_TRUE(PrimaryKeyHashTable<uint64_t>::GetBucketCount(docCount) >
                docCount * PrimaryKeyHashTable<uint64_t>::BUCKET_COUNT_FACTOR);
    ASSERT_TRUE(PrimaryKeyHashTable<uint64_t>::GetBucketCount(docCount) < 
                2 * docCount);

    //check normal
    docCount = 50000000;
    ASSERT_TRUE(PrimaryKeyHashTable<uint64_t>::GetBucketCount(docCount) >
                docCount * PrimaryKeyHashTable<uint64_t>::BUCKET_COUNT_FACTOR);
    ASSERT_TRUE(PrimaryKeyHashTable<uint64_t>::GetBucketCount(docCount) < 
                2 * docCount);
}
IE_NAMESPACE_END(index);

