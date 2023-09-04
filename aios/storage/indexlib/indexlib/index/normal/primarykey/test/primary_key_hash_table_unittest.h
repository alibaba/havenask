#ifndef __INDEXLIB_PRIMARYKEYHASHTABLETEST_H
#define __INDEXLIB_PRIMARYKEYHASHTABLETEST_H

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_hash_table.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class PrimaryKeyHashTableTest : public INDEXLIB_TESTBASE
{
public:
    PrimaryKeyHashTableTest();
    ~PrimaryKeyHashTableTest();

    DECLARE_CLASS_NAME(PrimaryKeyHashTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInsertAndLookUpPk();
    void TestGetBucketCount();

private:
    void InnerTestInsertAndLookUp(const std::string& kvStr, const std::string& queryStr);

    template <typename Key>
    void InnerTestInsertAndLookUpTyped(const std::string& kvStr, const std::string& queryStr);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyHashTableTest, TestInsertAndLookUpPk);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyHashTableTest, TestGetBucketCount);

//////////////////////////////////////////////////////////////////////////

template <typename Key>
void PrimaryKeyHashTableTest::InnerTestInsertAndLookUpTyped(const std::string& kvStr, const std::string& queryStr)
{
    std::vector<std::vector<std::string>> kvInfo;
    autil::StringUtil::fromString(kvStr, kvInfo, ":", ",");

    uint64_t docCount = kvInfo.size();
    std::set<std::string> pkSet;

    for (size_t i = 0; i < docCount; ++i) {
        assert(kvInfo[i].size() == 2);
        pkSet.insert(kvInfo[i][0]);
    }
    uint64_t pkCount = pkSet.size();

    size_t bufLen = PrimaryKeyHashTable<Key>::CalculateMemorySize(pkCount, docCount);
    char* buffer = new char[bufLen];
    {
        PrimaryKeyHashTable<Key> pkHashTable;
        pkHashTable.Init(buffer, pkCount, docCount);
        PKPair<Key> pkPair;
        for (size_t i = 0; i < docCount; ++i) {
            Key key = autil::StringUtil::fromString<Key>(kvInfo[i][0]);
            docid_t docId = autil::StringUtil::fromString<docid_t>(kvInfo[i][1]);
            pkPair.key = key;
            pkPair.docid = docId;
            pkHashTable.Insert(pkPair);
        }
    }
    {
        PrimaryKeyHashTable<Key> pkHashTable;
        pkHashTable.Init(buffer);
        std::vector<std::vector<std::string>> queryInfo;
        autil::StringUtil::fromString(queryStr, queryInfo, ":", ",");
        for (size_t i = 0; i < queryInfo.size(); ++i) {
            assert(queryInfo[i].size() == 2);
            Key key = autil::StringUtil::fromString<Key>(queryInfo[i][0]);
            docid_t expectedDocId = autil::StringUtil::fromString<docid_t>(queryInfo[i][1]);
            EXPECT_EQ(expectedDocId, pkHashTable.Find(key));
        }
    }
    delete[] buffer;
}
}} // namespace indexlib::index

#endif //__INDEXLIB_PRIMARYKEYHASHTABLETEST_H
