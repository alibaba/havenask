#pragma once

#include "indexlib/common_define.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/index/normal/primarykey/sorted_primary_key_formatter.h"
#include "indexlib/index/normal/primarykey/test/mock_primary_key_loader_plan.h"
#include "indexlib/index/normal/primarykey/test/primary_key_formatter_helper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace indexlib::file_system;
namespace indexlib { namespace index {

class SortedPrimaryKeyFormatterTest : public INDEXLIB_TESTBASE
{
public:
    SortedPrimaryKeyFormatterTest();
    ~SortedPrimaryKeyFormatterTest();

    DECLARE_CLASS_NAME(SortedPrimaryKeyFormatterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestFind();
    void TestFindManyItems();
    void TestSerializeFromHashMap();
    void TestDeserializeToSliceFile();
    void TestEstimateDirectLoadSizeUint64();
    void TestEstimateDirectLoadSizeUint128();

private:
    void CheckSliceFile(const file_system::FileWriterPtr& sliceFile, const std::string& docs,
                        const std::string& expectDocids);

    template <typename Key>
    void InnerTestEstimateDirectLoadSize(bool isLock, const vector<PKPair<Key>>& data);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyFormatterTest, TestFind);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyFormatterTest, TestFindManyItems);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyFormatterTest, TestSerializeFromHashMap);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyFormatterTest, TestDeserializeToSliceFile);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyFormatterTest, TestEstimateDirectLoadSizeUint64);
INDEXLIB_UNIT_TEST_CASE(SortedPrimaryKeyFormatterTest, TestEstimateDirectLoadSizeUint128);

template <typename Key>
void SortedPrimaryKeyFormatterTest::InnerTestEstimateDirectLoadSize(bool isLock, const vector<PKPair<Key>>& data)
{
    DirectoryPtr directory =
        PrimaryKeyFormatterHelper::PrepareMmapDirectory(isLock, GET_TEMP_DATA_PATH() + "/sorted_pk/");
    SortedPrimaryKeyFormatter<Key> formatter;
    string pkFileName = PrimaryKeyFormatterHelper::PreparePkData<Key>(data, formatter, directory);

    MockPrimaryKeyLoaderPlanPtr newLoadPlan(new MockPrimaryKeyLoaderPlan());
    EXPECT_CALL(*newLoadPlan, GetTargetFileName()).WillRepeatedly(Return(pkFileName));
    EXPECT_CALL(*newLoadPlan, GetTargetFileDirectory()).WillRepeatedly(Return(directory));

    size_t expected = isLock ? data.size() * sizeof(PKPair<Key>) : 0;
    ASSERT_EQ(expected, formatter.EstimateDirectLoadSize(newLoadPlan));
}
}} // namespace indexlib::index
