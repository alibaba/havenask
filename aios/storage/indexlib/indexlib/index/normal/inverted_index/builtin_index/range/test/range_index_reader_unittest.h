#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_reader.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class RangeIndexReaderTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    RangeIndexReaderTest();
    ~RangeIndexReaderTest();

    DECLARE_CLASS_NAME(RangeIndexReaderTest);

    using DocVec = std::vector<std::pair<std::string, std::string>>;   // {pk, price}
    using QueryVec = std::vector<std::pair<std::string, std::string>>; // {range = [xxx, xxx], result}

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMMapLock();
    void TestMMapNoLock();
    void TestCache();
    void TestFewItems();
    void TestManyItemsMMapLock();
    void TestManyItemsMMapNoLock();
    void TestManyItemsCache();
    void TestConcurrencyQuery();
    void TestRangeIndexReaderTermIllegal();
    void TestMultiValueDocNumber();

private:
    void PreparePSM(const std::string& loadConfig, const std::string& loadName, const DocVec& docs,
                    bool enableFileCompress, test::PartitionStateMachine* psm);
    void InnerTestSimple(const std::string& loadConfig, bool useBlockCache);
    void InnerTestRead(const std::string& loadConfig, const std::string& loadName, const DocVec& docs,
                       const QueryVec& querys);
    void InnerTestManyItems(const std::string& loadConfig, const std::string& loadName);
    void InnerTestConcurrencyQuery(const std::string& loadConfig, const std::string& loadName);

private:
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;
    std::string mCacheLoadConfig;
    std::string mMMapLockLoadConfig;
    std::string mMMapNoLockLoadConfig;

private:
    IE_LOG_DECLARE();
};
INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(RangeIndexReaderTestMode, RangeIndexReaderTest, testing::Values(true, false));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RangeIndexReaderTestMode, TestMMapLock);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RangeIndexReaderTestMode, TestMMapNoLock);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RangeIndexReaderTestMode, TestCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RangeIndexReaderTestMode, TestFewItems);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RangeIndexReaderTestMode, TestManyItemsMMapLock);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RangeIndexReaderTestMode, TestManyItemsMMapNoLock);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RangeIndexReaderTestMode, TestManyItemsCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RangeIndexReaderTestMode, TestConcurrencyQuery);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RangeIndexReaderTestMode, TestRangeIndexReaderTermIllegal);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(RangeIndexReaderTestMode, TestMultiValueDocNumber);

}} // namespace indexlib::index
