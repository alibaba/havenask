#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/index/inverted_index/config/HighFrequencyVocabulary.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/memory_control/QuotaControl.h"

namespace indexlib::index::legacy {

class TruncateIndexTest : public INDEXLIB_TESTBASE
{
public:
    TruncateIndexTest();
    ~TruncateIndexTest();

    DECLARE_CLASS_NAME(TruncateIndexTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForNoTruncateByAttribute();
    void TestCaseForNoTruncateByDocPayload();

    void TestCaseForTruncateByAttributeWithoutDistinct_1();
    void TestCaseForTruncateByAttributeWithoutDistinct_2();
    void TestCaseForTruncateByDocPayloadWithoutDistinct();
    void TestCaseForTruncateByAttributeWithDistinctNoExpand();
    void TestCaseForTruncateByDocPayloadWithDistinctNoExpand();
    void TestCaseForTruncateByAttributeWithDistinctExpand();
    void TestCaseForTruncateByDocPayloadWithDistinctExpand();

    void TestCaseForDocPayloadWithoutDistinctWithDefautLimit();
    void TestCaseForDocPayloadWithoutDistinct();
    void TestCaseForDocPayloadWithDistinctNoExpand();
    void TestCaseForDocPayloadWithDistinctExpand();
    void TestCaseForDocPayloadWithDistinctExpand2();
    void TestCaseForDocPayloadWithDistinctExpand3();

    void TestCaseForDocPayloadWithNoDocLeftForSomeTerm();
    void TestCaseForTruncateWithOnlyBitmap();
    void TestCaseForReadWithOnlyBitmap();
    void TestCaseForReadTruncateWithOnlyBitmap();
    void TestCaseForTruncateByMultiAttribute();
    void TestCaseForTruncateByAttributeAndDocPayload();
    void TestCaseForTruncateMetaStrategy();
    void TestCaseForTruncateByDocPayloadMetaStrategy();
    void TestCaseForTruncateMetaStrategyWithNoDoc();
    void TestCaseForTruncateWithEmptySegment();
    void TestCaseForInvalidConfig();
    void TestCaseForTruncateWithBalanceTreeMerge();
    void TestCaseForInvalidBeginTime();
    void TestCaseForCheckTruncateMeta();
    void TestCaseForCheckTruncateMetaWithDiffSortValue();
    void TestCaseForReTruncate();
    void TestCaseForCreateBucketMapsExceptionAndRecover();
    void TestCaseForIOExceptionShutDown();

    void TestCaseForTruncateByAttributeWithoutDistinctWithTestSplit_1();
    void TestCaseForTruncateByAttributeWithoutDistinctWithTestSplit_2();
    void TestCaseForTruncateByDocPayloadWithoutDistinctWithTestSplit_2();
    void TestCaseForTruncateByAttributeWithoutDistinctWithTestSplitWithEmpty_1();
    void TestCaseForTruncateByAttributeWithoutDistinctWithTestSplitWithEmpty_2();
    void TestCaseForTruncateByMultiAttributeWithTestSplit_1();
    void TestCaseForTruncateByMultiAttributeWithTestSplit_2();
    void TestCaseForTruncateByAttributeWithDistinctNoExpandWithTestSplit();
    void TestCaseForTruncateByAttributeWithDistinctExpandWithTestSplit();

private:
    partition::IndexPartitionReaderPtr CreatePartitionReader();
    void InnerTestForTruncate(const std::string& docStr, const std::string& expecetedTruncDocStr,
                              const std::string& expectedDocIds, const std::string& toCheckTokenStr,
                              const config::TruncateParams& param, const std::string& truncIndexNameStr,
                              bool hasHighFreq);

    void InnerTestForBitmapTruncate(const std::string& docStr, const std::string& expecetedTruncDocStr,
                                    const std::string& expectedDocIds, const std::string& toCheckTokenStr,
                                    const config::TruncateParams& param, const std::string& truncIndexNameStr,
                                    const std::string& expectedHighFreqTermDocIds);

    void BuildTruncateIndex(const config::IndexPartitionSchemaPtr& schema, const std::string& rawDocStr,
                            bool needMerge = true);

    void CheckTruncateIndex(const std::string& expectedTrucDocStr, const std::string& expectedDocIds,
                            const std::string& toCheckTokenStr, const std::string& indexNameStr);

    void CheckBitmapTruncateIndex(const std::string& expectedDocIds, const std::string& indexNameStr);

    void SetHighFrequenceDictionary(const std::string& indexName, bool hasHighFreq, HighFrequencyTermPostingType type);

    void ResetIndexPartitionSchema(const config::TruncateParams& param);

    void CreateTruncateMeta(const std::string& truncateName, const std::string& tokenStr, const std::string& valueStr,
                            const std::string& indexName = "", bool useArchiveFolder = true);

    void GetMetaFromMetafile(std::map<dictkey_t, int>& mapMeta, std::string truncate_index_name);

    void CheckTruncateMeta(std::string keys, std::string values, const std::map<dictkey_t, int>& mapMeta);

    std::string LoadOptionConfigString(const std::string& optionFile);

    void InnerTestForTruncateWithTestSplit(const std::string& docStr, const std::string& expecetedTruncDocStr,
                                           const std::string& expectedDocIds, const std::string& toCheckTokenStr,
                                           const config::TruncateParams& param, const std::string& truncIndexNameStr,
                                           bool hasHighFreq, int segmentCount = 3);

    void InnerTestForBitmapTruncateWithTestSplit(const std::string& docStr, const std::string& expecetedTruncDocStr,
                                                 const std::string& expectedDocIds, const std::string& toCheckTokenStr,
                                                 const config::TruncateParams& param,
                                                 const std::string& truncIndexNameStr,
                                                 const std::string& expectedHighFreqTermDocIds, int segmentCount = 3);

private:
    std::string mRootDir;
    std::string mRawDocStr;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    util::QuotaControlPtr mQuotaControl;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForNoTruncateByAttribute);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForNoTruncateByDocPayload);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByAttributeWithoutDistinct_1);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByAttributeWithoutDistinct_2);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByDocPayloadWithoutDistinct);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByAttributeWithDistinctNoExpand);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByDocPayloadWithDistinctNoExpand);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByAttributeWithDistinctExpand);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByDocPayloadWithDistinctExpand);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForDocPayloadWithoutDistinctWithDefautLimit);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForDocPayloadWithoutDistinct);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForDocPayloadWithDistinctNoExpand);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForDocPayloadWithDistinctExpand);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForDocPayloadWithDistinctExpand2);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForDocPayloadWithDistinctExpand3);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForDocPayloadWithNoDocLeftForSomeTerm);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateWithOnlyBitmap);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForReadWithOnlyBitmap);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForReadTruncateWithOnlyBitmap);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByMultiAttribute);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByAttributeAndDocPayload);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateMetaStrategy);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByDocPayloadMetaStrategy);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateMetaStrategyWithNoDoc);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateWithEmptySegment);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForInvalidConfig);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateWithBalanceTreeMerge);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForInvalidBeginTime);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForCheckTruncateMeta);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForCheckTruncateMetaWithDiffSortValue);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForReTruncate);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByAttributeWithoutDistinctWithTestSplit_1);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByAttributeWithoutDistinctWithTestSplit_2);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByDocPayloadWithoutDistinctWithTestSplit_2);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByAttributeWithoutDistinctWithTestSplitWithEmpty_1);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByAttributeWithoutDistinctWithTestSplitWithEmpty_2);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByMultiAttributeWithTestSplit_1);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByMultiAttributeWithTestSplit_2);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByAttributeWithDistinctNoExpandWithTestSplit);
INDEXLIB_UNIT_TEST_CASE(TruncateIndexTest, TestCaseForTruncateByAttributeWithDistinctExpandWithTestSplit);
} // namespace indexlib::index::legacy
