#ifndef __INDEXLIB_KVTABLEINTETESTTEST_H
#define __INDEXLIB_KVTABLEINTETESTTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

// first bool : compress & use package file
class KVTableInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    KVTableInteTest();
    ~KVTableInteTest();

    DECLARE_CLASS_NAME(KVTableInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestBuildReadWithDiffType();
    void TestNumberHash();
    void TestSearchWithTimestamp();
    void TestSearchWithTimestampForDefalutTTL();
    void TestSearchWithMultiSegment();
    void TestSearchKVWithDelete();
    void TestAddSameDocs();
    void TestAddKVWithoutValue();
    void TestBuildShardingSegment();
    void TestBuildLegacyKvDoc();
    void TestMergeTwoSegmentsSimple();
    void TestMergeSingleSegment();
    void TestMergeSameKey();
    void TestMergeDeleteKey();
    void TestMergeOptimizeSingleSegmentWithSharding();
    void TestNoFullBuildData();
    void TestMultiPartitionMerge();
    void TestMergeWithSeparateMode();
    void TestMergeWithEmptyColumn();
    void TestOptimizeMergeMultipleLevel();
    void TestMergeWithTTL();
    void TestMergeWithUserTs();
    void TestMergeWithDefaultTTL();
    void TestMergeWithTTLWhenAllDead();
    void TestMergeUntilBottomLevel();
    void TestShardBasedMergeWithBadDocs();

    void TestHashTableOneField();
    void TestHashTableOneStringField();
    void TestHashTableOneMultiField();
    void TestHashTableMultiField();
    void TestHashTableVarIndexMerge();
    void TestHashTableVarValueUseBlockCache();
    void TestHashTableVarKeyUseBlockCache();
    void TestHashTableFixUseBlockCache();
    void TestHashTableFixVarValue();
    void TestKVFormatOption();

    void TestAddTrigerDump();
    void TestOffsetLimitTriggerForceDump();
    void TestShortOffsetWithDeleteDoc();
    void TestMergeWithShortOffset();

    void TestReopenIncWithoutRt();
    void TestReopenIncOlderThanAllRt();
    void TestReopenIncCoverPartRtBuiltSegment();
    void TestReopenIncCoverPartRtBuildingSegment();
    void TestReopenIncCoverAllRt();
    void TestReopenIncKeyAndRtKeyInSameSecond();
    void TestReopenIncCoverBuiltAndBuildingSegment();
    void TestReopenIncCoverBuiltSegmentWhenReaderHold();

    void TestForceReopenIncWithoutRt();
    void TestForceReopenIncOlderThanAllRt();
    void TestForceReopenIncCoverPartRtBuiltSegment();
    void TestForceReopenIncCoverPartRtBuildingSegment();
    void TestForceReopenIncCoverAllRt();
    void TestForceReopenIncKeyAndRtKeyInSameSecond();

    void TestOpenWithoutEnoughMemory();
    void TestReopenWithoutEnoughMemory();
    void TestFlushRealtimeIndex();
    void TestCleanRealtimeIndex();
    void TestReleaseMemoryForTmpValueFile();

    void TestKeyIsExitError();
    void TestAsyncDumpWithIncReopen();
    void TestRtLoadBuiltRtSegmentWithIncCoverCore();
    void TestRtIndexSizeWhenUseMultiValue();
    void TestProhibitInMemDumpBug_13765192();
    void TestBugFix14177981();

    void TestBuildFixLengthWithLegacyFormat();
    void TestBuildFixLengthString();

    void TestCompactHashKeyForFixHashTable();
    void TestCompactHashKeyForVarHashTable();
    void TestAdaptiveReadWithShortOffsetReader();
    void TestShortOffset();
    void TestLegacyFormat();
    void TestMultiFloatField();
    void TestCompressedFixedLengthFloatField();
    void TestMergeWithCompressAndShortOffsetIsDisable();
    void testMergeMultiPartWithTTL();

    void TestPartitionInfo();
    void TestFixLenValueAutoInline();
    void TestCompactValueWithDenseHash();
    void TestCompactValueWithCuckooHash();
    void TestCompactValueWithDelete();
    void TestMergeNoBottomWithNoDelete();
    void TestCompressFloatValueAutoInline();
    void TestSingleFloatCompress();
    void TestDumpingSearchCache();
    void TestMetric();
    void TestCompatableError();

private:
    typedef std::pair<uint32_t, std::vector<segmentid_t>> LevelMetaInfo;
    void CheckLevelInfo(const std::vector<LevelMetaInfo>& levelInfos);
    std::string prepareDoc(int64_t baseKey, int64_t docCount, std::vector<int64_t> deleteKeys, int64_t timetamp);
    void InnerTestMergeWithShortOffset(const std::string& hashType, bool enableTTL, bool enableShortOffset);
    void InnerTestMergeWithShortOffsetTriggerHashCompress(const std::string& hashType, bool enableTTL);

    void InnerTestKVFormatOption(bool isShortOffset);

    void InnerTestCompactHashKey(bool isVarHashTable, const std::string& keyType, const std::string& dictType,
                                 bool expectCompact);

    void InnerTestShortOffset(const std::string& keyType, const std::string& dictType, bool enableTTL, bool useCompress,
                              bool useShortOffset);

    void InnerTestLegacyFormat(const std::string& keyType, const std::string& dictType, bool enableTTL,
                               bool useCompress);

    void InnerTestHashTableFixVarValue(bool cacheLoad);
    void InnerTestBuildFixLengthWithLegacyFormat(bool cacheLoad);
    void InnerTestHashTableVarValueUseBlockCache(bool cacheDecompressFile);

    void InnerTestBuildSearch(std::string fieldType, bool hasTTL,
                              const config::IndexPartitionOptions& options = config::IndexPartitionOptions());
    void InnerTestBuildForceDump(bool enableCompactHashKey, bool enableShortOffset, size_t maxValueFileSize,
                                 std::string hashType, size_t totalDocSize, size_t segmentNum, std::string dirName);
    void CheckMergeResult(const test::PartitionStateMachine& psm, versionid_t versionId, const std::string& docCount,
                          const std::string& keyCount, const std::vector<std::vector<segmentid_t>> levelInfo = {});
    void MakeOnePartitionData(const std::string& partRootDir, const std::string& docStrs);

    void DoTestReopenIncWithoutRt(bool isForce, bool flushRt);
    void DoTestReopenIncOlderThanAllRt(bool isForce, bool flushRt);
    void DoTestReopenIncCoverPartRtBuiltSegment(bool isForce, bool flushRt);
    void DoTestReopenIncCoverPartRtBuildingSegment(bool isForce, bool flushRt);
    void DoTestReopenIncCoverAllRt(bool isForce, bool flushRt);
    void DoTestReopenIncKeyAndRtKeyInSameSecond(bool isForce, bool flushRt);
    void DoTestReopenIncCoverBuiltAndBuildingSegment(bool flushRt);
    std::string GetCompressType() const;

    void MakeEmptyFullIndexWithIndexFormatVersion(const config::IndexPartitionSchemaPtr& schema,
                                                  const config::IndexPartitionOptions& options,
                                                  const std::string& formatVersionStr, const std::string& rootPath);
    void InnerTestMergeWithCompressAndShortOffsetIsDisable(const std::string& hashType,
                                                           const std::string& compressType);

protected:
    virtual config::IndexPartitionSchemaPtr CreateSchema(const std::string& fields, const std::string& key,
                                                         const std::string& values, int64_t ttl = INVALID_TTL);
    virtual std::vector<document::DocumentPtr> CreateKVDocuments(const config::IndexPartitionSchemaPtr& schema,
                                                                 const std::string& docStrs);

protected:
    config::IndexPartitionSchemaPtr mSchema;
    std::string mField;
    config::IndexPartitionOptions mOptions;
    bool mImpactValue;
    bool mPlainFormat;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition

#endif //__INDEXLIB_KVTABLEINTETEST_H
