#ifndef __INDEXLIB_KKVTABLETEST_H
#define __INDEXLIB_KKVTABLETEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/config/build_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

namespace {
const int64_t MAX_DIST_PKEY_COUNT = 5;
const int64_t MAX_SKEY_COUNT_PER_PKEY = 10;
const int64_t SKEY_COUNT_LIMITS = 3;
const int64_t KKV_TEST_TTL = 10;
} // namespace

class ExpectResultInfos
{
public:
    struct SkeyInfo {
        SkeyInfo() : value(0), ts(0) {}

        SkeyInfo(const std::string& _skey, uint32_t _value, int32_t _ts) : skey(_skey), value(_value), ts(_ts) {}

        bool operator==(const SkeyInfo& other) const { return skey == other.skey; }

        std::string skey;
        uint32_t value;
        int32_t ts;
    };
    typedef std::vector<SkeyInfo> SKeyInfoVec;
    typedef std::map<std::string, SKeyInfoVec> PKeySKeyInfoMap;
    class ExpectResultInfoComp
    {
    public:
        ExpectResultInfoComp(const std::string& sortParamStr) : mSortParams(config::StringToSortParams(sortParamStr)) {}

        bool operator()(const SkeyInfo& lft, const SkeyInfo& rht);

    private:
        config::SortParams mSortParams;
    };

public:
    ExpectResultInfos() : mMaxTs(0), mIndexSize(0) {}

    int32_t GetMaxTs() const { return mMaxTs; }
    size_t GetIndexSize() const { return mIndexSize; }

    void AddSkey(const std::string& pkey, const std::string& skey, uint32_t value, int32_t ts)
    {
        mMaxTs = std::max(mMaxTs, ts);
        SKeyInfoVec& skeyInfoVec = mInfoMap[pkey];
        SkeyInfo info(skey, value, ts);
        SKeyInfoVec::iterator iter = std::find(skeyInfoVec.begin(), skeyInfoVec.end(), info);
        if (iter == skeyInfoVec.end()) {
            skeyInfoVec.push_back(info);
        } else {
            *iter = info;
        }
    }

    void DelSkey(const std::string& pkey, const std::string& skey, int32_t ts)
    {
        mMaxTs = std::max(mMaxTs, ts);
        SKeyInfoVec& skeyInfoVec = mInfoMap[pkey];
        SkeyInfo info(skey, 0, ts);
        SKeyInfoVec::iterator iter = std::find(skeyInfoVec.begin(), skeyInfoVec.end(), info);
        if (iter != skeyInfoVec.end()) {
            skeyInfoVec.erase(iter);
        }
    }

    void DelPkey(const std::string& pkey, int32_t ts)
    {
        mMaxTs = std::max(mMaxTs, ts);
        mInfoMap[pkey].clear();
    }

    const PKeySKeyInfoMap& GetInfoMap() const { return mInfoMap; }

    void RemoveObseleteItems(int64_t deadLineTs)
    {
        PKeySKeyInfoMap::iterator iter = mInfoMap.begin();
        for (; iter != mInfoMap.end(); iter++) {
            SKeyInfoVec& skeyInfos = iter->second;
            SKeyInfoVec remainInfos;
            for (size_t i = 0; i < skeyInfos.size(); i++) {
                if ((int64_t)skeyInfos[i].ts >= deadLineTs) {
                    remainInfos.push_back(skeyInfos[i]);
                }
            }
            skeyInfos.assign(remainInfos.begin(), remainInfos.end());
        }
    }

    void TruncateItems(uint32_t topNum, const std::string& sortParamStr)
    {
        ExpectResultInfoComp comp(sortParamStr);
        PKeySKeyInfoMap::iterator iter = mInfoMap.begin();
        for (; iter != mInfoMap.end(); iter++) {
            SKeyInfoVec& skeyInfos = iter->second;
            if (skeyInfos.size() > topNum) {
                std::sort(skeyInfos.begin(), skeyInfos.end(), comp);
                skeyInfos.resize(topNum);
            }
        }
    }

private:
    PKeySKeyInfoMap mInfoMap;
    int32_t mMaxTs;
    size_t mIndexSize;
};

// first bool: str hash; second bool: use file compress;
// third bool: flush rt; fourth bool: use package file
class KKVTableTest : public INDEXLIB_TESTBASE_WITH_PARAM<std::tuple<index::PKeyTableType, bool, bool, bool, bool, bool>>
{
public:
    KKVTableTest();
    ~KKVTableTest();

    DECLARE_CLASS_NAME(KKVTableTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestSearchPkey();
    void TestSearchOnlyRt();
    void TestSearchPkeyAndSkeys();
    void TestPkeyWithDifferentType();
    void TestSkeyWithDifferentType();
    void TestDeleteDocs();
    void TestDeletePKeys();
    void TestDeleteSKeys();
    void TestSearchWithTimestamp();
    void TestSearchSkeysWithOptimization();
    void TestBuildSegmentInLevel0();
    void TestBuildShardingSegment();
    void TestMergeTwoSegmentSimple();
    void TestMergeDuplicateSKey();
    void TestMergeDuplicateSKey_13227243();
    void TestMergeDeleteSKey();
    void TestMergeDeletePKey();
    void TestMergeShardingSimple();
    void TestMergeWithCountLimit();
    void TestMergeWithOneColumnHasNoKey();

    void TestFullBuild();
    void TestIncBuildNoMerge();
    void TestIncBuildOptimizeMerge_1();
    void TestIncBuildOptimizeMerge_2();
    void TestIncBuildOptimizeMerge_3();
    void TestIncBuildOptimizeMerge_4();
    void TestRtBuild_1();
    void TestRtBuild_2();
    void TestShardBasedMerge();
    void TestShardBasedMergeWithBadDocs();
    void TestMergeClearTable();
    void TestMultiPartMerge();
    void TestNoNeedOptimizeMerge();
    void TestNoNeedShardBasedMerge();
    void TestMergeMultiTimesLongCaseTest();
    void TestFullMergeWithSKeyTruncate_1();
    void TestFullMergeWithSKeyTruncate_2();
    void TestFullMergeWithSKeyTruncate_3();
    void TestRtWithSKeyTruncate();

    void TestLongValue();
    void TestReopen();
    void TestSearchWithBlockCache();

    void TestFlushRealtimeIndex();
    void TestBuildShardingSegmentWithLegacyDoc();
    void TestOptimizeMergeWithoutTs();
    void TestOptimizeMergeWithUserTs();
    void TestFlushRealtimeIndexWithRemainRtIndex();
    void TestIncCoverAllRtIndex();
    void TestBuildWithProtectionThreshold();

    void TestSearchPkeyAndSkeysSeekBySKey();
    void TestSearchPkeyAndSkeysPerf();
    void TestSearchPkeyAndSkeysInCache();
    void TestBuildWithKeepSortSequence();
    void TestIncCoverAllRtIndexWithAsyncDump();
    void TestForceDumpByValueFull();
    void TestForceDumpBySKeyFull();
    void TestProhibitInMemDumpBug_13765192();
    void TestFlushRealtimeIndexForRemoteLoadConfig();
    void TestCompressedFixedLengthFloatField();
    void TestSingleFloatCompress();
    void TestFixedLengthMultiValueField();

    void TestRtDeleteDoc();

    void TestDocTTL();
    void TestCacheSomeSKeyExpired();

private:
    void InnerTestFixedLengthMultiValueField(const std::string& fieldTypeStr);

    void InnerTestSearchWithBlockCache(const std::string& blockStrategyStr);

    void InnerTestFullBuildWithTruncate(size_t fullDocs, size_t shardingCount, size_t maxDocCount,
                                        const std::string& sortParamsStr);

    void InnerTestRtBuildWithTruncate(size_t rtDocCount, const std::string& sortParamsStr);

    void InnerTestMergeMultiTimes(uint32_t shardingColumnNum, bool pkeyUnique);
    void InnerTestMultiPartMerge(uint32_t shardCount);

    void DoTestPKeyAndSkeyWithDifferentType(const std::string& pkeyType, const std::string& skeyType);

    void DoTestDocTTL(bool valueInline);

    void CheckKeyCount(const file_system::DirectoryPtr& segDir, uint32_t shardId, size_t pkeyCount, size_t skeyCount);

    void InnerTestBuild(size_t fullDocs, size_t incDocs, size_t rtDocs, size_t shardingCount, size_t maxDocCount,
                        bool mergInc = true);

    typedef std::pair<uint32_t, std::vector<segmentid_t>> LevelMetaInfo;
    void BuildDoc(uint32_t docCount);
    void CheckLevelInfo(const std::vector<LevelMetaInfo>& levelInfos);

    std::string MakeDocs(size_t docCount, ExpectResultInfos& resultInfo, uint32_t pkeyTimesNum = 1,
                         bool pKeyUnique = false);

    void CheckResult(test::PartitionStateMachine& psm, const ExpectResultInfos& resultInfo, int32_t curTs,
                     int64_t ttl = KKV_TEST_TTL, bool isOptimize = true);

    void Transfer(test::PsmEvent event, size_t docCount, const std::vector<std::string>& mergeInfo,
                  int64_t ttl = DEFAULT_TIME_TO_LIVE, bool pKeyUnique = false);

    void CheckLocator(int64_t locator, int64_t ts);

    std::string GetCompressType() const;
    std::string GetLoadConfigJsonStr() const;

    OnlinePartitionPtr MakeSortKKVPartition(const std::string& sortFieldInfoStr, std::string& docStr);

    void CheckKKVReader(const index::KKVReaderPtr& kkvReader, const std::string& pkey, const std::string& skeyInfos,
                        const std::string& expectResult);

protected:
    virtual config::IndexPartitionSchemaPtr CreateSchema(const std::string& fields, const std::string& pkey,
                                                         const std::string& skey, const std::string& values);

    virtual std::vector<document::DocumentPtr> CreateKKVDocuments(const config::IndexPartitionSchemaPtr& schema,
                                                                  const std::string& docStrs);

protected:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    ExpectResultInfos mResultInfos;
    partition::DumpSegmentContainerPtr mDumpSegmentContainer;
    bool mImpactValue;
    bool mPlainFormat;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition

#endif //__INDEXLIB_KKVTABLETEST_H
