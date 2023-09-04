#include "autil/Log.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/KVFormatOptions.h"
#include "indexlib/index/kv/KVIndexFactory.h"
#include "indexlib/index/kv/TTLFilter.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlibv2::index;
using namespace indexlibv2::framework;

namespace indexlibv2 { namespace table {

static const int DOC_COUNT = 200;

class KVTableMergerFixedLenTest : public TESTBASE
{
public:
    KVTableMergerFixedLenTest() {}
    ~KVTableMergerFixedLenTest() {}

public:
    void setUp() override
    {
        std::string field = "key:int64;value:int64";
        _tabletSchema = table::KVTabletSchemaMaker::Make(field, "key", "value");
        _docs1 = "cmd=add,key=1,value=1,ts=1,locator=0:1;"
                 "cmd=add,key=2,value=2,ts=2,locator=0:2;"
                 "cmd=add,key=3,value=3,ts=3,locator=0:3;"
                 "cmd=add,key=4,value=4,ts=4,locator=0:4;";
        _docs2 = "cmd=add,key=5,value=5,ts=5,locator=0:5;"
                 "cmd=add,key=6,value=6,ts=6,locator=0:6;"
                 "cmd=add,key=7,value=7,ts=7,locator=0:7;"
                 "cmd=add,key=8,value=8,ts=8,locator=0:8;"
                 "cmd=add,key=9,value=9,ts=9,locator=0:9;"
                 "cmd=add,key=3,value=103,ts=3,locator=0:10;"
                 "cmd=add,key=2,value=102,ts=3,locator=0:11;"
                 "cmd=add,key=1,value=101,ts=3,locator=0:12;"
                 "cmd=delete,key=1,ts=10,locator=0:13;"
                 "cmd=delete,key=5,ts=11,locator=0:14;";
        int64_t currentTimestamp = autil::TimeUtility::currentTime() - 10;
        string ts = to_string(currentTimestamp);
        for (int i = 10; i <= DOC_COUNT; ++i) {
            string str = to_string(i);
            string str1 = to_string(i + 5);
            _docs3.append("cmd=add,key=")
                .append(str)
                .append(",value=")
                .append(str)
                .append(",ts=")
                .append(ts)
                .append(",locator=0:")
                .append(str1)
                .append(";");
        }
        _docs4 = "cmd=delete,key=4,ts=10000,locator=0:10000;";
    }
    void tearDown() override {}

    std::unique_ptr<config::TabletOptions> CreateMultiShardOptions();

    void CheckDoc(KVTableTestHelper& mainHelper, const set<int>& deletedDoc);

private:
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
    std::string _docs1;
    std::string _docs2;
    std::string _docs3;
    std::string _docs4;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KVTableMergerFixedLenTest);

std::unique_ptr<config::TabletOptions> KVTableMergerFixedLenTest::CreateMultiShardOptions()
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 1,
            "level_num" : 3
        }
    }
    } )";
    auto tabletOptions = std::make_unique<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(1 * 1024 * 1024);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyParameterStr(
        "space_amplification=3");
    return tabletOptions;
}

void KVTableMergerFixedLenTest::CheckDoc(KVTableTestHelper& mainHelper, const set<int>& deletedDoc)
{
    for (int i = 1; i <= DOC_COUNT; ++i) {
        string keyStr = to_string(i);
        if (deletedDoc.end() == deletedDoc.find(i)) {
            if (i <= 3) {
                string valueStr = to_string(i + 100);
                ASSERT_TRUE(mainHelper.Query("kv", "key", keyStr, "value=" + valueStr));
            } else {
                string valueStr = to_string(i);
                ASSERT_TRUE(mainHelper.Query("kv", "key", keyStr, "value=" + valueStr));
            }
        } else {
            ASSERT_TRUE(mainHelper.Query("kv", "key", keyStr, ""));
        }
    }
}

TEST_F(KVTableMergerFixedLenTest, TestMergeDifferentLevel)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();
    auto [status, indexFactory] = indexFactoryCreator->Create("kv");
    ASSERT_TRUE(status.IsOK());
    auto kvIndexFactory = dynamic_cast<KVIndexFactory*>(indexFactory);
    // we set the key memory size very small to avoid merge to bottom level
    kvIndexFactory->TEST_SetMemoryFactor(1024);

    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, CreateMultiShardOptions()).IsOK());

    // merge to bottom level，FixedLenKVMerger::_dropDeleteKey = true
    {
        ASSERT_TRUE(mainHelper.BuildSegment(_docs1).IsOK());
        ASSERT_TRUE(mainHelper.BuildSegment(_docs2).IsOK());
        ASSERT_TRUE(mainHelper.BuildSegment(_docs3).IsOK());
        ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
        const auto& version = mainHelper.GetCurrentVersion();
        ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
        ASSERT_EQ(DOC_COUNT - 2,
                  framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetTabletInfos()->GetTabletDocCount());
        CheckDoc(mainHelper, {1, 5});
        auto tabletData = framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetTabletData();
        auto levelInfo = version.GetSegmentDescriptions()->GetLevelInfo();
        ASSERT_EQ(0, levelInfo->levelMetas[0].segments.size());
        ASSERT_EQ(1, levelInfo->levelMetas[1].segments.size());
        ASSERT_EQ(1, levelInfo->levelMetas[2].segments.size());
        ASSERT_EQ(INVALID_SEGMENTID, levelInfo->levelMetas[1].segments[0]);
        ASSERT_NE(INVALID_SEGMENTID, levelInfo->levelMetas[2].segments[0]);
        ASSERT_EQ(DOC_COUNT - 2,
                  tabletData->GetSegment(levelInfo->levelMetas[2].segments[0])->GetSegmentInfo()->docCount);
    }

    // not merge to bottom level，FixedLenKVMerger::_dropDeleteKey = false
    // merge is not expected, because of key size of level 0 is very small
    {
        ASSERT_TRUE(mainHelper.BuildSegment(_docs4).IsOK());
        ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());

        const auto& version = mainHelper.GetCurrentVersion();
        ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
        ASSERT_EQ(DOC_COUNT - 1,
                  framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetTabletInfos()->GetTabletDocCount());
        CheckDoc(mainHelper, {1, 4, 5});

        auto tabletData = framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetTabletData();
        auto levelInfo = version.GetSegmentDescriptions()->GetLevelInfo();
        ASSERT_EQ(0, levelInfo->levelMetas[0].segments.size());
        ASSERT_EQ(1, levelInfo->levelMetas[1].segments.size());
        ASSERT_EQ(1, levelInfo->levelMetas[2].segments.size());
        ASSERT_NE(INVALID_SEGMENTID, levelInfo->levelMetas[1].segments[0]);
        ASSERT_EQ(1, tabletData->GetSegment(levelInfo->levelMetas[1].segments[0])->GetSegmentInfo()->docCount);
        ASSERT_NE(INVALID_SEGMENTID, levelInfo->levelMetas[2].segments[0]);
        ASSERT_EQ(DOC_COUNT - 2,
                  tabletData->GetSegment(levelInfo->levelMetas[2].segments[0])->GetSegmentInfo()->docCount);
    }
}

TEST_F(KVTableMergerFixedLenTest, TestTTLFilter)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(_tabletSchema->GetIndexConfig("kv", "key"))
        ->SetTTL(100);

    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, CreateMultiShardOptions()).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs2).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs3).IsOK());
    ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
    ASSERT_EQ(DOC_COUNT - 9,
              framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetTabletInfos()->GetTabletDocCount());
    CheckDoc(mainHelper, {1, 2, 3, 4, 5, 6, 7, 8, 9});
}

TEST_F(KVTableMergerFixedLenTest, TestCompactHashBucket)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    auto indexFactoryCreator = indexlibv2::index::IndexFactoryCreator::GetInstance();
    auto [status, indexFactory] = indexFactoryCreator->Create("kv");
    ASSERT_TRUE(status.IsOK());
    auto kvIndexFactory = dynamic_cast<KVIndexFactory*>(indexFactory);
    // we set the key memory size very small to avoid merge to bottom level
    kvIndexFactory->TEST_SetMemoryFactor(1024);

    std::string field = "key:int64;value:int16";
    _tabletSchema = table::KVTabletSchemaMaker::Make(field, "key", "value", -1, "autoInline");
    auto indexConfig = _tabletSchema->GetIndexConfigs()[0];
    auto kvIndexConfig = dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
    kvIndexConfig->GetValueConfig()->EnableCompactFormat(true);

    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, CreateMultiShardOptions()).IsOK());

    // merge to bottom level，FixedLenKVMerger::_dropDeleteKey = true
    ASSERT_TRUE(mainHelper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs2).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs3).IsOK());
    ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
    ASSERT_EQ(DOC_COUNT - 2,
              framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetTabletInfos()->GetTabletDocCount());
    CheckDoc(mainHelper, {1, 5});
    auto tabletData = framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetTabletData();
    auto levelInfo = version.GetSegmentDescriptions()->GetLevelInfo();
    ASSERT_EQ(0, levelInfo->levelMetas[0].segments.size());
    ASSERT_EQ(1, levelInfo->levelMetas[1].segments.size());
    ASSERT_EQ(1, levelInfo->levelMetas[2].segments.size());
    ASSERT_EQ(INVALID_SEGMENTID, levelInfo->levelMetas[1].segments[0]);
    ASSERT_NE(INVALID_SEGMENTID, levelInfo->levelMetas[2].segments[0]);
    ASSERT_EQ(DOC_COUNT - 2, tabletData->GetSegment(levelInfo->levelMetas[2].segments[0])->GetSegmentInfo()->docCount);

    auto mergedSegment = tabletData->GetSegment(levelInfo->levelMetas[2].segments[0]);
    ASSERT_TRUE(mergedSegment);
    KVFormatOptions formatOpts;
    ASSERT_TRUE(formatOpts.Load(mergedSegment->GetSegmentDirectory()->GetDirectory("index/key", false)).IsOK());
    ASSERT_FALSE(formatOpts.IsShortOffset());
    ASSERT_TRUE(formatOpts.UseCompactBucket());
}

TEST_F(KVTableMergerFixedLenTest, TestEmptyTarget)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, CreateMultiShardOptions()).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs2).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs3).IsOK());
    ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    // empty target
    ASSERT_FALSE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    CheckDoc(mainHelper, {1, 5});
}

TEST_F(KVTableMergerFixedLenTest, TestNotShortOffset)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(_tabletSchema->GetIndexConfig("kv", "key"))
        ->SetTTL(100);
    std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(_tabletSchema->GetIndexConfig("kv", "key"))
        ->GetIndexPreference()
        .GetHashDictParam()
        .SetMaxValueSizeForShortOffset((int64_t)1024);

    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, CreateMultiShardOptions()).IsOK());

    ASSERT_TRUE(mainHelper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs2).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs3).IsOK());
    ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
    ASSERT_EQ(DOC_COUNT - 9,
              framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetTabletInfos()->GetTabletDocCount());
    CheckDoc(mainHelper, {1, 2, 3, 4, 5, 6, 7, 8, 9});

    auto tabletData = framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetTabletData();
    auto levelInfo = version.GetSegmentDescriptions()->GetLevelInfo();
    auto mergedSegment = tabletData->GetSegment(levelInfo->levelMetas[2].segments[0]);
    ASSERT_TRUE(mergedSegment);
    KVFormatOptions formatOpts;
    ASSERT_TRUE(formatOpts.Load(mergedSegment->GetSegmentDirectory()->GetDirectory("index/key", false)).IsOK());
    ASSERT_FALSE(formatOpts.IsShortOffset());
}

}} // namespace indexlibv2::table
