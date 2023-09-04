#include "indexlib/table/index_task/merger/test/MergeTestHelper.h"

#include "autil/StringTokenizer.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/MergeStrategyParameter.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/mock/MockTabletSchema.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {

MergeTestHelper::MergeTestHelper() {}

MergeTestHelper::~MergeTestHelper() {}

// level_info: segId,segId|cursor
// levelInfo:topo,shardCount,levelNum:segId,segId|cursor;segId,segId(level1)...
std::shared_ptr<framework::IndexTaskContext>
MergeTestHelper::CreateContextWithLevelInfo(const std::vector<FakeSegmentInfo>& segInfos,
                                            const std::string& levelInfoStr)
{
    auto context = CreateContext(segInfos);
    std::vector<std::string> infos;
    autil::StringUtil::fromString(levelInfoStr, infos, ":");
    std::vector<std::string> metas;
    autil::StringUtil::fromString(infos[0], metas, ",");
    auto topo = framework::LevelMeta::StrToTopology(metas[0]);
    size_t shardCount = autil::StringUtil::fromString<size_t>(metas[1]);
    size_t levelNum = autil::StringUtil::fromString<size_t>(metas[2]);
    auto levelInfo = std::make_shared<framework::LevelInfo>();
    levelInfo->Init(topo, levelNum, shardCount);

    std::vector<std::string> levelSegmentInfos = autil::StringUtil::split(infos[1], ";", false);
    for (size_t i = 0; i < levelSegmentInfos.size(); i++) {
        if (levelSegmentInfos[i].empty()) {
            continue;
        }
        std::vector<segmentid_t> levelSegments;
        std::vector<std::string> levelSegmentInfo = autil::StringUtil::split(levelSegmentInfos[i], "|", true);
        autil::StringUtil::fromString(levelSegmentInfo[0], levelSegments, ",");
        if (i == 0) {
            for (size_t j = 0; j < levelSegments.size(); j++) {
                if (levelSegments[j] != INVALID_SEGMENTID) {
                    levelInfo->levelMetas[i].segments.push_back(levelSegments[j]);
                }
            }
        } else {
            levelInfo->levelMetas[i].segments = levelSegments;
        }
        if (levelSegmentInfo.size() == 2) {
            uint32_t cursor = autil::StringUtil::fromString<uint32_t>(levelSegmentInfo[1]);
            levelInfo->levelMetas[i].cursor = cursor;
        }
    }
    auto tabletData = context->GetTabletData();
    const auto& diskVersion = tabletData->GetOnDiskVersion();
    const auto& segDesc = diskVersion.GetSegmentDescriptions();
    segDesc->SetLevelInfo(levelInfo);
    return context;
}

std::shared_ptr<framework::IndexTaskContext>
MergeTestHelper::CreateContext(const std::vector<FakeSegmentInfo>& segInfos)
{
    std::shared_ptr<framework::TabletData> tabletData(new framework::TabletData("test"));
    framework::Version onDiskVersion(0);
    FromJsonString(onDiskVersion, R"( {
        "versionid": 0,
        "segment_descriptions": {
            "level_info": {
                "level_metas": []
            }
         }
    } )");
    std::vector<std::shared_ptr<framework::Segment>> segments;
    segmentid_t maxMergedSegmentId = INVALID_SEGMENTID;
    for (const FakeSegmentInfo& info : segInfos) {
        assert(info.id != INVALID_SEGMENTID);
        maxMergedSegmentId = info.isMerged ? std::max(info.id, maxMergedSegmentId) : maxMergedSegmentId;
        framework::SegmentMeta segMeta(info.id);
        segMeta.segmentInfo->mergedSegment = info.isMerged;
        segMeta.segmentInfo->docCount = info.docCount;
        segMeta.segmentInfo->timestamp = info.timestamp;
        std::shared_ptr<indexlib::file_system::IDirectory> fakeSegmentDirectory(
            new FakeDirectory(info.segmentSize * 1024 * 1024));
        segMeta.segmentDir.reset(new indexlib::file_system::Directory(fakeSegmentDirectory));

        std::shared_ptr<FakeSegment> segment(new FakeSegment(framework::Segment::SegmentStatus::ST_BUILT));
        segment->TEST_SetSegmentMeta(segMeta);
        segments.push_back(segment);
        onDiskVersion.AddSegment(info.id);
        if (info.deleteDocCount != 0) {
            std::shared_ptr<index::DeletionMapDiskIndexer> indexer(
                new index::DeletionMapDiskIndexer(info.docCount, info.id));
            indexer->TEST_InitWithoutOpen();
            for (docid_t i = 0; i < info.deleteDocCount; i++) {
                auto status = indexer->Delete(i);
                assert(status.IsOK());
            }
            segment->AddIndexer(index::DELETION_MAP_INDEX_TYPE_STR, index::DELETION_MAP_INDEX_NAME, indexer);
        }
    }
    auto status = tabletData->Init(onDiskVersion.Clone(), segments, std::make_shared<framework::ResourceMap>());
    assert(status.IsOK());
    std::shared_ptr<FakeContext> context(new FakeContext);
    context->Init(tabletData);
    context->TEST_SetMaxMergedSegmentId(maxMergedSegmentId);
    context->TEST_SetMaxMergedVersionId(1);
    context->TEST_SetTabletSchema(std::make_shared<framework::MockTabletSchema>());
    return context;
}

std::shared_ptr<framework::IndexTaskContext>
MergeTestHelper::CreateContextAndParseParams(const std::string& rawStrategyParamString,
                                             const std::string& mergeStrategyName,
                                             const std::vector<FakeSegmentInfo>& segInfos)
{
    std::vector<std::string> params = autil::StringUtil::split(rawStrategyParamString, "|", false);
    std::string paramString = rawStrategyParamString;
    if (params.size() == 3) {
        std::map<std::string, std::string> m;
        m["input_limits"] = params[0];
        m["strategy_conditions"] = params[1];
        m["output_limits"] = params[2];
        paramString = autil::legacy::ToJsonString(m);
    }

    auto context = MergeTestHelper::CreateContext(segInfos);
    auto& mergeConfig = context->GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    mergeConfig.TEST_SetMergeStrategyStr(mergeStrategyName);
    mergeConfig.TEST_SetMergeStrategyParameterStr(paramString);
    if (!context->SetDesignateTask("merge", "default_merge")) {
        assert(false);
        return nullptr;
    }
    return context;
}

void MergeTestHelper::VerifyMergePlan(MergeStrategy* mergeStrategy, const framework::IndexTaskContext* context,
                                      const std::vector<std::vector<segmentid_t>>& expectSrcSegments)
{
    auto mergePlanPair = mergeStrategy->CreateMergePlan(context);
    ASSERT_TRUE(mergePlanPair.first.IsOK());
    auto mergePlan = mergePlanPair.second;
    ASSERT_TRUE(mergePlan);

    std::vector<std::vector<segmentid_t>> actualMergePlanSrcSegments;
    for (size_t i = 0; i < mergePlan->Size(); ++i) {
        const SegmentMergePlan& segmentMergePlan = mergePlan->GetSegmentMergePlan(i);
        std::vector<segmentid_t> segmentIds = segmentMergePlan.GetSrcSegments();
        std::sort(segmentIds.begin(), segmentIds.end());
        actualMergePlanSrcSegments.push_back(segmentIds);

        EXPECT_EQ(context->GetMaxMergedSegmentId() + i + 1, segmentMergePlan.GetTargetSegmentId(0));

        // check target segment info
        framework::SegmentInfo expectTargetSegInfo;
        for (segmentid_t segId : segmentIds) {
            SegmentMergePlan::UpdateTargetSegmentInfo(segId, context->GetTabletData(), &expectTargetSegInfo);
        }
        EXPECT_EQ(1, segmentMergePlan.GetTargetSegmentCount());
        framework::SegmentInfo actualTargetSegInfo = segmentMergePlan.GetTargetSegmentInfo(0);
        EXPECT_EQ(expectTargetSegInfo.docCount, actualTargetSegInfo.docCount);
        EXPECT_EQ(expectTargetSegInfo.timestamp, actualTargetSegInfo.timestamp);
        EXPECT_TRUE(actualTargetSegInfo.mergedSegment);
        EXPECT_EQ(expectTargetSegInfo.maxTTL, actualTargetSegInfo.maxTTL);
        EXPECT_EQ(expectTargetSegInfo.shardCount, actualTargetSegInfo.shardCount);
        EXPECT_EQ(expectTargetSegInfo.shardId, actualTargetSegInfo.shardId);
        EXPECT_EQ(expectTargetSegInfo.schemaId, actualTargetSegInfo.schemaId);

        // check target version
        framework::Version targetVersion = mergePlan->GetTargetVersion();
        EXPECT_EQ(context->GetMaxMergedVersionId() + 1, targetVersion.GetVersionId());
        EXPECT_EQ(context->GetTabletSchema()->GetSchemaId(), targetVersion.GetSchemaId());
        EXPECT_TRUE(targetVersion.HasSegment(segmentMergePlan.GetTargetSegmentId(0)));
        for (segmentid_t segId : segmentIds) {
            EXPECT_FALSE(targetVersion.HasSegment(segId))
                << "segment [" << segId << "] should not exist in target version: [" << targetVersion.ToString()
                << std::endl;
        }
        std::vector<segmentid_t> targetSegmentIds;
        std::vector<segmentid_t> orderedTargetSegmentIds;
        for (auto [segId, _] : targetVersion) {
            targetSegmentIds.push_back(segId);
            orderedTargetSegmentIds.push_back(segId);
        }
        std::sort(orderedTargetSegmentIds.begin(), orderedTargetSegmentIds.end());

        EXPECT_EQ(orderedTargetSegmentIds, targetSegmentIds);
    }
    EXPECT_THAT(actualMergePlanSrcSegments, testing::UnorderedElementsAreArray(expectSrcSegments));
}

void MergeTestHelper::TestMergeStrategy(MergeStrategy* mergeStrategy,
                                        const std::vector<std::vector<segmentid_t>>& rawExpectSrcSegments,
                                        const std::string& rawStrategyParamString,
                                        const std::vector<FakeSegmentInfo>& rawFakeSegInfos, bool isOptimizeMerge)
{
    // 改写SegmentId，把用户未指定的SegId自动++补全、把BuildSegmentId改成带Mask的
    std::map<segmentid_t, segmentid_t> segIdMap;
    std::vector<FakeSegmentInfo> fakeSegInfos;
    segmentid_t buildSegmentId = framework::Segment::PUBLIC_SEGMENT_ID_MASK | 0, mergeSegmentId = 0;
    segmentid_t rawSegmentId = -1;
    for (const FakeSegmentInfo& rawFakeSegInfo : rawFakeSegInfos) {
        FakeSegmentInfo info = rawFakeSegInfo;
        rawSegmentId = (rawFakeSegInfo.id == INVALID_SEGMENTID) ? rawSegmentId + 1 : rawFakeSegInfo.id;
        info.id = info.isMerged ? mergeSegmentId++ : buildSegmentId++;
        segIdMap[rawSegmentId] = info.id;
        fakeSegInfos.emplace_back(info);
    }
    std::vector<std::vector<segmentid_t>> expectSrcSegments = rawExpectSrcSegments;
    for (size_t i = 0; i < expectSrcSegments.size(); ++i) {
        for (size_t j = 0; j < expectSrcSegments[i].size(); ++j) {
            expectSrcSegments[i][j] = segIdMap[expectSrcSegments[i][j]]; // 期望的srcSegId必须合法
        }
    }

    // 执行测试
    auto context = CreateContextAndParseParams(rawStrategyParamString, mergeStrategy->GetName(), fakeSegInfos);
    if (isOptimizeMerge) {
        context->AddParameter(std::string(IS_OPTIMIZE_MERGE), true);
    }
    VerifyMergePlan(mergeStrategy, context.get(), expectSrcSegments);
}

void MergeTestHelper::StoreVersion(const framework::Version& version,
                                   const std::shared_ptr<indexlib::file_system::IDirectory>& fenceDir,
                                   bool ignoreEmptyVersion)
{
    if (!ignoreEmptyVersion || version.GetSegmentCount() > 0) {
        std::string content = version.ToString();
        std::stringstream ss;
        ss << VERSION_FILE_NAME_PREFIX << "." << version.GetVersionId();
        auto result = fenceDir->Store(ss.str(), content, indexlib::file_system::WriterOption::AtomicDump());
        ASSERT_TRUE(result.Status().IsOK());

        std::stringstream ss1;
        ss1 << DEPLOY_META_FILE_NAME_PREFIX << "." << version.GetVersionId();
        result = fenceDir->Store(ss1.str(), "", indexlib::file_system::WriterOption::AtomicDump());
        ASSERT_TRUE(result.Status().IsOK());
    }
}
}} // namespace indexlibv2::table
