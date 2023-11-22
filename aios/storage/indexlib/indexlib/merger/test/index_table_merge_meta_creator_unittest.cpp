#include "indexlib/merger/test/index_table_merge_meta_creator_unittest.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/plugin/plugin_manager.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::plugin;
using namespace indexlib::test;

namespace indexlib { namespace merger {

IndexTableMergeMetaCreatorTest::IndexTableMergeMetaCreatorTest() {}

IndexTableMergeMetaCreatorTest::~IndexTableMergeMetaCreatorTest() {}

void IndexTableMergeMetaCreatorTest::CaseSetUp() { mRootDir = GET_TEMP_DATA_PATH(); }

void IndexTableMergeMetaCreatorTest::CaseTearDown() {}

void IndexTableMergeMetaCreatorTest::TestGetNotMergedSegIds()
{
    Version lastVersion;
    SegmentMergeInfos segMergeInfos;

    IndexPartitionOptions options;
    IndexPartitionSchemaPtr schema;
    ReclaimMapCreatorPtr reclaimMapCreator;

    options.SetIsOnline(false);
    // empty
    {
        merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), Version()));
        segDir->Init(false, true);
        SegmentMergeInfos notMergedSegIds;
        Version currentVersion, lastVersion;
        IndexTableMergeMetaCreator creator(schema, reclaimMapCreator, nullptr);
        creator.mSegmentDirectory = segDir;
        creator.GetNotMergedSegments(notMergedSegIds);
        INDEXLIB_TEST_TRUE(notMergedSegIds.size() == 0);
    }
    // not empty
    {
        string versionStr = "1,1024,2,2000#true,3,1024,4,800,5,50#true";
        MakeFakeSegmentDir(versionStr, lastVersion, segMergeInfos);

        merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), lastVersion));
        segDir->Init(false, true);
        SegmentMergeInfos notMergedSegIds;
        Version currentVersion, lastVersion;
        IndexTableMergeMetaCreator creator(schema, reclaimMapCreator, nullptr);
        creator.mSegMergeInfos = segMergeInfos;
        creator.mSegmentDirectory = segDir;
        creator.GetNotMergedSegments(notMergedSegIds);
        INDEXLIB_TEST_EQUAL((size_t)3, notMergedSegIds.size());
        INDEXLIB_TEST_EQUAL((int32_t)1, notMergedSegIds[0].segmentId);
        INDEXLIB_TEST_EQUAL((int32_t)3, notMergedSegIds[1].segmentId);
        INDEXLIB_TEST_EQUAL((int32_t)4, notMergedSegIds[2].segmentId);
    }
}

void IndexTableMergeMetaCreatorTest::TestCreateMergeTaskForIncTruncate()
{
    Version lastVersion;
    SegmentMergeInfos segMergeInfos;
    string versionStr = "0,1#true,1,1024,2,2000#true,3,1024,4,800,5,50";
    MakeFakeSegmentDir(versionStr, lastVersion, segMergeInfos);

    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), lastVersion));
    segDir->Init(false, true);
    IndexPartitionSchemaPtr schema;
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    ReclaimMapCreatorPtr reclaimMapCreator;
    IndexTableMergeMetaCreator creator(schema, reclaimMapCreator, nullptr);
    creator.mSegmentDirectory = segDir;
    creator.mSegMergeInfos = segMergeInfos;
    MergeTask task;
    MergePlan plan;
    plan.AddSegment(segMergeInfos[1]);
    plan.AddSegment(segMergeInfos[5]);
    task.AddPlan(plan);
    creator.CreateMergeTaskForIncTruncate(task);
    INDEXLIB_TEST_EQUAL((size_t)3, task.GetPlanCount());
    INDEXLIB_TEST_EQUAL((size_t)1, task[1].GetSegmentCount());
    INDEXLIB_TEST_EQUAL((size_t)1, task[2].GetSegmentCount());
    INDEXLIB_TEST_TRUE(task[1].HasSegment(3));
    INDEXLIB_TEST_TRUE(task[2].HasSegment(4));
}

void IndexTableMergeMetaCreatorTest::TestExceedMaxDocId()
{
    auto doTest = [this](exdocid_t totalDocCount) {
        Version lastVersion;
        SegmentMergeInfos segMergeInfos;
        string versionStr = "0,1#true,1,9,2," + StringUtil::toString(totalDocCount - 10);
        MakeFakeSegmentDir(versionStr, lastVersion, segMergeInfos);
        SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), lastVersion));
        segDir->Init(false, false);
        auto schema = SchemaMaker::MakeSchema("f1:long", "pk:PRIMARYKEY64:f1", "", "");
        IndexTableMergeMetaCreator creator(schema, ReclaimMapCreatorPtr(), nullptr);
        MergeConfig mergeConfig;
        SegmentMergeInfos mergeInfos = MergeMetaCreator::CreateSegmentMergeInfos(schema, mergeConfig, segDir);
        creator.Init(segDir, mergeInfos, mergeConfig, DumpStrategyPtr(), PluginManagerPtr(), 1);
    };
    ASSERT_THROW(doTest(MAX_DOCID), util::UnSupportedException);
}

void IndexTableMergeMetaCreatorTest::MakeFakeSegmentDir(const string& versionsStr, Version& lastVersion,
                                                        SegmentMergeInfos& segMergeInfos)
{
    versionid_t versionId = 0;
    StringTokenizer st(versionsStr, ";", StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer st2(st[i], ",", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);

        segMergeInfos.clear();

        Version version(versionId++);
        for (size_t j = 0; j < st2.getNumTokens(); j += 2) {
            segmentid_t segmentId;
            StringUtil::strToInt32(st2[j].c_str(), segmentId);
            version.AddSegment(segmentId);
            string segmentPath = version.GetSegmentDirName(segmentId) + "/";
            file_system::DirectoryPtr segmentDir = GET_PARTITION_DIRECTORY()->MakeDirectory(segmentPath);
            segmentDir->MakeDirectory(DELETION_MAP_DIR_NAME);

            SegmentInfo segInfo;
            segInfo.SetLocator(indexlibv2::framework::Locator(0, segmentId));
            StringTokenizer st3(st2[j + 1], "#", StringTokenizer::TOKEN_TRIM);
            uint32_t docCount = 0;
            if (st3.getNumTokens() != 2) {
                StringUtil::strToUInt32(st2[j + 1].c_str(), docCount);
                segInfo.docCount = docCount;
            } else {
                StringUtil::strToUInt32(st3[0].c_str(), docCount);
                segInfo.docCount = docCount;
                if (st3[1] == "true") {
                    segInfo.mergedSegment = true;
                }
            }
            segInfo.Store(segmentDir);

            SegmentMergeInfo segMergeInfo;
            segMergeInfo.segmentId = segmentId;
            segMergeInfo.segmentInfo = segInfo;
            segMergeInfos.push_back(segMergeInfo);
        }
        string content = version.ToString();
        string versionFileName = VERSION_FILE_NAME_DOT_PREFIX + StringUtil::toString(version.GetVersionId());
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        GET_PARTITION_DIRECTORY()->RemoveFile(versionFileName, removeOption);
        GET_PARTITION_DIRECTORY()->Store(versionFileName, content);

        if (i == st.getNumTokens() - 1) {
            lastVersion = version;
        }
    }
}
}} // namespace indexlib::merger
