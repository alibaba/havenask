#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/parallel_merge/ParallelReduceMeta.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

namespace indexlibv2::index::ann {

class SegmentMetaTest : public TESTBASE
{
    void SetUp() override
    {
        auto fs = indexlib::file_system::FileSystemCreator::Create("test", "test_root").GetOrThrow();
        _rootDir = indexlib::file_system::Directory::Get(fs);
    };

public:
    void TestGeneral();
    void TestMerge();
    void TestParallelMergeLoad();

private:
    string GetParallelInstanceDirName(int32_t parallelCount, int32_t parallelId)
    {
        // TODO(peaker.lgf): this is from ParallelMergeItem to avoid depending old version code
        return "inst_" + StringUtil::toString(parallelCount) + "_" + StringUtil::toString(parallelId);
    }

    void DumpMetaString(std::shared_ptr<indexlib::file_system::Directory> path, const std::string& content)
    {
        using namespace indexlib::file_system;
        auto meta_file = "aitheta.segment.meta";
        path->RemoveFile(meta_file, RemoveOption::MayNonExist());
        auto writer = path->CreateFileWriter(meta_file);
        writer->Write(content.data(), content.size()).GetOrThrow();
        writer->Close().GetOrThrow();
    }

private:
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
};

static constexpr const char* STATS_TYPE_JSON_STRING = "json_string";

TEST_F(SegmentMetaTest, TestGeneral)
{
    SegmentMeta segmentMeta;
    segmentMeta.SetDimension(128);
    segmentMeta.SetSegmentType(ST_NORMAL);
    segmentMeta.SetSegmentSize(1024);
    IndexMeta indexInfo0;
    indexInfo0.docCount = 128;
    indexInfo0.trainStats.statsType = STATS_TYPE_JSON_STRING;
    indexInfo0.trainStats.stats = "{\"train_stats_prop\", 1}";
    indexInfo0.buildStats.statsType = STATS_TYPE_JSON_STRING;
    indexInfo0.buildStats.stats = "{\"build_stats_prop\", 2}";
    segmentMeta.AddIndexMeta(0, indexInfo0);
    IndexMeta indexInfo1;
    indexInfo1.docCount = 256;
    indexInfo1.trainStats.statsType = STATS_TYPE_JSON_STRING;
    indexInfo1.trainStats.stats = "{\"train_stats_prop\", 3}";
    indexInfo1.buildStats.statsType = STATS_TYPE_JSON_STRING;
    indexInfo1.buildStats.stats = "{\"build_stats_prop\", 4}";
    ASSERT_FALSE(segmentMeta.AddIndexMeta(0, indexInfo1));
    ASSERT_TRUE(segmentMeta.AddIndexMeta(1, indexInfo1));
    auto path = _rootDir->MakeDirectory("TestGeneral");
    ASSERT_TRUE(segmentMeta.Dump(path));

    SegmentMeta actualSegmentMeta;
    ASSERT_TRUE(actualSegmentMeta.Load(path));

    ASSERT_EQ(128, actualSegmentMeta.GetDimension());
    ASSERT_EQ(ST_NORMAL, actualSegmentMeta.GetSegmentType());
    ASSERT_EQ(1024, actualSegmentMeta.GetSegmentSize());
    ASSERT_EQ(128 + 256, actualSegmentMeta.GetDocCount());
    ASSERT_EQ(1 + 1, actualSegmentMeta.GetIndexCount());

    ASSERT_EQ(2, actualSegmentMeta.GetIndexMetaMap().size());
    IndexMeta actualIndexInfo0;
    ASSERT_TRUE(actualSegmentMeta.GetIndexMeta(0, actualIndexInfo0));
    ASSERT_EQ(128, actualIndexInfo0.docCount);
    ASSERT_EQ(STATS_TYPE_JSON_STRING, actualIndexInfo0.trainStats.statsType);
    ASSERT_EQ("{\"train_stats_prop\", 1}", actualIndexInfo0.trainStats.stats);
    ASSERT_EQ(STATS_TYPE_JSON_STRING, actualIndexInfo0.buildStats.statsType);
    ASSERT_EQ("{\"build_stats_prop\", 2}", actualIndexInfo0.buildStats.stats);

    IndexMeta actualIndexInfo1;
    ASSERT_TRUE(actualSegmentMeta.GetIndexMeta(1, actualIndexInfo1));
    ASSERT_EQ(256, actualIndexInfo1.docCount);
    ASSERT_EQ(STATS_TYPE_JSON_STRING, actualIndexInfo1.trainStats.statsType);
    ASSERT_EQ("{\"train_stats_prop\", 3}", actualIndexInfo1.trainStats.stats);
    ASSERT_EQ(STATS_TYPE_JSON_STRING, actualIndexInfo1.buildStats.statsType);
    ASSERT_EQ("{\"build_stats_prop\", 4}", actualIndexInfo1.buildStats.stats);
}

TEST_F(SegmentMetaTest, TestCompat)
{
    auto path = _rootDir->MakeDirectory("TestCompat");

    SegmentMeta segmentMeta;
    IndexMeta indexMeta;

    DumpMetaString(path, "{"
                         "  \"segment_type\":1,"
                         "  \"segment_type_string\":\"normal\","
                         "  \"doc_count\":1024,"
                         "  \"index_count\":2,"
                         "  \"segment_data_size\":102400,"
                         "  \"dimension\":25,"
                         "  \"index_info\":["
                         "    [1, {"
                         "      \"doc_count\":512,"
                         "      \"builder_name\":\"some_builder\","
                         "      \"searcher_name\":\"some_searcher\""
                         "    }],"
                         "    [3, {"
                         "      \"doc_count\":600,"
                         "      \"builder_name\":\"some_builder\","
                         "      \"searcher_name\":\"some_searcher\""
                         "    }]"
                         "  ]"
                         "}");

    ASSERT_TRUE(segmentMeta.Load(path));

    DumpMetaString(path, "{"
                         "  \"segment_type\":1,"
                         "  \"segment_type_string\":\"normal\","
                         "  \"doc_count\":1024,"
                         "  \"index_count\":2,"
                         "  \"segment_data_size\":102400,"
                         "  \"dimension\":25,"
                         "  \"index_info\":["
                         "    [1, {"
                         "      \"doc_count\":512,"
                         "      \"builder_name\":\"some_builder\","
                         "      \"searcher_name\":\"some_searcher\","
                         "      \"train_stats\":{"
                         "        \"stats_type\": \"json_string\","
                         "        \"stats\": \"{\\\"prop\\\":\\\"value\\\"}\""
                         "      },"
                         "      \"build_stats\":{"
                         "        \"stats_type\": \"json_string\","
                         "        \"stats\": \"{\\\"prop\\\":\\\"value\\\"}\""
                         "      }"
                         "    }],"
                         "    [3, {"
                         "      \"doc_count\":600,"
                         "      \"builder_name\":\"some_builder\","
                         "      \"searcher_name\":\"some_searcher\""
                         "    }]"
                         "  ]"
                         "}");

    ASSERT_TRUE(segmentMeta.Load(path));

    segmentMeta.GetIndexMeta(1, indexMeta);

    ASSERT_EQ(STATS_TYPE_JSON_STRING, indexMeta.trainStats.statsType);
    ASSERT_EQ("{\"prop\":\"value\"}", indexMeta.trainStats.stats);
    ASSERT_EQ(STATS_TYPE_JSON_STRING, indexMeta.buildStats.statsType);
    ASSERT_EQ("{\"prop\":\"value\"}", indexMeta.buildStats.stats);
}

TEST_F(SegmentMetaTest, TestMerge)
{
    SegmentMeta segmentMeta;
    segmentMeta.SetDimension(128);
    segmentMeta.SetSegmentType(ST_NORMAL);
    segmentMeta.SetSegmentSize(1024);
    {
        IndexMeta indexInfo;
        indexInfo.docCount = 128;
        segmentMeta.AddIndexMeta(0, indexInfo);
    }
    SegmentMeta segmentMeta1;
    segmentMeta1.SetDimension(128);
    segmentMeta1.SetSegmentType(ST_NORMAL);
    segmentMeta1.SetSegmentSize(1024);
    {
        IndexMeta indexInfo;
        indexInfo.docCount = 256;
        segmentMeta1.AddIndexMeta(1, indexInfo);
    }

    ASSERT_TRUE(segmentMeta.Merge(segmentMeta1));

    ASSERT_EQ(128, segmentMeta.GetDimension());
    ASSERT_EQ(ST_NORMAL, segmentMeta.GetSegmentType());
    ASSERT_EQ(2048, segmentMeta.GetSegmentSize());
    ASSERT_EQ(128 + 256, segmentMeta.GetDocCount());
    ASSERT_EQ(1 + 1, segmentMeta.GetIndexCount());

    ASSERT_EQ(2, segmentMeta.GetIndexMetaMap().size());
    IndexMeta indexMeta;
    ASSERT_TRUE(segmentMeta.GetIndexMeta(0, indexMeta));
    ASSERT_EQ(128, indexMeta.docCount);
    ASSERT_TRUE(segmentMeta.GetIndexMeta(1, indexMeta));
    ASSERT_EQ(256, indexMeta.docCount);
}

TEST_F(SegmentMetaTest, TestParallelMergeLoad)
{
    auto directory = _rootDir->MakeDirectory("TestParallelMergeLoad");
    indexlibv2::index::ann::ParallelReduceMeta parallelReduceMeta(2);
    parallelReduceMeta.Store(directory);

    string path = GetParallelInstanceDirName(2, 0);
    auto instanceDir = directory->MakeDirectory(path);

    string path1 = GetParallelInstanceDirName(2, 1);
    auto instanceDir1 = directory->MakeDirectory(path1);

    SegmentMeta segmentMeta;
    segmentMeta.SetDimension(128);
    segmentMeta.SetSegmentType(ST_NORMAL);
    segmentMeta.SetSegmentSize(1024);
    {
        IndexMeta indexInfo;
        indexInfo.docCount = 128;
        segmentMeta.AddIndexMeta(0, indexInfo);
    }
    segmentMeta.Dump(instanceDir);

    SegmentMeta segmentMeta1;
    segmentMeta1.SetDimension(128);
    segmentMeta1.SetSegmentType(ST_NORMAL);
    segmentMeta1.SetSegmentSize(1024);
    {
        IndexMeta indexInfo;
        indexInfo.docCount = 256;
        segmentMeta1.AddIndexMeta(1, indexInfo);
    }
    segmentMeta1.Dump(instanceDir1);

    SegmentMeta segmentMeta2;
    ASSERT_TRUE(segmentMeta2.Load(directory));

    ASSERT_EQ(128, segmentMeta2.GetDimension());
    ASSERT_EQ(ST_NORMAL, segmentMeta2.GetSegmentType());
    ASSERT_EQ(2048, segmentMeta2.GetSegmentSize());
    ASSERT_EQ(128 + 256, segmentMeta2.GetDocCount());
    ASSERT_EQ(1 + 1, segmentMeta2.GetIndexCount());

    ASSERT_EQ(2, segmentMeta2.GetIndexMetaMap().size());
    IndexMeta indexMeta;
    ASSERT_TRUE(segmentMeta2.GetIndexMeta(0, indexMeta));
    ASSERT_EQ(128, indexMeta.docCount);
    ASSERT_TRUE(segmentMeta2.GetIndexMeta(1, indexMeta));
    ASSERT_EQ(256, indexMeta.docCount);
}

} // namespace indexlibv2::index::ann
