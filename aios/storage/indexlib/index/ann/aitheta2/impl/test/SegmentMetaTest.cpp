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

private:
    std::shared_ptr<indexlib::file_system::Directory> _rootDir;
};

TEST_F(SegmentMetaTest, TestGeneral)
{
    SegmentMeta segmentMeta;
    segmentMeta.SetDimension(128);
    segmentMeta.SetSegmentType(ST_NORMAL);
    segmentMeta.SetSegmentSize(1024);
    IndexMeta indexInfo0;
    indexInfo0.docCount = 128;
    segmentMeta.AddIndexMeta(0, indexInfo0);
    IndexMeta indexInfo1;
    indexInfo1.docCount = 256;
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
    IndexMeta actualIndexInfo1;
    ASSERT_TRUE(actualSegmentMeta.GetIndexMeta(1, actualIndexInfo1));
    ASSERT_EQ(256, actualIndexInfo1.docCount);
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
