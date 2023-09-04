#include "indexlib/index/ann/aitheta2/util/parallel_merge/ParallelMergeUtil.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentMeta.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
namespace indexlibv2::index::ann {

class ParallelMergeUtilTest : public TESTBASE
{
public:
    ParallelMergeUtilTest() = default;
    ~ParallelMergeUtilTest() = default;

public:
    void TestLoadSegmentMetas();
};

TEST_F(ParallelMergeUtilTest, TestLoadSegmentMetas)
{
    SegmentMeta segmentMeta0;
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto partitionDir = indexlib::file_system::Directory::Get(fs);
    auto root = partitionDir->MakeDirectory("ParallelMergeUtilTest");
    auto str0 = "inst_" + StringUtil::toString(3) + "_" + StringUtil::toString(0);
    auto directory0 = root->MakeDirectory(str0);
    {
        index_id_t indexId = 1;
        IndexMeta indexMeta;
        indexMeta.docCount = 10;
        segmentMeta0.AddIndexMeta(indexId, indexMeta);
    }
    {
        index_id_t indexId = 2;
        IndexMeta indexMeta;
        indexMeta.docCount = 20;
        segmentMeta0.AddIndexMeta(indexId, indexMeta);
    }
    {
        index_id_t indexId = 3;
        IndexMeta indexMeta;
        indexMeta.docCount = 30;
        segmentMeta0.AddIndexMeta(indexId, indexMeta);
    }
    segmentMeta0.Dump(directory0);

    SegmentMeta segmentMeta1;
    auto str1 = "inst_" + StringUtil::toString(3) + "_" + StringUtil::toString(1);
    auto directory1 = root->MakeDirectory(str1);
    {
        index_id_t indexId = 2;
        IndexMeta indexMeta;
        indexMeta.docCount = 200;
        segmentMeta1.AddIndexMeta(indexId, indexMeta);
    }
    {
        index_id_t indexId = 3;
        IndexMeta indexMeta;
        indexMeta.docCount = 300;
        segmentMeta1.AddIndexMeta(indexId, indexMeta);
    }
    {
        index_id_t indexId = 4;
        IndexMeta indexMeta;
        indexMeta.docCount = 400;
        segmentMeta1.AddIndexMeta(indexId, indexMeta);
    }
    segmentMeta1.Dump(directory1);

    SegmentMeta segmentMeta2;
    auto str2 = "inst_" + StringUtil::toString(3) + "_" + StringUtil::toString(2);
    auto directory2 = root->MakeDirectory(str2);
    {
        index_id_t indexId = 3;
        IndexMeta indexMeta;
        indexMeta.docCount = 3000;
        segmentMeta2.AddIndexMeta(indexId, indexMeta);
    }
    {
        index_id_t indexId = 4;
        IndexMeta indexMeta;
        indexMeta.docCount = 4000;
        segmentMeta2.AddIndexMeta(indexId, indexMeta);
    }
    {
        index_id_t indexId = 5;
        IndexMeta indexMeta;
        indexMeta.docCount = 5000;
        segmentMeta2.AddIndexMeta(indexId, indexMeta);
    }
    segmentMeta2.Dump(directory2);

    ParallelReduceMeta parallelReduceMeta(3);
    vector<indexlib::file_system::DirectoryPtr> directories;
    (ParallelMergeUtil::GetParallelMergeDirs(root, parallelReduceMeta, directories));
    ASSERT_EQ(3, directories.size());
}

} // namespace indexlibv2::index::ann
