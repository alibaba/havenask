#include "fslib/fs/FileSystem.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/SimpleMultiSegmentKVIterator.h"
#include "indexlib/index/kv/VarLenKVMerger.h"
#include "indexlib/index/kv/test/MultiSegmentKVIteratorTestBase.h"

namespace indexlibv2::index {

using SegmentMergeInfos = IIndexMerger::SegmentMergeInfos;
using SourceSegment = IIndexMerger::SourceSegment;

class MockVarLenKVMerger : public VarLenKVMerger
{
public:
    MOCK_METHOD(Status, LoadSegmentStatistics, (const SegmentMergeInfos&, std::vector<SegmentStatistics>&),
                (const, override));
};
using NiceMockVarLenKVMerger = ::testing::NiceMock<MockVarLenKVMerger>;

class KVSortMergeTest : public MultiSegmentKVIteratorTestBase
{
public:
    void setUp() override
    {
        MultiSegmentKVIteratorTestBase::setUp();
        _params[DROP_DELETE_KEY] = std::string("true");
        _merger = std::make_unique<NiceMockVarLenKVMerger>();
    }

protected:
    void PrepareSegmentMergeInfos(const std::vector<std::shared_ptr<framework::Segment>>& inputs,
                                  SegmentMergeInfos& segMergeInfos) const
    {
        segmentid_t targetSegmentId = -1;
        for (const auto& segment : inputs) {
            segMergeInfos.srcSegments.emplace_back(SourceSegment {0, segment});
            targetSegmentId = segment->GetSegmentId() + 1;
        }
        auto targetSegmentMeta = std::make_shared<framework::SegmentMeta>(targetSegmentId);
        targetSegmentMeta->schema = _schema;
        targetSegmentMeta->segmentMetrics = std::make_shared<indexlib::framework::SegmentMetrics>();
        if (targetSegmentId >= 0) {
            targetSegmentMeta->segmentDir = MakeSegmentDirectory(targetSegmentId);
            segMergeInfos.targetSegments.emplace_back(std::move(targetSegmentMeta));
        }
    }

    std::shared_ptr<framework::Segment> LoadMergedSegment(const SegmentMergeInfos& segMergeInfos)
    {
        auto fs = _onlineDirectory->GetFileSystem();
        auto s = fs->MountSegment(segMergeInfos.targetSegments[0]->segmentDir->GetLogicalPath()).Status();
        if (!s.IsOK()) {
            return nullptr;
        }
        return LoadSegment(_onlineDirectory, segMergeInfos.targetSegments[0]->segmentId, true);
    }

protected:
    std::map<std::string, std::any> _params;
    std::unique_ptr<NiceMockVarLenKVMerger> _merger;
};

ACTION_P2(DoFillStatistics, keyCountVec)
{
    ASSERT_EQ(arg0.srcSegments.size(), keyCountVec.size());
    for (const auto& keyCount : keyCountVec) {
        SegmentStatistics stat;
        stat.keyCount = keyCount;
        arg1.push_back(stat);
    }
}

TEST_F(KVSortMergeTest, testNoSegments)
{
    SegmentMergeInfos segMergeInfos;
    PrepareSegmentMergeInfos({}, segMergeInfos);

    ASSERT_TRUE(_merger->Init(_config, _params).IsOK());
    EXPECT_CALL(*_merger, LoadSegmentStatistics(_, _)).Times(0);
    ASSERT_TRUE(_merger->Merge(segMergeInfos, nullptr).IsOK());
}

TEST_F(KVSortMergeTest, testEmptySegments)
{
    std::vector<std::shared_ptr<framework::Segment>> segments;
    PrepareSegments({"", "", ""}, segments);
    ASSERT_EQ(3, segments.size());
    SegmentMergeInfos segMergeInfos;
    PrepareSegmentMergeInfos(segments, segMergeInfos);
    ASSERT_EQ(3, segMergeInfos.srcSegments.size());
    ASSERT_EQ(1, segMergeInfos.targetSegments.size());
    ASSERT_EQ(3, segMergeInfos.targetSegments[0]->segmentId);
    ASSERT_TRUE(segMergeInfos.targetSegments[0]->schema);
    ASSERT_TRUE(segMergeInfos.targetSegments[0]->segmentDir);
    ASSERT_TRUE(segMergeInfos.targetSegments[0]->schema);

    auto keyCountVec = std::vector<int64_t> {0, 0, 0};
    EXPECT_CALL(*_merger, LoadSegmentStatistics(_, _))
        .WillOnce(DoAll(DoFillStatistics(keyCountVec), Return(Status::OK())));
    ASSERT_TRUE(_merger->Init(_config, _params).IsOK());
    ASSERT_TRUE(_merger->Merge(segMergeInfos, nullptr).IsOK());
    ASSERT_TRUE(_merger->_sortDescriptions);

    auto segment = LoadMergedSegment(segMergeInfos);
    ASSERT_TRUE(segment);

    SimpleMultiSegmentKVIterator iterator(_schema->GetSchemaId(), _config, nullptr, {segment});
    ASSERT_TRUE(iterator.Init().IsOK());
    ASSERT_TRUE(iterator.HasNext());
    Record r;
    auto s = iterator.Next(_pool.get(), r);
    ASSERT_TRUE(s.IsEof()) << s.ToString();
    ASSERT_FALSE(iterator.HasNext());
}

TEST_F(KVSortMergeTest, testOneSegment)
{
    auto docs = "cmd=add,key=1,f1=100,f2=1,f3=abc;"
                "cmd=add,key=2,f1=200,f2=2,f3=bcd;"
                "cmd=add,key=3,f1=100,f2=1,f3=def;"
                "cmd=add,key=4,f1=200,f2=0,f3=efg;"
                "cmd=delete,key=5;";
    std::vector<std::shared_ptr<framework::Segment>> segments;
    PrepareSegments({std::move(docs)}, segments);
    ASSERT_EQ(1, segments.size());

    SegmentMergeInfos segMergeInfos;
    PrepareSegmentMergeInfos(segments, segMergeInfos);

    auto keyCountVec = std::vector<int64_t> {5};
    EXPECT_CALL(*_merger, LoadSegmentStatistics(_, _))
        .WillOnce(DoAll(DoFillStatistics(keyCountVec), Return(Status::OK())));
    ASSERT_TRUE(_merger->Init(_config, _params).IsOK());
    ASSERT_TRUE(_merger->Merge(segMergeInfos, nullptr).IsOK());
    ASSERT_TRUE(_merger->_sortDescriptions);

    auto segment = LoadMergedSegment(segMergeInfos);
    ASSERT_TRUE(segment);

    SimpleMultiSegmentKVIterator iterator(_schema->GetSchemaId(), _config, nullptr, {segment});
    ASSERT_TRUE(iterator.Init().IsOK());

    CheckIterator(&iterator, {{4, false}, {2, false}, {1, false}, {3, false}},
                  {{"200", "0", "efg"}, {"200", "2", "bcd"}, {"100", "1", "abc"}, {"100", "1", "def"}});
}

TEST_F(KVSortMergeTest, testMultiSegments)
{
    auto seg0 = "cmd=add,key=1,f1=100,f2=1,f3=abc;" // x
                "cmd=add,key=2,f1=20,f2=2,f3=bcd;"
                "cmd=add,key=3,f1=100,f2=1,f3=def;" // x
                "cmd=add,key=4,f1=200,f2=0,f3=efg;" // x
                "cmd=delete,key=5;"
                "cmd=add,key=6,f1=600,f2=6,f3=666;";
    auto seg2 = "cmd=add,key=1,f1=20,f2=2,f3=abc;"
                "cmd=delete,key=3;"
                "cmd=add,key=4,f1=100,f2=0,f3=efg;"
                "cmd=add,key=7,f1=7,f2=7,f3=777";
    std::vector<std::shared_ptr<framework::Segment>> segments;
    PrepareSegments({std::move(seg0), "", std::move(seg2)}, segments);
    ASSERT_EQ(3, segments.size());

    SegmentMergeInfos segMergeInfos;
    PrepareSegmentMergeInfos(segments, segMergeInfos);

    auto keyCountVec = std::vector<int64_t> {5, 0, 4};
    EXPECT_CALL(*_merger, LoadSegmentStatistics(_, _))
        .WillOnce(DoAll(DoFillStatistics(keyCountVec), Return(Status::OK())));

    ASSERT_TRUE(_merger->Init(_config, _params).IsOK());
    ASSERT_TRUE(_merger->Merge(segMergeInfos, nullptr).IsOK());
    ASSERT_TRUE(_merger->_sortDescriptions);

    auto segment = LoadMergedSegment(segMergeInfos);
    ASSERT_TRUE(segment);

    SimpleMultiSegmentKVIterator iterator(_schema->GetSchemaId(), _config, nullptr, {segment});
    ASSERT_TRUE(iterator.Init().IsOK());

    auto keys = std::vector<std::pair<keytype_t, bool>> {{6, false}, {4, false}, {1, false}, {2, false}, {7, false}};
    auto values = std::vector<std::vector<std::string>> {
        {"600", "6", "666"}, {"100", "0", "efg"}, {"20", "2", "abc"}, {"20", "2", "bcd"}, {"7", "7", "777"}};
    CheckIterator(&iterator, keys, values);
}

} // namespace indexlibv2::index
