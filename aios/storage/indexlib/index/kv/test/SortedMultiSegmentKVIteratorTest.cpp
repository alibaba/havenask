#include "indexlib/index/kv/SortedMultiSegmentKVIterator.h"

#include "indexlib/index/kv/test/MultiSegmentKVIteratorTestBase.h"

namespace indexlibv2::index {

class SortedMultiSegmentKVIteratorTest : public MultiSegmentKVIteratorTestBase
{
};

TEST_F(SortedMultiSegmentKVIteratorTest, testNoSegment)
{
    SortedMultiSegmentKVIterator iterator(_schema->GetSchemaId(), _config, nullptr, {}, _indexerParam.sortDescriptions);
    ASSERT_TRUE(iterator.Init().IsOK());
    ASSERT_FALSE(iterator.HasNext());
}

TEST_F(SortedMultiSegmentKVIteratorTest, testEmptySegments)
{
    std::vector<std::shared_ptr<framework::Segment>> segments;
    PrepareSegments({"", "", ""}, segments);
    std::reverse(segments.begin(), segments.end());
    ASSERT_EQ(3, segments.size());

    SortedMultiSegmentKVIterator iterator(_schema->GetSchemaId(), _config, nullptr, std::move(segments),
                                          _indexerParam.sortDescriptions);
    ASSERT_TRUE(iterator.Init().IsOK());
    ASSERT_FALSE(iterator.HasNext());
}

TEST_F(SortedMultiSegmentKVIteratorTest, testOneSegment)
{
    auto docs = "cmd=add,key=1,f1=100,f2=1,f3=abc;"
                "cmd=add,key=2,f1=200,f2=2,f3=bcd;"
                "cmd=add,key=3,f1=100,f2=1,f3=def;"
                "cmd=add,key=4,f1=200,f2=0,f3=efg;"
                "cmd=delete,key=5;";
    std::vector<std::shared_ptr<framework::Segment>> segments;
    PrepareSegments({std::move(docs)}, segments);
    ASSERT_EQ(1, segments.size());

    SortedMultiSegmentKVIterator iterator(_schema->GetSchemaId(), _config, nullptr, std::move(segments),
                                          _indexerParam.sortDescriptions);
    ASSERT_TRUE(iterator.Init().IsOK());
    ASSERT_TRUE(iterator._unusedKeys.empty());
    ASSERT_TRUE(iterator._valueHeap);
    ASSERT_TRUE(iterator.HasNext());
    CheckIterator(&iterator, {{5, true}, {4, false}, {2, false}, {1, false}, {3, false}},
                  {{}, {"200", "0", "efg"}, {"200", "2", "bcd"}, {"100", "1", "abc"}, {"100", "1", "def"}});
    ASSERT_FALSE(iterator.HasNext());
}

TEST_F(SortedMultiSegmentKVIteratorTest, testMultiSegments)
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
    std::reverse(segments.begin(), segments.end());

    SortedMultiSegmentKVIterator iterator(_schema->GetSchemaId(), _config, nullptr, std::move(segments),
                                          _indexerParam.sortDescriptions);
    ASSERT_TRUE(iterator.Init().IsOK());
    ASSERT_EQ(3, iterator._iters.size());
    ASSERT_EQ(3, iterator._unusedKeys.size());
    ASSERT_TRUE(iterator._unusedKeys.count({1, 2}) > 0);
    ASSERT_TRUE(iterator._unusedKeys.count({3, 2}) > 0);
    ASSERT_TRUE(iterator._unusedKeys.count({4, 2}) > 0);
    auto keys = std::vector<std::pair<keytype_t, bool>> {{3, true},  {5, true},  {6, false}, {4, false},
                                                         {1, false}, {2, false}, {7, false}};
    auto values = std::vector<std::vector<std::string>> {
        {}, {}, {"600", "6", "666"}, {"100", "0", "efg"}, {"20", "2", "abc"}, {"20", "2", "bcd"}, {"7", "7", "777"}};
    CheckIterator(&iterator, keys, values);
    ASSERT_FALSE(iterator.HasNext());
}

} // namespace indexlibv2::index
