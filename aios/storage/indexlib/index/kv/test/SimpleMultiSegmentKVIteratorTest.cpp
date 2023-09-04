#include "indexlib/index/kv/SimpleMultiSegmentKVIterator.h"

#include "indexlib/index/kv/test/MultiSegmentKVIteratorTestBase.h"

namespace indexlibv2::index {

class SimpleMultiSegmentKVIteratorTest : public MultiSegmentKVIteratorTestBase
{
};

TEST_F(SimpleMultiSegmentKVIteratorTest, testNoSegment)
{
    SimpleMultiSegmentKVIterator iterator(_schema->GetSchemaId(), _config, nullptr, {});
    ASSERT_TRUE(iterator.Init().IsOK());
    ASSERT_FALSE(iterator.HasNext());
}

TEST_F(SimpleMultiSegmentKVIteratorTest, testSegmentEmpty)
{
    std::vector<std::shared_ptr<framework::Segment>> segments;
    PrepareSegments({"", "", ""}, segments);
    ASSERT_EQ(3, segments.size());

    SimpleMultiSegmentKVIterator iterator(_schema->GetSchemaId(), _config, nullptr, std::move(segments));
    ASSERT_TRUE(iterator.Init().IsOK());
    ASSERT_TRUE(iterator.HasNext());
    Record r;
    auto s = iterator.Next(_pool.get(), r);
    ASSERT_TRUE(s.IsEof()) << s.ToString();
    ASSERT_FALSE(iterator.HasNext());
}

TEST_F(SimpleMultiSegmentKVIteratorTest, testOneSegment)
{
    auto docs = "cmd=add,key=1,f1=100,f2=1,f3=abc;"
                "cmd=add,key=2,f1=200,f2=2,f3=bcd;"
                "cmd=add,key=3,f1=100,f2=1,f3=def;"
                "cmd=add,key=4,f1=200,f2=0,f3=efg;"
                "cmd=delete,key=5;";
    std::vector<std::shared_ptr<framework::Segment>> segments;
    PrepareSegments({std::move(docs)}, segments);
    ASSERT_EQ(1, segments.size());

    SimpleMultiSegmentKVIterator iterator(_schema->GetSchemaId(), _config, nullptr, std::move(segments));
    ASSERT_TRUE(iterator.Init().IsOK());
    ASSERT_TRUE(iterator.HasNext());
    CheckIterator(&iterator, {{5, true}, {4, false}, {2, false}, {1, false}, {3, false}},
                  {{}, {"200", "0", "efg"}, {"200", "2", "bcd"}, {"100", "1", "abc"}, {"100", "1", "def"}});
    ASSERT_TRUE(iterator.HasNext());
    Record r;
    auto s = iterator.Next(_pool.get(), r);
    ASSERT_TRUE(s.IsEof()) << s.ToString();
    ASSERT_FALSE(iterator.HasNext());
}

TEST_F(SimpleMultiSegmentKVIteratorTest, testMultiSegments)
{
    auto seg0 = "cmd=add,key=1,f1=100,f2=1,f3=abc;"
                "cmd=add,key=2,f1=200,f2=2,f3=bcd;"
                "cmd=add,key=3,f1=100,f2=1,f3=def;"
                "cmd=add,key=4,f1=200,f2=0,f3=efg;"
                "cmd=delete,key=5;";
    auto seg2 = "cmd=add,key=1,f1=150,f2=2,f3=abc;"
                "cmd=add,key=2,f1=200,f2=2,f3=bcd;"
                "cmd=delete,key=3;"
                "cmd=add,key=4,f1=100,f2=0,f3=efg;";
    std::vector<std::shared_ptr<framework::Segment>> segments;
    PrepareSegments({std::move(seg0), "", std::move(seg2)}, segments);
    ASSERT_EQ(3, segments.size());
    std::reverse(segments.begin(), segments.end());

    SimpleMultiSegmentKVIterator iterator(_schema->GetSchemaId(), _config, nullptr, std::move(segments));
    ASSERT_TRUE(iterator.Init().IsOK());
    auto keys = std::vector<std::pair<keytype_t, bool>> {{3, true},  {2, false}, {1, false}, {4, false}, {5, true},
                                                         {4, false}, {2, false}, {1, false}, {3, false}};
    auto values = std::vector<std::vector<std::string>> {{},
                                                         {"200", "2", "bcd"},
                                                         {"150", "2", "abc"},
                                                         {"100", "0", "efg"},
                                                         {},
                                                         {"200", "0", "efg"},
                                                         {"200", "2", "bcd"},
                                                         {"100", "1", "abc"},
                                                         {"100", "1", "def"}};
    CheckIterator(&iterator, keys, values);
}

} // namespace indexlibv2::index
