#include "indexlib/util/counter/CounterMap.h"

#include "indexlib/util/Exception.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/Counter.h"
#include "indexlib/util/counter/MultiCounter.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/counter/StringCounter.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace util {

class CounterMapTest : public INDEXLIB_TESTBASE
{
public:
    CounterMapTest();
    ~CounterMapTest();

    DECLARE_CLASS_NAME(CounterMapTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetNotExistCounter();
    void TestGetExistCounter();
    void TestGetByNodePath();
    void TestMerge();
    void TestMergeException();
    void TestJsonize();
    void TestJsonizeException();
    void TestFindCounters();
    void TestCreateMultiCounter();
};

INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestGetNotExistCounter);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestGetExistCounter);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestGetByNodePath);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestMerge);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestMergeException);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestJsonize);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestJsonizeException);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestFindCounters);
INDEXLIB_UNIT_TEST_CASE(CounterMapTest, TestCreateMultiCounter);

CounterMapTest::CounterMapTest() {}

CounterMapTest::~CounterMapTest() {}

void CounterMapTest::CaseSetUp() {}

void CounterMapTest::CaseTearDown() {}

void CounterMapTest::TestGetNotExistCounter()
{
    CounterMap counterMap;
    auto accCounter = counterMap.GetAccCounter("a");
    ASSERT_NE(nullptr, accCounter);
    accCounter->Increase(1);
    ASSERT_EQ(string("{\n\"__type__\":\n  \"ACC\",\n\"value\":\n  1\n}"), counterMap.Get("a"));
    auto counterInGroup = counterMap.GetAccCounter("group.b");
    ASSERT_NE(nullptr, counterInGroup);
    counterInGroup->Increase(2);
    ASSERT_EQ(string("{\n\"__type__\":\n  \"ACC\",\n\"value\":\n  2\n}"), counterMap.Get("group.b"));
}

void CounterMapTest::TestGetExistCounter()
{
    CounterMap counterMap;
    CounterPtr counterA = counterMap.GetAccCounter("group.a");
    CounterPtr counterInvalid = counterMap.GetAccCounter("group");
    ASSERT_FALSE(counterInvalid);
    CounterPtr counterB = counterMap.GetAccCounter("group.a");
    ASSERT_EQ(counterA, counterB);
    CounterPtr counterC = counterMap.GetStateCounter("group.b");
    ASSERT_NE(nullptr, counterC);

    ASSERT_FALSE(counterMap.GetStateCounter("group.a"));
    ASSERT_FALSE(counterMap.GetAccCounter("group.b"));
}

void CounterMapTest::TestGetByNodePath()
{
    CounterMap counterMap;
    auto counterA = counterMap.GetStateCounter("group.a");
    counterA->Set(10);
    auto counterB = counterMap.GetAccCounter("group.b");
    counterB->Increase(20);
    string result = counterMap.Get("group");
    string expectResult = R"({
"__type__":
  "DIR",
"a":
  {
  "__type__":
    "STATE",
  "value":
    10
  },
"b":
  {
  "__type__":
    "ACC",
  "value":
    20
  }
})";
    ASSERT_EQ(expectResult, result);
}

void CounterMapTest::TestMerge()
{
    CounterMap counterMap1;
    counterMap1.GetStateCounter("offline.a")->Set(10);
    counterMap1.GetAccCounter("offline.builder.b")->Increase(20);
    counterMap1.GetStateCounter("offline.processor.reader.c")->Set(30);

    string result = counterMap1.ToJsonString();
    CounterMap counterMap2;
    counterMap2.GetStateCounter("online.d")->Set(40);
    counterMap2.GetAccCounter("offline.builder.b")->Increase(50);
    counterMap2.GetStateCounter("offline.processor.reader.c")->Set(60);

    counterMap2.Merge(result, CounterBase::FJT_MERGE);
    EXPECT_EQ(40, counterMap2.GetStateCounter("online.d")->Get());
    EXPECT_EQ(10, counterMap2.GetStateCounter("offline.a")->Get());
    EXPECT_EQ(20 + 50, counterMap2.GetAccCounter("offline.builder.b")->Get());
    EXPECT_EQ(30, counterMap2.GetStateCounter("offline.processor.reader.c")->Get());

    CounterMap counterMap3;
    counterMap3.GetStateCounter("online.d")->Set(40);
    counterMap3.Merge(result, CounterBase::FJT_OVERWRITE);
    EXPECT_EQ(40, counterMap3.GetStateCounter("online.d")->Get());
    EXPECT_EQ(10, counterMap3.GetStateCounter("offline.a")->Get());
    EXPECT_EQ(20, counterMap3.GetAccCounter("offline.builder.b")->Get());
    EXPECT_EQ(30, counterMap3.GetStateCounter("offline.processor.reader.c")->Get());

    counterMap3.Merge(result, CounterBase::FJT_OVERWRITE);
    EXPECT_EQ(40, counterMap3.GetStateCounter("online.d")->Get());
    EXPECT_EQ(10, counterMap3.GetStateCounter("offline.a")->Get());
    EXPECT_EQ(20, counterMap3.GetAccCounter("offline.builder.b")->Get());
    EXPECT_EQ(30, counterMap3.GetStateCounter("offline.processor.reader.c")->Get());

    counterMap3.Merge(CounterMap::EMPTY_COUNTER_MAP_JSON, CounterBase::FJT_OVERWRITE);
    EXPECT_EQ(40, counterMap3.GetStateCounter("online.d")->Get());
    EXPECT_EQ(10, counterMap3.GetStateCounter("offline.a")->Get());
    EXPECT_EQ(20, counterMap3.GetAccCounter("offline.builder.b")->Get());
    EXPECT_EQ(30, counterMap3.GetStateCounter("offline.processor.reader.c")->Get());

    counterMap3.Merge(CounterMap::EMPTY_COUNTER_MAP_JSON, CounterBase::FJT_MERGE);
    EXPECT_EQ(40, counterMap3.GetStateCounter("online.d")->Get());
    EXPECT_EQ(10, counterMap3.GetStateCounter("offline.a")->Get());
    EXPECT_EQ(20, counterMap3.GetAccCounter("offline.builder.b")->Get());
    EXPECT_EQ(30, counterMap3.GetStateCounter("offline.processor.reader.c")->Get());
}

void CounterMapTest::TestMergeException()
{
    CounterMap counterMap1;
    counterMap1.GetStateCounter("offline.a")->Set(10);
    counterMap1.GetAccCounter("offline.builder.b")->Increase(20);
    counterMap1.GetStateCounter("offline.processor.reader.c")->Set(30);

    CounterMap counterMap2;
    counterMap2.GetStateCounter("offline.builder.b")->Set(20);
    auto content = counterMap2.ToJsonString();

    ASSERT_THROW(counterMap1.Merge(content, CounterBase::FJT_MERGE), InconsistentStateException);
    ASSERT_THROW(counterMap1.Merge(content, CounterBase::FJT_OVERWRITE), InconsistentStateException);
}

void CounterMapTest::TestJsonize()
{
    CounterMap counterMap;
    auto counterA = counterMap.GetStateCounter("group.a");
    counterA->Set(10);
    auto counterB = counterMap.GetAccCounter("group.b");
    counterB->Increase(-20);
    string result = counterMap.ToJsonString(false);
    string expectResult = R"({
"__type__":
  "DIR",
"group":
  {
  "__type__":
    "DIR",
  "a":
    {
    "__type__":
      "STATE",
    "value":
      10
    },
  "b":
    {
    "__type__":
      "ACC",
    "value":
      -20
    }
  }
})";
    ASSERT_EQ(expectResult, result);

    CounterMap counterMap2;
    counterMap2.FromJsonString(expectResult);
    ASSERT_EQ(expectResult, counterMap2.ToJsonString(false));
}

void CounterMapTest::TestJsonizeException()
{
    string jsonStr = R"("{"a":  "str"}")";
    CounterMap counterMap;
    ASSERT_ANY_THROW(counterMap.FromJsonString(jsonStr));
}

void CounterMapTest::TestFindCounters()
{
    CounterMap counterMap;
    string jsonStr = R"(
{
    "__type__" : "DIR",
    "bs" :
    {
        "__type__" : "DIR",
        "processor" :
        {
            "__type__" : "DIR",
            "swiftReadDelay" :
            {
                "__type__" : "STATE",
                "value": 10
            },
            "checkpoint" :
            {
                "__type__" : "STRING",
                "value": "test-checkpoint"
            },
            "parseError" :
            {
                "value": 20,
                "__type__" : "ACC"
            }
        },
        "builder" :
        {
            "__type__" : "DIR",
            "full" :
            {
                "__type__" : "DIR",
                "docCount" :
                {
                    "__type__" : "ACC",
                    "value" : 10
                },
                "addDocCount" :
                {
                    "__type__" : "ACC",
                    "value" : 23
                }
            },
            "deleteDocCount" :
            {
                "__type__" : "ACC",
                "value" : 24
            }
        }
    },
    "index" :
    {
        "__type__" : "STATE",
        "value" : 16
    }
}
)";
    ASSERT_NO_THROW(counterMap.FromJsonString(jsonStr));
    vector<CounterBasePtr> counterVec = counterMap.FindCounters("non-exist");
    EXPECT_EQ(0u, counterVec.size());

    counterVec = counterMap.FindCounters("bs.builder");
    EXPECT_EQ(3u, counterVec.size());
    EXPECT_EQ(string("bs.builder.deleteDocCount"), counterVec[0]->GetPath());
    EXPECT_EQ(24, std::dynamic_pointer_cast<Counter>(counterVec[0])->Get());
    EXPECT_EQ(string("bs.builder.full.addDocCount"), counterVec[1]->GetPath());
    EXPECT_EQ(23, std::dynamic_pointer_cast<Counter>(counterVec[1])->Get());
    EXPECT_EQ(string("bs.builder.full.docCount"), counterVec[2]->GetPath());
    EXPECT_EQ(10, std::dynamic_pointer_cast<Counter>(counterVec[2])->Get());

    counterVec = counterMap.FindCounters("bs.processor.non-exist");
    EXPECT_EQ(0u, counterVec.size());

    counterVec = counterMap.FindCounters("bs.processor.parseError");
    EXPECT_EQ(1u, counterVec.size());
    EXPECT_EQ(string("bs.processor.parseError"), counterVec[0]->GetPath());
    EXPECT_EQ(20, std::dynamic_pointer_cast<Counter>(counterVec[0])->Get());

    counterVec = counterMap.FindCounters("bs.processor.checkpoint");
    EXPECT_EQ(1u, counterVec.size());
    EXPECT_EQ(string("bs.processor.checkpoint"), counterVec[0]->GetPath());
    EXPECT_EQ("test-checkpoint", std::dynamic_pointer_cast<StringCounter>(counterVec[0])->Get());

    counterVec = counterMap.FindCounters("index");
    EXPECT_EQ(1u, counterVec.size());
    EXPECT_EQ(string("index"), counterVec[0]->GetPath());
    EXPECT_EQ(16u, std::dynamic_pointer_cast<Counter>(counterVec[0])->Get());

    counterVec = counterMap.FindCounters("");
    ASSERT_EQ(7u, counterVec.size());
    EXPECT_EQ(string("bs.builder.deleteDocCount"), counterVec[0]->GetPath());
    EXPECT_EQ(24, std::dynamic_pointer_cast<Counter>(counterVec[0])->Get());
    EXPECT_EQ(string("bs.builder.full.addDocCount"), counterVec[1]->GetPath());
    EXPECT_EQ(23, std::dynamic_pointer_cast<Counter>(counterVec[1])->Get());
    EXPECT_EQ(string("bs.builder.full.docCount"), counterVec[2]->GetPath());
    EXPECT_EQ(10, std::dynamic_pointer_cast<Counter>(counterVec[2])->Get());

    EXPECT_EQ(string("bs.processor.checkpoint"), counterVec[3]->GetPath());
    EXPECT_EQ("test-checkpoint", std::dynamic_pointer_cast<StringCounter>(counterVec[3])->Get());
    EXPECT_EQ(string("bs.processor.parseError"), counterVec[4]->GetPath());
    EXPECT_EQ(20, std::dynamic_pointer_cast<Counter>(counterVec[4])->Get());
    EXPECT_EQ(string("bs.processor.swiftReadDelay"), counterVec[5]->GetPath());
    EXPECT_EQ(10, std::dynamic_pointer_cast<Counter>(counterVec[5])->Get());
    EXPECT_EQ(string("index"), counterVec[6]->GetPath());
    EXPECT_EQ(16, std::dynamic_pointer_cast<Counter>(counterVec[6])->Get());
}

void CounterMapTest::TestCreateMultiCounter()
{
    CounterMap counterMap;
    MultiCounterPtr c1 = counterMap.GetMultiCounter("attribute");
    c1->CreateStateCounter("a")->Set(10);
    c1->CreateStateCounter("b")->Set(20);
    auto accCounter = c1->CreateAccCounter("c");
    accCounter->Increase(14);
    accCounter->Increase(16);
    c1->CreateMultiCounter("d")->CreateStateCounter("size")->Set(40);
    EXPECT_EQ(10, counterMap.GetStateCounter("attribute.a")->Get());
    EXPECT_EQ(20, counterMap.GetStateCounter("attribute.b")->Get());
    EXPECT_EQ(30, counterMap.GetAccCounter("attribute.c")->Get());
    EXPECT_EQ(40, counterMap.GetStateCounter("attribute.d.size")->Get());
    EXPECT_EQ(100, c1->Sum());
}
}} // namespace indexlib::util
