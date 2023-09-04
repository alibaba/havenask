#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/index/pack_attribute/test/PackAttributeTestHelper.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class PackAttributeIndexReaderTest : public TESTBASE
{
public:
    PackAttributeIndexReaderTest() = default;
    ~PackAttributeIndexReaderTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(PackAttributeIndexReaderTest, testAccessCounter)
{
    PackAttributeTestHelper helper;
    ASSERT_TRUE(helper.MakeSchema("int:int32;str:string", "packAttr:int,str").IsOK());

    auto metricsManager = std::make_shared<framework::MetricsManager>(helper.GetTableName(), nullptr);
    helper.MutableIndexerParameter()->metricsManager = metricsManager.get();
    std::string rootPath = GET_TEMP_DATA_PATH();
    ASSERT_TRUE(helper.Open(rootPath, rootPath).IsOK());

    const auto& counterMap = metricsManager->GetCounterMap();
    ASSERT_TRUE(counterMap);
    const auto& intCounter = std::dynamic_pointer_cast<indexlib::util::AccumulativeCounter>(
        counterMap->GetAccCounter("online.access.attribute.int"));
    ASSERT_TRUE(intCounter);
    const auto& strCounter = std::dynamic_pointer_cast<indexlib::util::AccumulativeCounter>(
        counterMap->GetAccCounter("online.access.attribute.str"));
    ASSERT_TRUE(strCounter);
    ASSERT_EQ(0L, intCounter->Get());
    ASSERT_EQ(0L, strCounter->Get());

    ASSERT_TRUE(helper.BuildDiskSegment("cmd=add,str=k0,int=0;cmd=add,str=k1,int=1").IsOK());
    ASSERT_TRUE(helper.Query(0, "str=k0,int=0")); // 1
    ASSERT_EQ(1L, intCounter->Get());
    ASSERT_EQ(1L, strCounter->Get());

    ASSERT_TRUE(helper.Query(1, "str=k1,int=1")); // 2
    ASSERT_TRUE(helper.BuildDiskSegment("cmd=add,str=k2,int=2").IsOK());
    ASSERT_TRUE(helper.Query(0, "str=k0,int=0")); // 3
    ASSERT_TRUE(helper.Query(2, "str=k2,int=2")); // 4
    ASSERT_TRUE(helper.Build("str=k3,int=3").IsOK());
    ASSERT_TRUE(helper.Query(3, "str=k3,int=3")); // 5
    ASSERT_EQ(5L, intCounter->Get());
    ASSERT_EQ(5L, strCounter->Get());
}

} // namespace indexlibv2::index
