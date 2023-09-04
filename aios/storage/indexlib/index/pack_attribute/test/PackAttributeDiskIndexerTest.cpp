#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/index/pack_attribute/test/PackAttributeTestHelper.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class PackAttributeDiskIndexerTest : public TESTBASE
{
public:
    PackAttributeDiskIndexerTest() = default;
    ~PackAttributeDiskIndexerTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(PackAttributeDiskIndexerTest, testSimple)
{
    PackAttributeTestHelper helper;
    ASSERT_TRUE(helper.MakeSchema("int:int32;str:string", "packAttr:int,str").IsOK());
    std::string rootPath = GET_TEMP_DATA_PATH();
    ASSERT_TRUE(helper.Open(rootPath, rootPath).IsOK());
    ASSERT_TRUE(helper.BuildDiskSegment("cmd=add,str=k0,int=0;cmd=add,str=k1,int=1").IsOK());
    ASSERT_TRUE(helper.Query(0, "str=k0,int=0"));
    ASSERT_TRUE(helper.Query(1, "str=k1,int=1"));
    ASSERT_TRUE(helper.BuildDiskSegment("cmd=add,str=k2,int=2").IsOK());
    ASSERT_TRUE(helper.Query(0, "str=k0,int=0"));
    ASSERT_TRUE(helper.Query(2, "str=k2,int=2"));
    ASSERT_TRUE(helper.Build("cmd=add,str=k3,int=3").IsOK());
    ASSERT_TRUE(helper.Query(3, "str=k3,int=3"));
}

TEST_F(PackAttributeDiskIndexerTest, testMultiSingleValue)
{
    PackAttributeTestHelper helper;
    ASSERT_TRUE(helper.MakeSchema("int:int32;long:uint64;flt:float", "packAttr:int,long,flt").IsOK());
    std::string rootPath = GET_TEMP_DATA_PATH();
    ASSERT_TRUE(helper.Open(rootPath, rootPath).IsOK());
    ASSERT_TRUE(helper.BuildDiskSegment("cmd=add,long=11,int=-1,flt=10;cmd=add,long=22,int=-2,flt=20").IsOK());
    ASSERT_TRUE(helper.Query(0, "long=11,int=-1,flt=10"));
    ASSERT_TRUE(helper.Query(1, "long=22,int=-2,flt=20"));
    ASSERT_TRUE(helper.BuildDiskSegment("cmd=add,long=33,int=-3,flt=30").IsOK());
    ASSERT_TRUE(helper.Query(0, "long=11,int=-1,flt=10"));
    ASSERT_TRUE(helper.Query(2, "long=33,int=-3,flt=30"));
    ASSERT_TRUE(helper.Build("cmd=add,long=44,int=-4,flt=40").IsOK());
    ASSERT_TRUE(helper.Query(3, "long=44,int=-4,flt=40"));
}

} // namespace indexlibv2::index
