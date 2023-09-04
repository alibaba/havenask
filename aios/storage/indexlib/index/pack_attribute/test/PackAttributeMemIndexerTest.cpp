#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/index/pack_attribute/test/PackAttributeTestHelper.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class PackAttributeMemIndexerTest : public TESTBASE
{
public:
    PackAttributeMemIndexerTest() = default;
    ~PackAttributeMemIndexerTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(PackAttributeMemIndexerTest, testSimple)
{
    PackAttributeTestHelper helper;
    ASSERT_TRUE(helper.MakeSchema("int:int32;str:string", "packAttr:int,str").IsOK());
    std::string rootPath = GET_TEMP_DATA_PATH();
    ASSERT_TRUE(helper.Open(rootPath, rootPath).IsOK());
    std::string docStr = "cmd=add,str=k0,int=0;cmd=add,str=k1,int=1";
    ASSERT_TRUE(helper.Build(docStr).IsOK());
    ASSERT_TRUE(helper.Dump().IsOK());
    ASSERT_TRUE(helper.Query(0, "str=k0,int=0"));
    ASSERT_TRUE(helper.Query(1, "str=k1,int=1"));

    docStr = "cmd=add,str=k2,int=2;cmd=add,str=k3,int=3";
    ASSERT_TRUE(helper.Build(docStr).IsOK());
    ASSERT_TRUE(helper.Query(0, "str=k0,int=0"));
    ASSERT_TRUE(helper.Query(2, "str=k2,int=2"));
    ASSERT_TRUE(helper.Query(3, "str=k3,int=3"));
}

TEST_F(PackAttributeMemIndexerTest, testSingleOnly)
{
    PackAttributeTestHelper helper;
    ASSERT_TRUE(helper.MakeSchema("int:int32", "packAttr:int").IsOK());
    std::string rootPath = GET_TEMP_DATA_PATH();
    ASSERT_TRUE(helper.Open(rootPath, rootPath).IsOK());
    std::string docStr = "cmd=add,int=0;cmd=add,int=1";
    ASSERT_TRUE(helper.Build(docStr).IsOK());
    ASSERT_TRUE(helper.Dump().IsOK());
    ASSERT_TRUE(helper.Query(0, "int=0"));
    ASSERT_TRUE(helper.Query(1, "int=1"));

    docStr = "cmd=add,int=2;cmd=add,int=3";
    ASSERT_TRUE(helper.Build(docStr).IsOK());
    ASSERT_TRUE(helper.Query(0, "int=0"));
    ASSERT_TRUE(helper.Query(2, "int=2"));
    ASSERT_TRUE(helper.Query(3, "int=3"));
}

TEST_F(PackAttributeMemIndexerTest, testMultiOnly)
{
    PackAttributeTestHelper helper;
    ASSERT_TRUE(helper.MakeSchema("str:string", "packAttr:str").IsOK());
    std::string rootPath = GET_TEMP_DATA_PATH();
    ASSERT_TRUE(helper.Open(rootPath, rootPath).IsOK());
    std::string docStr = "cmd=add,str=k0;cmd=add,str=k1";
    ASSERT_TRUE(helper.Build(docStr).IsOK());
    ASSERT_TRUE(helper.Dump().IsOK());
    ASSERT_TRUE(helper.Query(0, "str=k0"));
    ASSERT_TRUE(helper.Query(1, "str=k1"));

    docStr = "cmd=add,str=k2;cmd=add,str=k3";
    ASSERT_TRUE(helper.Build(docStr).IsOK());
    ASSERT_TRUE(helper.Query(0, "str=k0"));
    ASSERT_TRUE(helper.Query(2, "str=k2"));
    ASSERT_TRUE(helper.Query(3, "str=k3"));
}

} // namespace indexlibv2::index
