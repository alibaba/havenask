#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/index/pack_attribute/test/PackAttributeTestHelper.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class PackAttributeIndexMergerTest : public TESTBASE
{
public:
    PackAttributeIndexMergerTest() = default;
    ~PackAttributeIndexMergerTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(PackAttributeIndexMergerTest, testSimple)
{
    PackAttributeTestHelper helper;
    ASSERT_TRUE(helper.MakeSchema("int:int32;str:string", "packAttr:int,str").IsOK());
    std::string rootPath = GET_TEMP_DATA_PATH();
    ASSERT_TRUE(helper.Open(rootPath, rootPath).IsOK());
    auto seg0 = helper.BuildDiskSegment("cmd=add,str=k0,int=0;cmd=add,str=k1,int=1").get();
    auto seg1 = helper.BuildDiskSegment("cmd=add,str=k2,int=2").get();
    ASSERT_TRUE(helper.Merge({seg0, seg1}, {}, "").IsOK());
    ASSERT_TRUE(helper.Query(0, "str=k0,int=0"));
}

TEST_F(PackAttributeIndexMergerTest, testMultiOutputSegment)
{
    {
        PackAttributeTestHelper helper;
        std::string fieldsDesc = "int:int32;flt:float;mdbl:double:true;str:string;mstr:string:true";
        std::string indexDesc = "packAttr:int,flt,mdbl,str,mstr";
        ASSERT_TRUE(helper.MakeSchema(fieldsDesc, indexDesc).IsOK());
        std::string rootPath = GET_TEMP_DATA_PATH();
        ASSERT_TRUE(helper.Open(rootPath, rootPath).IsOK());
        auto Build = [&helper](const std::string& docStr) { return helper.BuildDiskSegment(docStr).get(); };

        ASSERT_EQ(0, Build("cmd=add,str=k01,int=1,flt=10,mdbl=10 11;"
                           "cmd=add,str=k02,int=2,flt=20"));
        ASSERT_EQ(1, Build("cmd=add,str=k11,int=11,flt=110,mdbl=110 111;"
                           "cmd=add,str=k12,int=12,flt=120"));
        ASSERT_EQ(2, Build("cmd=add,str=k21,int=21,flt=210,mdbl=210 211"));
        ASSERT_EQ(3, Build("cmd=add,str=k31,int=31,flt=310,mdbl=310 311"));
        // 0:k01,k02 | 1:k11,k12 | 2:k21 | 3:k31 -->
        // 4:k01,k21 | 5:k31,k01,k12
        // oldSegId:oldDocId>newSegId:newDocId
        std::string docMapStr = "0:0>4:0, 0:1>5:1, 1:0>:, 1:1>5:2, 2:0>4:1, 3:0>5:0";
        ASSERT_EQ((std::vector<segmentid_t> {4, 5}), helper.Merge({0, 1, 2, 3}, {4, 5}, docMapStr).get());
        ASSERT_TRUE(helper.Query(0, "str=k01,int=1,flt=10,mdbl=10 11"));
        ASSERT_TRUE(helper.Query(1, "str=k21,int=21,flt=210,mdbl=210 211"));
        ASSERT_TRUE(helper.Query(2, "str=k31,int=31,flt=310,mdbl=310 311"));
        ASSERT_TRUE(helper.Query(3, "str=k02,int=2,flt=20,mdbl="));
        ASSERT_TRUE(helper.Query(4, "str=k12,int=12,flt=120,mdbl="));
    }
    {
        PackAttributeTestHelper helper;
        std::string fieldsDesc = "int:int32;flt:float;mdbl:double:true;str:string;mstr:string:true";
        std::string indexDesc = "packAttr:int,flt,mdbl,str,mstr";
        ASSERT_TRUE(helper.MakeSchema(fieldsDesc, indexDesc).IsOK());
        std::string rootPath = GET_TEMP_DATA_PATH();
        ASSERT_TRUE(helper.Open(rootPath, rootPath).IsOK());
        auto Build = [&helper](const std::string& docStr) { return helper.BuildDiskSegment(docStr).get(); };
        ASSERT_EQ(0, Build("cmd=add,str=k01,int=1,flt=10,mdbl=10 11;"
                           "cmd=add,str=k02,int=2,flt=20"));
        ASSERT_EQ(1, Build("cmd=add,str=k11,int=11,flt=110,mdbl=110 111;"
                           "cmd=add,str=k12,int=12,flt=120"));
        ASSERT_EQ(2, Build("cmd=add,str=k21,int=21,flt=210,mdbl=210 211"));
        ASSERT_EQ(3, Build("cmd=add,str=k31,int=31,flt=310,mdbl=310 311"));
        // 0:k01,k02 | 1:k11,k12 | 2:k21 | 3:k31 -->
        // 0:k01,k02 | 4:k11 | 5:k12,k31
        std::string docMapStr = "1:0>4:0, 1:1>5:0, 2:0>:, 3:0>5:1";
        ASSERT_EQ((std::vector<segmentid_t> {4, 5}), helper.Merge({1, 2, 3}, {4, 5}, docMapStr).get());
        ASSERT_TRUE(helper.Query(0, "str=k01,int=1,flt=10,mdbl=10 11"));
        ASSERT_TRUE(helper.Query(1, "str=k02,int=2,flt=20,mdbl="));
        ASSERT_TRUE(helper.Query(2, "str=k11,int=11,flt=110,mdbl=110 111"));
        ASSERT_TRUE(helper.Query(3, "str=k12,int=12,flt=120,mdbl="));
        ASSERT_TRUE(helper.Query(4, "str=k31,int=31,flt=310,mdbl=310 311"));
    }
}

} // namespace indexlibv2::index
