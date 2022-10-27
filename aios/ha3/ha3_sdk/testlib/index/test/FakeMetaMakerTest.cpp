#include <unittest/unittest.h>
#include <ha3/util/Log.h>
#include <ha3/index/index.h>
#include <ha3_sdk/testlib/index/FakeMetaMaker.h>
#include <string>

using namespace std;
IE_NAMESPACE_BEGIN(index);

class FakeMetaMakerTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(index, FakeMetaMakerTest);

TEST_F(FakeMetaMakerTest, testMakeFakeMeta) {
    string first = "1:1-10[0_10_2,1_10_1];2-30[0_8_1];\n";
    string second = "3:1-8[0_20_1,1_21_5,2_15_6];\n";
    FakeTextIndexReader::DocSectionMap docSecMetaMap;
    FakeMetaMaker::makeFakeMeta(first + second, docSecMetaMap);
    size_t mapSize = docSecMetaMap.size();
    ASSERT_EQ((size_t)2, mapSize);
    FakeTextIndexReader::DocSectionMap::iterator iter =
        docSecMetaMap.find((docid_t)1);
    ASSERT_TRUE(iter != docSecMetaMap.end());
    std::map<int32_t, field_len_t> fieldLenMap = iter->second.fieldLength;
    mapSize = fieldLenMap.size();
    ASSERT_EQ((size_t)2, mapSize);
    std::map<int32_t, field_len_t>::iterator iter2 = 
        fieldLenMap.find((int32_t)1);
    ASSERT_TRUE(iter2 != fieldLenMap.end());
    ASSERT_EQ((field_len_t)10, iter2->second);
    iter2 = fieldLenMap.find((int32_t)2);
    ASSERT_TRUE(iter2 != fieldLenMap.end());
    ASSERT_EQ((field_len_t)30, iter2->second);

    iter = docSecMetaMap.find((docid_t)3);
    ASSERT_TRUE(iter != docSecMetaMap.end());
    FakeTextIndexReader::FieldAndSectionMap fieldSecMap
        = iter->second.fieldAndSecionInfo;
    ASSERT_EQ((size_t)3, fieldSecMap.size());
    FakeTextIndexReader::FieldAndSectionMap::iterator iter3 =
        fieldSecMap.find(std::make_pair(1, 0));
    ASSERT_TRUE(iter3 != fieldSecMap.end());
    ASSERT_EQ((section_len_t)20, iter3->second.sectionLength);
    ASSERT_EQ((section_weight_t)1, iter3->second.sectionWeight);
    iter3 = fieldSecMap.find(std::make_pair(1, 2));
    ASSERT_TRUE(iter3 != fieldSecMap.end());
    ASSERT_EQ((section_len_t)15, iter3->second.sectionLength);
    ASSERT_EQ((section_weight_t)6, iter3->second.sectionWeight);
       
    ASSERT_TRUE(true);
}

IE_NAMESPACE_END(index);
