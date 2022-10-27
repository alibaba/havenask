#include <ha3_sdk/testlib/index/FakeMetaMaker.h>
#include <unittest/unittest.h>
#include <ha3/util/Log.h>

using namespace std;
IE_NAMESPACE_BEGIN(index);

class FakeSectionAttributeReaderTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(index, FakeSectionAttributeReaderTest);

TEST_F(FakeSectionAttributeReaderTest, testGetSection) {
    string first = "1:1-10[0_10_2,1_10_1];2-30[0_8_1];\n";
    string second = "3:1-8[0_20_1,1_21_5,2_15_6];\n";
    FakeTextIndexReader::DocSectionMap docSecMetaMap;
    FakeMetaMaker::makeFakeMeta(first + second, docSecMetaMap);
    FakeSectionAttributeReader sectionAttrReader(docSecMetaMap);
    InDocSectionMetaPtr sectionMetaPtr = sectionAttrReader.GetSection((docid_t)1);
    ASSERT_EQ((section_len_t)8, sectionMetaPtr->GetSectionLen(2, 0));
    ASSERT_EQ((section_weight_t)2, sectionMetaPtr->GetSectionWeight(1, 0));
    ASSERT_EQ((field_len_t)30, sectionMetaPtr->GetFieldLen(2));
}

IE_NAMESPACE_END(index);
