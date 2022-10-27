#include <unittest/unittest.h>
#include <ha3/index/index.h>
#include <ha3_sdk/testlib/index/FakePostingMaker.h>
#include <ha3/util/Log.h>
#include <sstream>

using namespace std;
IE_NAMESPACE_BEGIN(index);

class MakeFakePostingsTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(index, MakeFakePostingsTest);

static std::string combineString(FakeTextIndexReader::Map::iterator it) {
    std::stringstream ret;
    ret << it->first << ":";
    for (uint32_t i = 0; i < it->second.second.size(); i++) {
        ret << it->second.second[i].docid << "[";
        for (uint32_t j = 0; j < it->second.second[i].occArray.size(); j++) {
            ret << it->second.second[i].occArray[j];
            if (j + 1 < it->second.second[i].occArray.size()) {
                ret << ",";
            }
        }
        ret << "];";

    }
    ret << "\n";
    return ret.str();
}

static std::string combineStringDetail(FakeTextIndexReader::Map::iterator it) {
    std::stringstream ret;
    ret << it->first << "^"<< it->second.first << ":";
    for (uint32_t i = 0; i < it->second.second.size(); i++) {
         ret << it->second.second[i].docid << "^"<< it->second.second[i].docPayload<< "["; 
         for (uint32_t j = 0; j < it->second.second[i].occArray.size(); j++) {
	    ret << it->second.second[i].occArray[j] << "_" << (int32_t)it->second.second[i].fieldBitArray[j];
            if (j + 1 < it->second.second[i].occArray.size()) {
                ret << ",";
            }
        }
        ret << "];";

    }
    ret << "\n";
    return ret.str();
}

TEST_F(MakeFakePostingsTest, testMakeSimpleFakePostings) {
    HA3_LOG(DEBUG, "Begin Test");
    std::string str = "abc:1[1,2,5,7,100,909];3[2,5,90];\n";
    FakeTextIndexReader::Map mp;
    FakePostingMaker::makeFakePostingsDetail(str, mp);
    FakeTextIndexReader::Map::iterator it = mp.begin();
    ASSERT_EQ((size_t)1, mp.size());
    ASSERT_TRUE(it != mp.end());
    ASSERT_EQ(str, combineString(it));
}

TEST_F(MakeFakePostingsTest, testMakeFakePostings) {
    HA3_LOG(DEBUG, "Begin Test");
    std::string str = "abc^1:1^5[1_1,2_0,5_1,7_0,100_1,909_1];3^10[2_1,5_0,90_1];\n";
    FakeTextIndexReader::Map mp;
    FakePostingMaker::makeFakePostingsDetail(str, mp);
    FakeTextIndexReader::Map::iterator it = mp.begin();
    ASSERT_EQ((size_t)1, mp.size());
    ASSERT_TRUE(it != mp.end());
    ASSERT_EQ(str, combineStringDetail(it));
}

TEST_F(MakeFakePostingsTest, testMakeFakePostings2) {
    HA3_LOG(DEBUG, "Begin Test");
    string str1 = "abc^1:1^5[1_0,3_0,5_1,23_0];19^4[4_1,23_3,55_0];28^1[8_1,23_2,59_0];";
    string str2 = "def^2:2^6[1_2,3_3,5_3,23_4];19^7[5_0,27_1,59_1];28^8[10_0,24_2,52_1];";
    string str3 = "hijk^3:1^7[1_1,3_2,5_1,23_1];19^100[41_1,230_2,557_2];28^50[1_1,25_1,66_0];";
    
    FakeTextIndexReader::Map mp; 
    FakePostingMaker::makeFakePostingsDetail(str1, mp);
    FakePostingMaker::makeFakePostingsDetail(str2, mp);
    FakePostingMaker::makeFakePostingsDetail(str3, mp);
    ASSERT_EQ((size_t)3, mp.size());
    FakeTextIndexReader::Map::iterator it = mp.find("abc");
    ASSERT_TRUE(it != mp.end());
    ASSERT_EQ(str1 + "\n", combineStringDetail(it));  

    it = mp.find("def");
    ASSERT_TRUE(it != mp.end());
    ASSERT_EQ(str2 + "\n", combineStringDetail(it));  

    it = mp.find("hijk");
    ASSERT_TRUE(it != mp.end());
    ASSERT_EQ(str3 + "\n", combineStringDetail(it));
}

TEST_F(MakeFakePostingsTest, testMakeFakePostingsSection) {
    HA3_LOG(DEBUG, "Begin Test");
    string one = "IN:1[1_1_0];3[0_1_0,1_0_0,7_0_0,10_1_0,11_0_1,12_0_2];6[1_1_0];7[1_2_0];8[1_2_0];\n";
    string two = "HANGZHOU:2[1_2_0];3[2_0_0,3_1_0,8_0_0,9_0_1,13_0_2,14_0_2];5[1_2_0];8[9_1_0,10_2_0];\n";

    FakeTextIndexReader::Map mp;
    FakePostingMaker::makeFakePostingsSection(one, mp);
    FakePostingMaker::makeFakePostingsSection(two, mp);
    ASSERT_EQ((size_t)2, mp.size());
    FakeTextIndexReader::Map::iterator it = mp.find("IN");
    ASSERT_TRUE(it != mp.end());
    
    FakeTextIndexReader::Postings postings = it->second.second;
    ASSERT_EQ((size_t)5, postings.size());
    ASSERT_EQ((docid_t)1, postings[0].docid);
    ASSERT_EQ((docid_t)6, postings[2].docid);
    ASSERT_EQ((docid_t)8, postings[4].docid);
    
    
    FakeTextIndexReader::Posting posting = postings[1];
    ASSERT_EQ((size_t)6, posting.fieldBitArray.size());
    ASSERT_EQ((size_t)6, posting.sectionIdArray.size());
    ASSERT_EQ((size_t)6, posting.occArray.size());

    std::vector<sectionid_t>& sectionArray = posting.sectionIdArray;
    ASSERT_EQ((sectionid_t)2, sectionArray[5]);
    ASSERT_EQ((sectionid_t)1, sectionArray[4]);

    std::vector<pos_t> &occArray = posting.occArray;
    ASSERT_EQ((pos_t)7, occArray[2]);
    ASSERT_EQ((pos_t)10, occArray[3]);
    ASSERT_EQ((pos_t)12, occArray[5]);

    std::vector<int32_t> &fieldBitArray = posting.fieldBitArray;
    ASSERT_EQ((int32_t)1, fieldBitArray[0]);
    ASSERT_EQ((int32_t)0, fieldBitArray[5]);

}
IE_NAMESPACE_END(index);
