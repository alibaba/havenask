#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/IPListParser.h>

using namespace std;

BEGIN_HA3_NAMESPACE(util);

class IPListParserTest : public TESTBASE {
public:
    IPListParserTest();
    ~IPListParserTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, IPListParserTest);


IPListParserTest::IPListParserTest() { 
}

IPListParserTest::~IPListParserTest() { 
}

void IPListParserTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void IPListParserTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(IPListParserTest, testParseIPList) { 
    HA3_LOG(DEBUG, "Begin Test!");

    string ipListStr = ";;10.250.8.[123-130]; "
                       "10.250.12.26;"
                       "-10.250.8.126;"
                       "10.250.8.126;"
                       "10.250.8.125;"
                       "10.250.12.33";
    set<string> ipSet;
    map<string, set<string> > typedIpList;
    ASSERT_TRUE(IPListParser::parseIPList(ipListStr, ipSet, typedIpList));
    vector<string> ipList(ipSet.begin(), ipSet.end());
    ASSERT_EQ((size_t)9, ipList.size());
    ASSERT_EQ((size_t)0, typedIpList.size());
    ASSERT_EQ(string("10.250.12.26"), ipList[0]);
    ASSERT_EQ(string("10.250.12.33"), ipList[1]);
    ASSERT_EQ(string("10.250.8.123"), ipList[2]);
    ASSERT_EQ(string("10.250.8.124"), ipList[3]);
    ASSERT_EQ(string("10.250.8.125"), ipList[4]);
    ASSERT_EQ(string("10.250.8.127"), ipList[5]);
    ASSERT_EQ(string("10.250.8.128"), ipList[6]);
    ASSERT_EQ(string("10.250.8.129"), ipList[7]);
    ASSERT_EQ(string("10.250.8.130"), ipList[8]);
}

TEST_F(IPListParserTest, testParseIPListFail) {
    HA3_LOG(DEBUG, "Begin Test!");
#define TEST_PARSE_FAIL(ipListStr)                                      \
    {                                                                   \
        set<string> ipList;                                             \
        map<string, set<string> > typedIpList;                          \
        ASSERT_TRUE(!IPListParser::parseIPList(ipListStr, ipList, typedIpList)); \
    }

    TEST_PARSE_FAIL("10.555.12.55");
    TEST_PARSE_FAIL("10.250.12.[55]");
    TEST_PARSE_FAIL("aaax.250.12.55");
    TEST_PARSE_FAIL("10.250.12.[44-55");
    TEST_PARSE_FAIL("10.-250.12.[44-55]");
    TEST_PARSE_FAIL("10.250.12.[55-44]");
    TEST_PARSE_FAIL("10.250.12");
    TEST_PARSE_FAIL("10.250.12.2 3");
    TEST_PARSE_FAIL("10.250.12.2333");
    TEST_PARSE_FAIL("10.250.12.[55-aa]");
}

TEST_F(IPListParserTest, testParseIPListWithType) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    string ipListStr = "type1{10.250.12.33;;10.250.12.[30-35];-10.250.12.34}|"
                       "10.250.12.11|10.250.12.40|"
                       "type2{10.250.12.22}";
    set<string> ipSet;
    map<string, set<string> > typedIpList;
    ASSERT_TRUE(IPListParser::parseIPList(ipListStr, ipSet, typedIpList));
    
    vector<string> ipList(ipSet.begin(), ipSet.end());
    ASSERT_EQ((size_t)8, ipList.size());
    ASSERT_EQ(string("10.250.12.11"), ipList[0]);
    ASSERT_EQ(string("10.250.12.22"), ipList[1]);
    ASSERT_EQ(string("10.250.12.30"), ipList[2]);
    ASSERT_EQ(string("10.250.12.31"), ipList[3]);
    ASSERT_EQ(string("10.250.12.32"), ipList[4]);
    ASSERT_EQ(string("10.250.12.33"), ipList[5]);
    ASSERT_EQ(string("10.250.12.35"), ipList[6]);
    ASSERT_EQ(string("10.250.12.40"), ipList[7]);

    ASSERT_EQ((size_t)2, typedIpList.size());
    ASSERT_TRUE(typedIpList.find("type1") != typedIpList.end());
    vector<string> ipList2(typedIpList["type1"].begin(),
                           typedIpList["type1"].end());
    ASSERT_EQ((size_t)5, ipList2.size());
    ASSERT_EQ(string("10.250.12.30"), ipList2[0]);
    ASSERT_EQ(string("10.250.12.31"), ipList2[1]);
    ASSERT_EQ(string("10.250.12.32"), ipList2[2]);
    ASSERT_EQ(string("10.250.12.33"), ipList2[3]);
    ASSERT_EQ(string("10.250.12.35"), ipList2[4]);

    ASSERT_TRUE(typedIpList.find("type2") != typedIpList.end());
    vector<string> ipList3(typedIpList["type2"].begin(),
                           typedIpList["type2"].end());
    ASSERT_EQ((size_t)1, ipList3.size());
    ASSERT_EQ(string("10.250.12.22"), ipList3[0]);    
}

TEST_F(IPListParserTest, testParseIPListFailWithType) {
    HA3_LOG(DEBUG, "Begin Test!");
#define TEST_PARSE_FAIL(ipListStr)                                      \
    {                                                                   \
        set<string> ipList;                                             \
        map<string, set<string> > typedIpList;                          \
        ASSERT_TRUE(!IPListParser::parseIPList(ipListStr, ipList, typedIpList)); \
    }

    TEST_PARSE_FAIL("xx{,{10.555.12.55}");
    TEST_PARSE_FAIL("xx{10.555.12.55");
    TEST_PARSE_FAIL("xx{10.250.12.22}xx");
}


END_HA3_NAMESPACE(util);

