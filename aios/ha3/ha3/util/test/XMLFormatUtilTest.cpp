#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/XMLFormatUtil.h>
#include <sstream>
#include <autil/LongHashValue.h>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(util);

class XMLFormatUtilTest : public TESTBASE {
public:
    XMLFormatUtilTest();
    ~XMLFormatUtilTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, XMLFormatUtilTest);


XMLFormatUtilTest::XMLFormatUtilTest() { 
}

XMLFormatUtilTest::~XMLFormatUtilTest() { 
}

void XMLFormatUtilTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void XMLFormatUtilTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(XMLFormatUtilTest, testFormatCdataValue)
{
    HA3_LOG(DEBUG, "Begin Test!");

    stringstream ss;
    
    string orignalStr = "orgnal]]>string]]>";
    XMLFormatUtil::formatCdataValue(orignalStr, ss);
    HA3_LOG(TRACE3, "format string : \n%s", ss.str().c_str());

    string expectStr = "<![CDATA[orgnal]]&gt;\tstring]]&gt;]]>";
    ASSERT_EQ(expectStr, ss.str());

    stringstream ss1;
    orignalStr = "orgnal string";
    XMLFormatUtil::formatCdataValue(orignalStr, ss1);

    expectStr = "<![CDATA[orgnal string]]>";
    ASSERT_EQ(expectStr, ss1.str());

    stringstream ss2;
    orignalStr = "orgnal string]]";
    XMLFormatUtil::formatCdataValue(orignalStr, ss2);

    expectStr = "<![CDATA[orgnal string]]]]>";
    ASSERT_EQ(expectStr, ss2.str());

    stringstream ss3;
    orignalStr = "orgnal string]>";
    XMLFormatUtil::formatCdataValue(orignalStr, ss3);

    expectStr = "<![CDATA[orgnal string]>]]>";
    ASSERT_EQ(expectStr, ss3.str());

    stringstream ss4;
    orignalStr = "orgnalstring>";
    XMLFormatUtil::formatCdataValue(orignalStr, ss4);

    expectStr = "<![CDATA[orgnal\tstring>]]>";
    ASSERT_EQ(expectStr, ss4.str());

    stringstream ss5;
    orignalStr = "orgnal\r\n\tstring>";
    XMLFormatUtil::formatCdataValue(orignalStr, ss5);

    expectStr = "<![CDATA[orgnal\r\n\tstring>]]>";
    ASSERT_EQ(expectStr, ss5.str());

}

TEST_F(XMLFormatUtilTest, testToXMLStringSingle) {
#define TEST_SINGLE_VALUE_TO_XMLSTRING(t, v, e) { \
        t temp = v;                               \
        std::stringstream ss;                     \
        XMLFormatUtil::toXMLString(temp, ss);        \
        ASSERT_EQ(std::string(e), ss.str()); \
}
    TEST_SINGLE_VALUE_TO_XMLSTRING(int8_t, 1, "1");
    TEST_SINGLE_VALUE_TO_XMLSTRING(uint8_t, 2, "2");
    TEST_SINGLE_VALUE_TO_XMLSTRING(int16_t, 3, "3");
    TEST_SINGLE_VALUE_TO_XMLSTRING(uint16_t, 4, "4");
    TEST_SINGLE_VALUE_TO_XMLSTRING(int32_t, 5, "5");
    TEST_SINGLE_VALUE_TO_XMLSTRING(uint32_t, 6, "6");
    TEST_SINGLE_VALUE_TO_XMLSTRING(int64_t, 7, "7");
    TEST_SINGLE_VALUE_TO_XMLSTRING(uint64_t, 8, "8");
    TEST_SINGLE_VALUE_TO_XMLSTRING(float, 8.8, "8.8");
    TEST_SINGLE_VALUE_TO_XMLSTRING(double, 9.9, "9.9");
    TEST_SINGLE_VALUE_TO_XMLSTRING(bool, true, "1");
    TEST_SINGLE_VALUE_TO_XMLSTRING(bool, false, "0");
    TEST_SINGLE_VALUE_TO_XMLSTRING(std::string, "xyz", "<![CDATA[xyz]]>");
    uint128_t x128;
    x128.value[0] = 20;
    x128.value[1] = 30;
    std::stringstream ss;
    ss<<"<![CDATA[";
    ss<<x128;
    ss<<"]]>";

    TEST_SINGLE_VALUE_TO_XMLSTRING(uint128_t, x128, ss.str());
#undef TEST_SINGLE_VALUE_TO_XMLSTRING
}

TEST_F(XMLFormatUtilTest, testToXMLStringMulti) {

#define TEST_MULTI_VALUE_TO_XMLSTRING(t, v1, v2, v3, e) {        \
        std::vector<t> temp;                                      \
        temp.push_back(v1);                                          \
        temp.push_back(v2);                                          \
        temp.push_back(v3);                                          \
        std::stringstream ss;                                     \
        XMLFormatUtil::toXMLString(temp, ss);                     \
        ASSERT_EQ(std::string(e), ss.str());           \
}
    TEST_MULTI_VALUE_TO_XMLSTRING(int8_t, 1, 2, 3, "1 2 3");
    TEST_MULTI_VALUE_TO_XMLSTRING(uint8_t, 2, 3, 4, "2 3 4");
    TEST_MULTI_VALUE_TO_XMLSTRING(int16_t, 3, 4, 5, "3 4 5");
    TEST_MULTI_VALUE_TO_XMLSTRING(uint16_t, 4, 5, 6, "4 5 6");
    TEST_MULTI_VALUE_TO_XMLSTRING(int32_t, 5, 6, 7, "5 6 7");
    TEST_MULTI_VALUE_TO_XMLSTRING(uint32_t, 6, 7, 8, "6 7 8");
    TEST_MULTI_VALUE_TO_XMLSTRING(int64_t, 7, 8, 9, "7 8 9");
    TEST_MULTI_VALUE_TO_XMLSTRING(uint64_t, 8, 9, 10, "8 9 10");
    TEST_MULTI_VALUE_TO_XMLSTRING(float, 8.1, 9.1, 10.1, "8.1 9.1 10.1");
    TEST_MULTI_VALUE_TO_XMLSTRING(double, 9.1, 10.1, 11.1, "9.1 10.1 11.1");
    TEST_MULTI_VALUE_TO_XMLSTRING(bool, true, false, true, "1 0 1");
    TEST_MULTI_VALUE_TO_XMLSTRING(std::string, "xyz", "yzx", "zxy", "<![CDATA[xyz yzx zxy]]>");
    uint128_t x1281;
    x1281.value[0] = 20;
    x1281.value[1] = 30;
    uint128_t x1282;
    x1282.value[0] = 30;
    x1282.value[1] = 40;

    uint128_t x1283;
    x1283.value[0] = 40;
    x1283.value[1] = 50;
    
    std::stringstream ss;
    ss<<"<![CDATA[";
    ss<<x1281<<" "<<x1282<<" "<<x1283;
    ss<<"]]>";

    TEST_MULTI_VALUE_TO_XMLSTRING(uint128_t, x1281, x1282, x1283, ss.str());
#undef TEST_MULTI_VALUE_TO_XMLSTRING
}

END_HA3_NAMESPACE(util);

