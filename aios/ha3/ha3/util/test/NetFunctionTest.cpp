#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/NetFunction.h>
#include <tr1/functional>
//#include <iostream>
//#include <iterator>

BEGIN_HA3_NAMESPACE(util);

class NetFunctionTest : public TESTBASE {
public:
    NetFunctionTest();
    ~NetFunctionTest();
public:
    void setUp();
    void tearDown();
protected:
    void testAChooseIP(const char ** p);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, NetFunctionTest);


using namespace std::tr1::placeholders;

NetFunctionTest::NetFunctionTest() {
}

NetFunctionTest::~NetFunctionTest() {
}

void NetFunctionTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void NetFunctionTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

void NetFunctionTest::testAChooseIP(const char ** p) {
    NetFunction::AllIPType ips;
    ASSERT_TRUE(p);
    const char * pivot = *p++;
    const char * cur = NULL;
    ASSERT_TRUE(pivot);
    while((cur = *p++)) {
        in_addr temp;
        ASSERT_TRUE(inet_aton(cur, &temp));
        ips.push_back(std::make_pair("",temp));
    }
    ASSERT_EQ(std::string(pivot), NetFunction::chooseIP(ips));
}

TEST_F(NetFunctionTest, testChooseIP) {
    HA3_LOG(DEBUG, "Begin Test!");

    const char * t1[] = {
        "127.0.0.1",
        "127.0.0.1",
        0
    };
    testAChooseIP(t1);

    const char * t2[] = {
        "192.168.4.4",
        "127.0.0.1",
        "192.168.4.4",
        0
    };
    testAChooseIP(t2);

    const char * t3[] = {
        "172.28.4.4",
        "127.0.0.1",
        "192.168.4.4",
        "172.28.4.4",
        0
    };
    testAChooseIP(t3);

    const char * t4[] = {
        "10.28.4.4",
        "127.0.0.1",
        "192.168.4.4",
        "172.28.4.4",
        "10.28.4.4",
        0
    };
    testAChooseIP(t4);

    const char * t5[] = {
        "201.28.4.4",
        "127.0.0.1",
        "192.168.4.4",
        "172.28.4.4",
        "10.28.4.4",
        "201.28.4.4",
        0
    };
    testAChooseIP(t5);

    // test broadcast
    const char * t6[] = {
        "10.28.4.4",
        "127.0.0.1",
        "192.168.4.4",
        "172.28.4.4",
        "10.28.4.4",
        "241.1.0.0",
        0
    };
    testAChooseIP(t6);

    const char * t7[] = {
        "172.32.1.3",
        "127.0.0.1",
        "192.168.4.4",
        "172.28.4.4",
        "10.28.4.4",
        "172.32.1.3",
        0
    };
    testAChooseIP(t7);

}

TEST_F(NetFunctionTest, testGetPrimaryIP) {
    HA3_LOG(DEBUG, "Begin Test!");

    std::string ip;
    ASSERT_TRUE(NetFunction::getPrimaryIP(ip));
}

TEST_F(NetFunctionTest, testContainIP) {
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_TRUE(NetFunction::containIP("x", ""));
    ASSERT_TRUE(!NetFunction::containIP("1.1.1", "1.1.1.1"));
    ASSERT_TRUE(NetFunction::containIP("1.1.1.1", "1.1.1.1"));
    ASSERT_TRUE(NetFunction::containIP("1.1.1.1x", "1.1.1.1"));
    ASSERT_TRUE(NetFunction::containIP("a1.1.1.1 ", "1.1.1.1"));
    ASSERT_TRUE(!NetFunction::containIP("1.1.1.12 ", "1.1.1.1"));
    ASSERT_TRUE(!NetFunction::containIP("11.1.1.1 ", "1.1.1.1"));

    ASSERT_TRUE(NetFunction::containIP("11.1.1.1 1.1.1.1", "1.1.1.1"));
    ASSERT_TRUE(NetFunction::containIP("11.1.1.1 1.1.1.12 1.1.1.1", "1.1.1.1"));
}

TEST_F(NetFunctionTest, testEncodeIP) {
    HA3_LOG(DEBUG, "Begin Test!");
    ASSERT_EQ((uint32_t)-1, NetFunction::encodeIp(std::string("255.255.255.255")));
    ASSERT_EQ((uint32_t)0, NetFunction::encodeIp(std::string("0.0.0.0")));
    ASSERT_EQ((uint32_t)0x0fff0fff, NetFunction::encodeIp(std::string("15.255.15.255")));
}

END_HA3_NAMESPACE(util);
