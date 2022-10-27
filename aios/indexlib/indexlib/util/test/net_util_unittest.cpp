#include <autil/StringUtil.h>
#include "indexlib/util/test/net_util_unittest.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, NetUtilTest);

NetUtilTest::NetUtilTest()
{
}

NetUtilTest::~NetUtilTest()
{
}

void NetUtilTest::CaseSetUp()
{
}

void NetUtilTest::CaseTearDown()
{
}

void NetUtilTest::TestSimpleProcess()
{
    vector<string> ips;
    ASSERT_TRUE(NetUtil::GetIp(ips));
    ASSERT_TRUE(ips.size() >= 1);
    for (const auto& ip : ips) {
        // cout << ip << endl;
        CheckIp(ip);
    }

    string ip;
    ASSERT_TRUE(NetUtil::GetDefaultIp(ip));
    CheckIp(ip);
    // cout << ip << endl;

    string hostname;
    ASSERT_TRUE(NetUtil::GetHostName(hostname));
    ASSERT_FALSE(hostname.empty());
    // cout << hostname << endl;
}

void NetUtilTest::CheckIp(const std::string& ip)
{
    vector<string> substrs;
    StringUtil::split(substrs, ip, ".", false);
    ASSERT_EQ(4, substrs.size()) << "Invalid ip [" << ip << "]";
    int32_t tmp;
    for (const auto& substr : substrs) {
        ASSERT_TRUE(StringUtil::fromString(substr, tmp) && tmp >= 0 && tmp < 256);
    }
}

IE_NAMESPACE_END(util);

