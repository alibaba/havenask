#include "swift/util/IpMapper.h"

#include <iosfwd>
#include <string>
#include <unordered_map>

#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace util {

class IpMapperTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void IpMapperTest::setUp() {}

void IpMapperTest::tearDown() {}

TEST_F(IpMapperTest, testSimple) {
    unordered_map<string, string> ipMap = {{"11.12.13", "EA110"}, {"10.23.23", "EA120"}};
    IpMapper ipMapper(ipMap);
    ASSERT_EQ("EA110", ipMapper.mapIp("11.12.13.20"));
    ASSERT_EQ("EA110", ipMapper.mapIp("11.12.13.20:1000"));
    ASSERT_EQ("EA120", ipMapper.mapIp("10.23.23.20:1000"));
    ASSERT_EQ("unknown", ipMapper.mapIp("11.123.23.20"));
    ASSERT_EQ("unknown", ipMapper.mapIp(""));
    ASSERT_EQ("unknown", ipMapper.mapIp("."));
}

}; // namespace util
}; // namespace swift
