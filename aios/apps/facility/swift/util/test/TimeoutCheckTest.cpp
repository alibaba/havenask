#include <iostream>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>

#include "autil/TimeUtility.h"
#include "swift/util/TimeoutChecker.h"
#include "unittest/unittest.h"

namespace swift {
namespace util {

using namespace std;

class TimeoutCheckerTest : public TESTBASE {};

TEST_F(TimeoutCheckerTest, testCheckTimeout) {
    int64_t beginTime = autil::TimeUtility::currentTime();
    TimeoutChecker timeoutCheck(beginTime);
    EXPECT_FALSE(timeoutCheck.isTimeout());
    EXPECT_FALSE(timeoutCheck.checkTimeout());
    EXPECT_FALSE(timeoutCheck.isTimeout());
    usleep(TimeoutChecker::DEFALUT_EXPIRE_TIMEOUT + 100);
    EXPECT_FALSE(timeoutCheck.isTimeout());
    EXPECT_TRUE(timeoutCheck.checkTimeout());
    EXPECT_TRUE(timeoutCheck.isTimeout());
}

TEST_F(TimeoutCheckerTest, testPoniter) {
    string test = "test string";
    string *p = &test;
    std::stringstream ss;
    ss << uint64_t(p);
    string pstr = ss.str();

    string *pt;
    // pt = (string*)atol(pstr.c_str());
    pt = reinterpret_cast<string *>(atol(pstr.c_str()));

    cout << "\n pointer: " << pstr << endl
         << " raw: " << p << endl
         << " content: " << *p << endl
         << " pt: " << pt << endl
         << " decode: " << *pt << endl
         << endl;
}

} // namespace util
} // namespace swift
