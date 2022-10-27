#ifndef __INDEXLIB_NETUTILTEST_H
#define __INDEXLIB_NETUTILTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/net_util.h"

IE_NAMESPACE_BEGIN(util);

class NetUtilTest : public INDEXLIB_TESTBASE
{
public:
    NetUtilTest();
    ~NetUtilTest();

    DECLARE_CLASS_NAME(NetUtilTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    void CheckIp(const std::string& ip);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(NetUtilTest, TestSimpleProcess);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_NETUTILTEST_H
