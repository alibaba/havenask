#include <autil/StringUtil.h>
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/tc_delegation.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(util);

class TcDelegationTest : public INDEXLIB_TESTBASE
{
public:
    TcDelegationTest();
    ~TcDelegationTest();

    DECLARE_CLASS_NAME(TcDelegationTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
};

INDEXLIB_UNIT_TEST_CASE(TcDelegationTest, TestSimpleProcess);

TcDelegationTest::TcDelegationTest()
{
}

TcDelegationTest::~TcDelegationTest()
{
}

void TcDelegationTest::CaseSetUp()
{
}

void TcDelegationTest::CaseTearDown()
{
}

void TcDelegationTest::TestSimpleProcess() {
    auto inst = TcDelegation::Inst();
    ASSERT_TRUE(inst);
}

IE_NAMESPACE_END(util);

