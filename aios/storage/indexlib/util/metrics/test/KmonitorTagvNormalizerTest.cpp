#include "indexlib/util/metrics/KmonitorTagvNormalizer.h"

#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace util {

class KmonitorTagvNormalizerTest : public INDEXLIB_TESTBASE
{
public:
    KmonitorTagvNormalizerTest();
    ~KmonitorTagvNormalizerTest();

    DECLARE_CLASS_NAME(KmonitorTagvNormalizerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
};

INDEXLIB_UNIT_TEST_CASE(KmonitorTagvNormalizerTest, TestSimpleProcess);
KmonitorTagvNormalizerTest::KmonitorTagvNormalizerTest() {}

KmonitorTagvNormalizerTest::~KmonitorTagvNormalizerTest() {}

void KmonitorTagvNormalizerTest::CaseSetUp() {}

void KmonitorTagvNormalizerTest::CaseTearDown() {}

void KmonitorTagvNormalizerTest::TestSimpleProcess()
{
    string str = "adtAsd10234_21234_1.234";
    string expect = "adtAsd10234_21234_1.234";
    ASSERT_EQ(expect, KmonitorTagvNormalizer::GetInstance()->Normalize(str));

    str = "^asd@1234]";
    expect = "_asd_1234_";
    ASSERT_EQ(expect, KmonitorTagvNormalizer::GetInstance()->Normalize(str));

    str = "asd@1234_";
    expect = "asd-1234_";
    ASSERT_EQ(expect, KmonitorTagvNormalizer::GetInstance()->Normalize(str, '-'));
}

}} // namespace indexlib::util
