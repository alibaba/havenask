#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;

namespace indexlib { namespace util {

class AA
{
public:
    AA(int a, int b) {}
    ~AA() {}
};

template <typename T>
class B : public AA
{
public:
    B(int a, int b) : AA(a, b) {}
};

class ClassTypedFactoryTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ClassTypedFactoryTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForSimple()
    {
        AA* a = ClassTypedFactory<AA, B, int, int>::GetInstance()->Create(ft_int32, 0, 1);
        delete a;
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, ClassTypedFactoryTest);

INDEXLIB_UNIT_TEST_CASE(ClassTypedFactoryTest, TestCaseForSimple);
}} // namespace indexlib::util
