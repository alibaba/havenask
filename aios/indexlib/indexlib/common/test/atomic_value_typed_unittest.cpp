#include "indexlib/common/test/atomic_value_typed_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, AtomicValueTypedTest);

AtomicValueTypedTest::AtomicValueTypedTest()
{
}

AtomicValueTypedTest::~AtomicValueTypedTest()
{
}

void AtomicValueTypedTest::CaseSetUp()
{
}

void AtomicValueTypedTest::CaseTearDown()
{
}

void AtomicValueTypedTest::TestSimpleProcess()
{
    AtomicValuePtr atomicValue(new AtomicValueTyped<uint8_t>);
    INDEXLIB_TEST_EQUAL(sizeof(uint8_t), atomicValue->GetSize());
}

IE_NAMESPACE_END(common);

