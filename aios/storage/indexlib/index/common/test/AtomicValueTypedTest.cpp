
#include "indexlib/index/common/AtomicValueTyped.h"

#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {

class AtomicValueTypedTest : public INDEXLIB_TESTBASE
{
public:
    AtomicValueTypedTest();
    ~AtomicValueTypedTest();

    DECLARE_CLASS_NAME(AtomicValueTypedTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
};

INDEXLIB_UNIT_TEST_CASE(AtomicValueTypedTest, TestSimpleProcess);

AtomicValueTypedTest::AtomicValueTypedTest() {}

AtomicValueTypedTest::~AtomicValueTypedTest() {}

void AtomicValueTypedTest::CaseSetUp() {}

void AtomicValueTypedTest::CaseTearDown() {}

void AtomicValueTypedTest::TestSimpleProcess()
{
    std::shared_ptr<AtomicValue> atomicValue(new AtomicValueTyped<uint8_t>);
    ASSERT_EQ(sizeof(uint8_t), atomicValue->GetSize());
}
} // namespace indexlib::index
