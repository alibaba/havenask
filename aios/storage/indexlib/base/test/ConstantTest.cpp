#include "indexlib/base/Constant.h"

#include "autil/MultiValueType.h"
#include "unittest/unittest.h"

namespace indexlib {

class ConstantTest : public TESTBASE
{
public:
    ConstantTest() = default;
    ~ConstantTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void ConstantTest::setUp() {}

void ConstantTest::tearDown() {}

TEST_F(ConstantTest, testSimple)
{
    static_assert(autil::MULTI_VALUE_DELIMITER == MULTI_VALUE_SEPARATOR,
                  "MULTI_VALUE_SEPARATOR should equal autil::MULTI_VALUE_DELIMITER");
    ASSERT_EQ(autil::MULTI_VALUE_DELIMITER, MULTI_VALUE_SEPARATOR);
}

} // namespace indexlib
