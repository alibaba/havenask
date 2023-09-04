#include "iquan/common/IquanException.h"

#include <string>

#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class IquanExceptionTest : public TESTBASE {};

TEST_F(IquanExceptionTest, testIquanException) {
    IquanException exception("test-exception");
    ASSERT_EQ("test-exception", std::string(exception.what()));
}

} // namespace iquan
