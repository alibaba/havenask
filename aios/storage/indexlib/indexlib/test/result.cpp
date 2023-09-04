#include "indexlib/test/result.h"

using namespace std;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, Result);

Result::Result() : mIsSubResult(false) {}

Result::~Result() {}
}} // namespace indexlib::test
