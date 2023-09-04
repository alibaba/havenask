#pragma once

#include "autil/Log.h"
#include "iquan/common/Common.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class IquanTestBase : public TESTBASE {
protected:
    static void SetUpTestCase();
    static void TearDownTestCase();

    virtual void setUp() override;
    virtual void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace iquan
