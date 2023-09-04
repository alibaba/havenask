#include "iquan/jni/test/testlib/IquanTestBase.h"

#include <mutex>

#include "autil/EnvUtil.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/jni/IquanEnv.h"

namespace iquan {

AUTIL_LOG_SETUP(iquan, IquanTestBase);

void IquanTestBase::SetUpTestCase() {
    autil::EnvUtil::setEnv(BINARY_PATH, ".");
    Status status = IquanEnv::jvmSetup(jt_hdfs_jvm, {}, "");
    autil::EnvUtil::setEnv(BINARY_PATH, "");
    ASSERT_TRUE(status.ok()) << status.errorMessage();
}

void IquanTestBase::TearDownTestCase() {}

void IquanTestBase::setUp() {}

void IquanTestBase::tearDown() {}

} // namespace iquan
