#include "swift/util/PermissionCenter.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <stdint.h>
#include <unistd.h>
#include <vector>

#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "swift/util/Permission.h"
#include "swift/util/TimeoutChecker.h"
#include "unittest/unittest.h"

using namespace std;

namespace swift {
namespace util {

class PermissionCenterTest : public TESTBASE {
private:
    void applySameKey();

private:
    bool _hasApplyed;
    PermissionCenter *_permissionCenter;
};

TEST_F(PermissionCenterTest, testSimpleProcess) {
    PermissionCenter permissionCenter(3, 3);
    PermissionKey key1("key1");
    int64_t currentTime = autil::TimeUtility::currentTime();
    TimeoutChecker timeoutChecker(currentTime);
    PermissionPtr permission = permissionCenter.apply(key1, &timeoutChecker);
    EXPECT_TRUE(permission);
    EXPECT_TRUE(!permissionCenter.apply(key1, &timeoutChecker));
    permission->release();
    EXPECT_TRUE(permissionCenter.apply(key1, &timeoutChecker));
}

TEST_F(PermissionCenterTest, testApplyWithLimit) {
    PermissionCenter permissionCenter(2, 1);
    PermissionKey key1("key1");
    PermissionKey key2("key2");
    PermissionKey key3("key3");
    int64_t currentTime = autil::TimeUtility::currentTime();
    TimeoutChecker timeoutChecker(currentTime);
    PermissionPtr permission1 = permissionCenter.apply(key1, &timeoutChecker);
    EXPECT_TRUE(permission1);
    PermissionPtr permission2 = permissionCenter.apply(key2, &timeoutChecker);
    EXPECT_TRUE(permission2);
    PermissionPtr permission3 = permissionCenter.apply(key3, &timeoutChecker);
    EXPECT_TRUE(!permission3);
    permission2->release();
    currentTime = autil::TimeUtility::currentTime();
    TimeoutChecker timeoutChecker2(currentTime);
    permission3 = permissionCenter.apply(key3, &timeoutChecker2);
    EXPECT_TRUE(permission3);
}

TEST_F(PermissionCenterTest, testApplyWithMaxWaitTime) {
    PermissionCenter permissionCenter(2, (int64_t)5 * 1000 * 1000);
    PermissionKey key1("key1");
    int64_t currentTime = autil::TimeUtility::currentTime();
    TimeoutChecker timeoutChecker(currentTime);
    timeoutChecker.setExpireTimeout((int64_t)2 * 1000 * 1000);
    PermissionPtr permission1 = permissionCenter.apply(key1, &timeoutChecker);
    EXPECT_TRUE(permission1);
    PermissionPtr permission2 = permissionCenter.apply(key1, &timeoutChecker);
    EXPECT_TRUE(timeoutChecker.isTimeout());
    EXPECT_TRUE(!permission2);

    timeoutChecker.setExpireTimeout((int64_t)20 * 1000 * 1000);
    permission2 = permissionCenter.apply(key1, &timeoutChecker);
    EXPECT_TRUE(!permission2);
    EXPECT_TRUE(!timeoutChecker.isTimeout());

    permission1.reset();
    permission2 = permissionCenter.apply(key1, &timeoutChecker);
    EXPECT_TRUE(!permission2);
}

void PermissionCenterTest::applySameKey() {
    PermissionKey key1("key1");
    while (true) {
        PermissionPtr permission = _permissionCenter->apply(key1, NULL);
        if (permission) {
            EXPECT_TRUE(!_hasApplyed);
            _hasApplyed = true;
            usleep(10 * 1000);
            _hasApplyed = false;
            permission->release();
            break;
        }
    }
}

TEST_F(PermissionCenterTest, testApplyWithMultiThread) {
    _hasApplyed = false;
    _permissionCenter = new PermissionCenter(5, (int64_t)200 * 1000 * 1000);
    vector<autil::ThreadPtr> threads;
    uint32_t threadNum = 100;
    for (uint32_t i = 0; i < threadNum; ++i) {
        autil::ThreadPtr thread = autil::Thread::createThread(std::bind(&PermissionCenterTest::applySameKey, this));
        threads.push_back(thread);
    }
    for (uint32_t i = 0; i < threadNum; ++i) {
        threads[i].reset();
    }
    delete _permissionCenter;
}

TEST_F(PermissionCenterTest, testReadWriteThreadPermission) {
    _permissionCenter = new PermissionCenter(2, 10 * 000, 2, 2);
    EXPECT_TRUE(_permissionCenter->incReadCount());
    EXPECT_TRUE(_permissionCenter->incReadCount());
    EXPECT_TRUE(!_permissionCenter->incReadCount());
    _permissionCenter->decReadCount();
    EXPECT_TRUE(_permissionCenter->incGetMaxIdCount());
    EXPECT_TRUE(!_permissionCenter->incGetMaxIdCount());
    _permissionCenter->decReadCount();
    EXPECT_TRUE(_permissionCenter->incGetMaxIdCount());
    EXPECT_TRUE(!_permissionCenter->incGetMaxIdCount());
    _permissionCenter->decGetMaxIdCount();
    EXPECT_TRUE(_permissionCenter->incGetMinIdCount());
    EXPECT_TRUE(!_permissionCenter->incGetMinIdCount());
    _permissionCenter->decGetMaxIdCount();
    EXPECT_TRUE(_permissionCenter->incGetMinIdCount());
    EXPECT_TRUE(!_permissionCenter->incGetMinIdCount());
    EXPECT_TRUE(!_permissionCenter->incGetMaxIdCount());
    EXPECT_TRUE(!_permissionCenter->incReadCount());

    EXPECT_TRUE(_permissionCenter->incWriteCount());
    EXPECT_TRUE(_permissionCenter->incWriteCount());
    EXPECT_TRUE(!_permissionCenter->incWriteCount());
    _permissionCenter->decWriteCount();
    EXPECT_TRUE(_permissionCenter->incWriteCount());
    EXPECT_TRUE(!_permissionCenter->incWriteCount());

    EXPECT_TRUE(_permissionCenter->incReadFileCount());
    EXPECT_TRUE(_permissionCenter->incReadFileCount());
    EXPECT_TRUE(!_permissionCenter->incReadFileCount());
    _permissionCenter->decReadFileCount();
    EXPECT_TRUE(_permissionCenter->incReadFileCount());
    EXPECT_TRUE(!_permissionCenter->incReadFileCount());

    delete _permissionCenter;
}

TEST_F(PermissionCenterTest, testScopedPermission) {
    _permissionCenter = new PermissionCenter(2, 10 * 000, 2, 2);
    EXPECT_TRUE(_permissionCenter->incReadCount());
    {
        ScopedReadPermission readPermission(_permissionCenter);
        EXPECT_EQ(uint32_t(1), _permissionCenter->getCurReadCount());
    }
    EXPECT_EQ(uint32_t(0), _permissionCenter->getCurReadCount());

    EXPECT_TRUE(_permissionCenter->incWriteCount());
    {
        ScopedWritePermission writePermission(_permissionCenter);
        EXPECT_EQ(uint32_t(1), _permissionCenter->getCurWriteCount());
    }
    EXPECT_EQ(uint32_t(0), _permissionCenter->getCurWriteCount());

    EXPECT_TRUE(_permissionCenter->incReadFileCount());
    {
        ScopedReadFilePermission readFilePermission(_permissionCenter);
        EXPECT_EQ(uint32_t(1), _permissionCenter->getCurReadFileCount());
    }
    EXPECT_EQ(uint32_t(0), _permissionCenter->getCurReadFileCount());

    EXPECT_TRUE(_permissionCenter->incGetMaxIdCount());
    {
        ScopedMaxIdPermission maxIdPermission(_permissionCenter);
        EXPECT_EQ(uint32_t(1), _permissionCenter->getCurMaxIdCount());
    }
    EXPECT_EQ(uint32_t(0), _permissionCenter->getCurMaxIdCount());

    EXPECT_TRUE(_permissionCenter->incGetMinIdCount());
    {
        ScopedMinIdPermission minIdPermission(_permissionCenter);
        EXPECT_EQ(uint32_t(1), _permissionCenter->getCurMinIdCount());
    }
    EXPECT_EQ(uint32_t(0), _permissionCenter->getCurMinIdCount());

    delete _permissionCenter;
}

} // namespace util
} // namespace swift
