#include "swift/util/ScopedAtomic.h"

#include <stdint.h>

#include "swift/util/Atomic.h"
#include "unittest/unittest.h"

namespace swift {
namespace util {

class ScopedAtomicTest : public TESTBASE {};

TEST_F(ScopedAtomicTest, testSimpleProcess) {
    Atomic32 atomic32Count;
    EXPECT_EQ((int32_t)0, atomic32Count.get());
    ScopedAtomic<int32_t> *atomicWrapper = new ScopedAtomic<int32_t>(&atomic32Count);
    EXPECT_EQ((int32_t)1, atomic32Count.get());
    delete atomicWrapper;
    EXPECT_EQ((int32_t)0, atomic32Count.get());
    Atomic64 atomic64Count;
    EXPECT_EQ((int64_t)0, atomic64Count.get());
    ScopedAtomic<int64_t> *atomicWrapper2 = new ScopedAtomic<int64_t>(&atomic64Count);
    EXPECT_EQ((int64_t)1, atomic64Count.get());
    delete atomicWrapper2;
    EXPECT_EQ((int64_t)0, atomic64Count.get());
}

} // namespace util
} // namespace swift
