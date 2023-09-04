#include "swift/broker/storage/FlowControl.h"

#include <iosfwd>

#include "swift/util/Atomic.h"
#include "unittest/unittest.h"

using namespace std;
namespace swift {
namespace storage {

class FlowControlTest : public TESTBASE {
    void setUp() {}
    void tearDown() {}
};

TEST_F(FlowControlTest, testSimple) {
    FlowControl fc(100, 200);

    EXPECT_EQ(100, fc._writeSizeLimit);
    EXPECT_EQ(200, fc._readSizeLimit);
    EXPECT_EQ(0, fc._lastWriteTime.get());
    EXPECT_EQ(0, fc._lastReadTime.get());
    EXPECT_EQ(0, fc._writeSize.get());
    EXPECT_EQ(0, fc._readSize.get());

    fc.setMaxReadSize(10);
    EXPECT_EQ(10, fc._readSizeLimit);
    fc.setMaxReadSize(200);

    // test read
    EXPECT_TRUE(fc.canRead(100 * 1000));
    fc.updateReadSize(80);
    EXPECT_TRUE(fc.canRead(600 * 1000));
    fc.updateReadSize(70);
    EXPECT_TRUE(fc.canRead(900 * 1000));
    fc.updateReadSize(50);
    EXPECT_FALSE(fc.canRead(999 * 1000));
    EXPECT_FALSE(fc.canRead(1000 * 1000));
    EXPECT_EQ(200, fc._readSize.get());
    EXPECT_TRUE(fc.canRead(1001 * 1000));
    EXPECT_EQ(1001 * 1000, fc._lastReadTime.get());
    EXPECT_EQ(0, fc._readSize.get());

    // test write
    EXPECT_TRUE(fc.canWrite(100 * 1000));
    fc.updateWriteSize(10);
    EXPECT_TRUE(fc.canWrite(600 * 1000));
    fc.updateWriteSize(60);
    EXPECT_TRUE(fc.canWrite(900 * 1000));
    fc.updateWriteSize(30);
    EXPECT_FALSE(fc.canWrite(999 * 1000));
    EXPECT_FALSE(fc.canWrite(1000 * 1000));
    EXPECT_EQ(100, fc._writeSize.get());
    EXPECT_TRUE(fc.canWrite(1001 * 1000));
    EXPECT_EQ(1001 * 1000, fc._lastWriteTime.get());
    EXPECT_EQ(0, fc._readSize.get());
}

} // namespace storage
} // namespace swift
