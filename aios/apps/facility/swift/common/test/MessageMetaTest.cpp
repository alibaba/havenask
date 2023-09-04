#include "swift/common/MessageMeta.h"

#include <iosfwd>
#include <stdint.h>

#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace common {

class MessageMetaTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void MessageMetaTest::setUp() {}

void MessageMetaTest::tearDown() {}

struct MergedMessageMetaOld {
    MergedMessageMetaOld() : endPos(0), isCompress(false), maskPayload(0), payload(0) {}
    uint64_t endPos      : 30;
    bool isCompress      : 1;
    bool                 : 1;
    uint64_t maskPayload : 8;
    uint64_t payload     : 16;
};

// test compatible
TEST_F(MessageMetaTest, testMergedMessageMeta) {
    ASSERT_EQ(8, sizeof(MergedMessageMetaOld));
    ASSERT_EQ(8, sizeof(MergedMessageMeta));

    {
        MergedMessageMetaOld old;
        MergedMessageMeta *newMeta = (MergedMessageMeta *)&old;
        ASSERT_EQ(old.endPos, newMeta->endPos);
        ASSERT_EQ(old.isCompress, newMeta->isCompress);
        ASSERT_EQ(old.maskPayload, newMeta->maskPayload);
        ASSERT_EQ(old.payload, newMeta->payload);
    }
    {
        MergedMessageMetaOld old;
        old.endPos = (1 << 30) - 1;
        old.isCompress = true;
        old.maskPayload = 255;
        old.payload = 65535;

        MergedMessageMeta *newMeta = (MergedMessageMeta *)&old;
        ASSERT_EQ(old.endPos, newMeta->endPos);
        ASSERT_EQ(old.isCompress, newMeta->isCompress);
        ASSERT_EQ(old.maskPayload, newMeta->maskPayload);
        ASSERT_EQ(old.payload, newMeta->payload);
    }
    {
        MergedMessageMetaOld old;
        old.endPos = 33256;
        old.isCompress = false;
        old.maskPayload = 102;
        old.payload = 655;

        MergedMessageMeta *newMeta = (MergedMessageMeta *)&old;
        ASSERT_EQ(old.endPos, newMeta->endPos);
        ASSERT_EQ(old.isCompress, newMeta->isCompress);
        ASSERT_EQ(old.maskPayload, newMeta->maskPayload);
        ASSERT_EQ(old.payload, newMeta->payload);
    }
    {
        MergedMessageMeta newMeta;
        MergedMessageMetaOld *old = (MergedMessageMetaOld *)&newMeta;
        ASSERT_EQ(newMeta.endPos, old->endPos);
        ASSERT_EQ(newMeta.isCompress, old->isCompress);
        ASSERT_EQ(newMeta.maskPayload, old->maskPayload);
        ASSERT_EQ(newMeta.payload, old->payload);
    }
    {
        MergedMessageMeta newMeta;
        newMeta.endPos = (1 << 30) - 1;
        newMeta.isCompress = true;
        newMeta.maskPayload = 255;
        newMeta.payload = 65535;

        MergedMessageMetaOld *old = (MergedMessageMetaOld *)&newMeta;
        ASSERT_EQ(newMeta.endPos, old->endPos);
        ASSERT_EQ(newMeta.isCompress, old->isCompress);
        ASSERT_EQ(newMeta.maskPayload, old->maskPayload);
        ASSERT_EQ(newMeta.payload, old->payload);
    }
    {
        MergedMessageMeta newMeta;
        newMeta.endPos = 33256;
        newMeta.isCompress = false;
        newMeta.maskPayload = 102;
        newMeta.payload = 655;

        MergedMessageMetaOld *old = (MergedMessageMetaOld *)&newMeta;
        ASSERT_EQ(newMeta.endPos, old->endPos);
        ASSERT_EQ(newMeta.isCompress, old->isCompress);
        ASSERT_EQ(newMeta.maskPayload, old->maskPayload);
        ASSERT_EQ(newMeta.payload, old->payload);
    }
}

}; // namespace common
}; // namespace swift
