#include "swift/common/FileMessageMeta.h"

#include <iostream>
#include <stdint.h>

#include "unittest/unittest.h"

using namespace std;
namespace swift {
namespace common {

class FileMessageMetaTest : public TESTBASE {
protected:
    void compare(int64_t time,
                 uint64_t end,
                 uint16_t p,
                 uint8_t maskP,
                 bool compress = false,
                 int64_t msgLen = 0,
                 bool merged = false);
};

TEST_F(FileMessageMetaTest, testToBufAndFromBuf) {
    ASSERT_EQ(16, sizeof(FileMessageMeta));
    compare(1, 2, 3, 4);
    compare(1, 2, 0, 10);
    compare(1234567890, 987654321, 0, 20);
    compare(0x7567890abcdef, 987654321, 0, 30);
    compare(0x7dcba09876543, 987654321, 0, 40);
    compare(1, 2, 65535, 50);
    compare(1234567890, 987654321, 65535, 60);
    compare(0x734567890abcd, 987654321, 65535, 80);
    compare(0xfdcba09876543, 987654321, 65535, 155);
    compare(0xfdcba09876543, 987654321, 65535, 255, true, 0);
    compare(0xffcba09876543, 987654321, 65535, 255, false, 130);
    compare(0x7dcba09876543, 987654321, 0, 0, true, 255);
    compare(0x7dcba09876543, 0, 65535, 0, true, 900);
    compare(0x7dcba09876543, (int64_t(1) << 38) - 1, 65535, 255, true, 800);
    compare(0x7ffffffffffff, (int64_t(1) << 38) - 1, 65535, 255, false, 0);
    compare(0x7ffffffffffff, (int64_t(1) << 38) - 1, 65535, 255, false, true);
}

void FileMessageMetaTest::compare(
    int64_t time, uint64_t end, uint16_t p, uint8_t maskP, bool compress, int64_t msgLen, bool merged) {

    ASSERT_EQ(16, FILE_MESSAGE_META_SIZE);
    FileMessageMeta fmm;
    fmm.timestamp = time;
    fmm.endPos = end;
    fmm.payload = p;
    fmm.maskPayload = maskP;
    fmm.isCompress = compress;
    fmm.isMerged = merged;
    fmm.msgLenHighPart = msgLen;
    char buf[FILE_MESSAGE_META_SIZE] = {0};
    fmm.toBuf(buf);
    char bufOld[FILE_MESSAGE_META_SIZE] = {0};
    fmm.toBufOld(bufOld);

    FileMessageMeta fmm2;
    fmm2.fromBuf(buf);
    EXPECT_EQ(time, fmm2.timestamp);
    EXPECT_EQ(end, (uint64_t)fmm2.endPos);
    EXPECT_EQ(uint16_t(p), (uint16_t)fmm2.payload);
    EXPECT_EQ(maskP, (uint8_t)fmm2.maskPayload);
    EXPECT_EQ(compress, fmm2.isCompress);
    EXPECT_EQ(merged, fmm2.isMerged);
    EXPECT_EQ(msgLen, fmm2.msgLenHighPart);

    FileMessageMeta fmm3;
    char *buf2 = (char *)(&fmm);
    fmm3.fromBuf(buf2);
    EXPECT_EQ(time, fmm3.timestamp);
    EXPECT_EQ(end, (uint64_t)fmm3.endPos);
    EXPECT_EQ(uint16_t(p), (uint16_t)fmm3.payload);
    EXPECT_EQ(maskP, (uint8_t)fmm3.maskPayload);
    EXPECT_EQ(compress, fmm3.isCompress);
    EXPECT_EQ(merged, fmm3.isMerged);
    EXPECT_EQ(msgLen, fmm3.msgLenHighPart);

    FileMessageMeta *fmm4 = (FileMessageMeta *)(buf);
    EXPECT_EQ(time, fmm4->timestamp);
    EXPECT_EQ(end, (uint64_t)fmm4->endPos);
    EXPECT_EQ(uint16_t(p), (uint16_t)fmm4->payload);
    EXPECT_EQ(maskP, (uint8_t)fmm4->maskPayload);
    EXPECT_EQ(compress, fmm4->isCompress);
    EXPECT_EQ(merged, fmm4->isMerged);
    EXPECT_EQ(msgLen, fmm4->msgLenHighPart);

    FileMessageMeta *fmm5 = (FileMessageMeta *)(bufOld);
    EXPECT_EQ(time, fmm5->timestamp);
    EXPECT_EQ(end, (uint64_t)fmm5->endPos);
    EXPECT_EQ(uint16_t(p), (uint16_t)fmm5->payload);
    EXPECT_EQ(maskP, (uint8_t)fmm5->maskPayload);
    EXPECT_EQ(compress, fmm5->isCompress);
    EXPECT_EQ(merged, fmm5->isMerged);
    EXPECT_EQ(0, fmm5->msgLenHighPart);
}

} // namespace common
} // namespace swift
