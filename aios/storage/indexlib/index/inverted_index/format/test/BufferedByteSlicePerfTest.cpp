#include <time.h>

#include "autil/Lock.h"
#include "indexlib/index/common/AtomicValueTyped.h"
#include "indexlib/index/inverted_index/format/BufferedByteSlice.h"
#include "indexlib/index/inverted_index/format/BufferedByteSliceReader.h"
#include "indexlib/index/inverted_index/format/DocListFormat.h"
#include "indexlib/index/inverted_index/format/test/SuspendableThread.h"
#include "unittest/unittest.h"

namespace indexlib::index {

std::atomic_bool isDone;
autil::ThreadMutex mutex;

class BufferedByteSlicePerfTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

    static bool CheckBuffer(BufferedByteSlice* postingBuffer, MultiValue* value, size_t expectCount);
    template <typename T>
    static bool CheckSingleBuffer(BufferedByteSlice* postingBuffer, BufferedByteSliceReader* reader, size_t idx,
                                  size_t& count);

private:
    void DoTestSnapShot(MultiValue* value, uint8_t compressMode, uint32_t sec = 5);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, BufferedByteSlicePerfTest);

void WriteBufferedByteSlice(BufferedByteSlice* postingBuffer, MultiValue* value, uint8_t compressMode)
{
#define PUSH_BACK_MACRO(valuetype, type)                                                                               \
    case valuetype: {                                                                                                  \
        AtomicValueTyped<type>* typedValue = dynamic_cast<AtomicValueTyped<type>*>(atomicValue);                       \
        typedValue->SetValue(idx % (std::numeric_limits<type>::max() - 1));                                            \
        postingBuffer->PushBack(typedValue);                                                                           \
        break;                                                                                                         \
    }
    size_t idx = 0;
    while (!isDone) {
        autil::ScopedLock lock(mutex);
        for (size_t i = 0; i < value->GetAtomicValueSize(); ++i) {
            AtomicValue* atomicValue = value->GetAtomicValue(i);
            switch (atomicValue->GetType()) {
                PUSH_BACK_MACRO(AtomicValue::vt_uint8, uint8_t);
                PUSH_BACK_MACRO(AtomicValue::vt_uint16, uint16_t);
                PUSH_BACK_MACRO(AtomicValue::vt_uint32, uint32_t);
                PUSH_BACK_MACRO(AtomicValue::vt_int8, int8_t);
                PUSH_BACK_MACRO(AtomicValue::vt_int16, int16_t);
                PUSH_BACK_MACRO(AtomicValue::vt_int32, int32_t);
            default:
                assert(false);
                break;
            }
        }
        if (postingBuffer->NeedFlush()) {
            postingBuffer->Flush(compressMode);
        }
        idx++;
    }
}

void SnapShotBufferedByteSlice(BufferedByteSlice* postingBuffer, MultiValue* value, int* errCode)
{
    autil::mem_pool::Pool byteSlicePool(1024 * 1024 * 10);
    autil::mem_pool::RecyclePool bufferPool(1024 * 1024 * 10, 8);
    while (!isDone) {
        BufferedByteSlice snapShotBufferedByteSlice(&byteSlicePool, &bufferPool);

        size_t countBeforeSnapShot = 0;
        {
            autil::ScopedLock lock(mutex);
            countBeforeSnapShot = postingBuffer->GetTotalCount();
        }

        postingBuffer->SnapShot(&snapShotBufferedByteSlice);
        if (!BufferedByteSlicePerfTest::CheckBuffer(&snapShotBufferedByteSlice, value, countBeforeSnapShot)) {
            *errCode = -1;
            return;
        }
    }
}

void BufferedByteSlicePerfTest::DoTestSnapShot(MultiValue* value, uint8_t compressMode, uint32_t sec)
{
    const size_t threadNum = 10;
    int status[threadNum] = {0};
    isDone = false;

    autil::mem_pool::Pool byteSlicePool(1024 * 1024 * 10);
    autil::mem_pool::RecyclePool bufferPool(1024 * 1024 * 10, 8);

    BufferedByteSlice postingBuffer(&byteSlicePool, &bufferPool);
    postingBuffer.Init(value);

    std::vector<std::shared_ptr<SuspendableThread>> readThreads;
    for (size_t i = 0; i < threadNum; ++i) {
        std::shared_ptr<SuspendableThread> thread =
            SuspendableThread::createThread(std::bind(&SnapShotBufferedByteSlice, &postingBuffer, value, &(status[i])));
        readThreads.push_back(thread);
    }

    std::shared_ptr<SuspendableThread> writeThread =
        SuspendableThread::createThread(std::bind(&WriteBufferedByteSlice, &postingBuffer, value, compressMode));

    int64_t beginTime = autil::TimeUtility::currentTimeInSeconds();
    int64_t endTime = beginTime;
    while (endTime - beginTime < sec) {
        usleep(100000);
        endTime = autil::TimeUtility::currentTimeInSeconds();
    }
    isDone = true;

    for (size_t i = 0; i < threadNum; ++i) {
        readThreads[i].reset();
    }
    writeThread.reset();

    for (size_t i = 0; i < threadNum; ++i) {
        ASSERT_EQ((int)0, status[i]);
    }
}

bool BufferedByteSlicePerfTest::CheckBuffer(BufferedByteSlice* postingBuffer, MultiValue* value, size_t expectCount)
{
#define DECODE_MACRO(valuetype, type)                                                                                  \
    case valuetype: {                                                                                                  \
        if (!CheckSingleBuffer<type>(postingBuffer, &reader, idx, decodeCount)) {                                      \
            AUTIL_LOG(ERROR, "last decode count [%lu], i %lu", lastDecodeCount, i);                                    \
            return false;                                                                                              \
        }                                                                                                              \
        break;                                                                                                         \
    }
    if (postingBuffer->_flushInfo.GetFlushCount() == 0 && postingBuffer->_flushInfo.GetFlushLength() != 0) {
        std::cout << "flush count is 0 and flush length is " << postingBuffer->_flushInfo.GetFlushLength() << std::endl;
        return false;
    }

    BufferedByteSliceReader reader;
    reader.Open(postingBuffer);

    size_t idx = 0;
    while (true) {
        size_t lastDecodeCount = 0;
        for (size_t i = 0; i < value->GetAtomicValueSize(); ++i) {
            size_t decodeCount = 0;
            AtomicValue* atomicValue = value->GetAtomicValue(i);
            switch (atomicValue->GetType()) {
                DECODE_MACRO(AtomicValue::vt_uint8, uint8_t);
                DECODE_MACRO(AtomicValue::vt_uint16, uint16_t);
                DECODE_MACRO(AtomicValue::vt_uint32, uint32_t);
                DECODE_MACRO(AtomicValue::vt_int8, int8_t);
                DECODE_MACRO(AtomicValue::vt_int16, int16_t);
                DECODE_MACRO(AtomicValue::vt_int32, int32_t);
            default:
                assert(false);
                break;
            }
            if (lastDecodeCount && (lastDecodeCount != decodeCount)) {
                return false;
            }
            lastDecodeCount = decodeCount;
            if (lastDecodeCount == 0) {
                break;
            }
        }
        if (lastDecodeCount == 0) {
            break;
        }
        idx += lastDecodeCount;
    }
    if ((size_t)idx < expectCount) {
        std::cout << "snapshot buffer length[" << idx << "] "
                  << "buffer length before snapshot" << expectCount << "]" << std::endl;
        return false;
    }
    return true;
}

template <typename T>
bool BufferedByteSlicePerfTest::CheckSingleBuffer(BufferedByteSlice* postingBuffer, BufferedByteSliceReader* reader,
                                                  size_t idx, size_t& count)

{
    count = 0;
    T buffer[MAX_DOC_PER_RECORD];
    if (!reader->Decode(buffer, MAX_DOC_PER_RECORD, count)) {
        return true;
    }
    for (size_t i = 0; i < count; ++i) {
        if ((idx % (std::numeric_limits<T>::max() - 1)) != (size_t)buffer[i]) {
            uint64_t actual = buffer[i];
            uint64_t expect = idx % (std::numeric_limits<T>::max() - 1);
            AUTIL_LOG(ERROR,
                      "actual [%lu], expect [%lu], total [%lu], start [%lu]"
                      ", total count [%lu], i [%lu]",
                      actual, expect, count, idx, postingBuffer->GetTotalCount(), i);
            return false;
        }
        idx++;
    }
    return true;
}

TEST_F(BufferedByteSlicePerfTest, testSnapShotSingleRowWithCompress)
{
    DocListFormatOption option(OPTION_FLAG_NONE);
    DocListFormat docListFormat(option);
    DoTestSnapShot(&docListFormat, PFOR_DELTA_COMPRESS_MODE);
}

TEST_F(BufferedByteSlicePerfTest, testSnapShotMultiRowWithCompress)
{
    DocListFormatOption option(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(option);
    DoTestSnapShot(&docListFormat, PFOR_DELTA_COMPRESS_MODE);
}

TEST_F(BufferedByteSlicePerfTest, testMultiSnapShotMultiRowWithCompress)
{
    DocListFormatOption option(OPTION_FLAG_NONE);
    DocListFormat docListFormat(option);
    DoTestSnapShot(&docListFormat, PFOR_DELTA_COMPRESS_MODE);
}

TEST_F(BufferedByteSlicePerfTest, testSnapShotMultiRowWithNoCompress)
{
    DocListFormatOption option(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(option);
    DoTestSnapShot(&docListFormat, SHORT_LIST_COMPRESS_MODE);
}

} // namespace indexlib::index
