#include <time.h>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format.h"
#include "indexlib/common/atomic_value_typed.h"
#include "indexlib/index/normal/inverted_index/perf_test/buffered_byte_slice_perf_unittest.h"
#include "indexlib/index/normal/inverted_index/perf_test/suspendable_thread.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BufferedByteSlicePerfTest);

volatile bool isDone;
autil::ThreadMutex mutex;

BufferedByteSlicePerfTest::BufferedByteSlicePerfTest()
{
}

BufferedByteSlicePerfTest::~BufferedByteSlicePerfTest()
{
}

void BufferedByteSlicePerfTest::CaseSetUp()
{
}

void BufferedByteSlicePerfTest::CaseTearDown()
{
}

void BufferedByteSlicePerfTest::TestSnapShotSingleRowWithCompress()
{
    DocListFormatOption option(OPTION_FLAG_NONE);
    DocListFormat docListFormat(option);
    DoTestSnapShot(&docListFormat, PFOR_DELTA_COMPRESS_MODE);
}

void BufferedByteSlicePerfTest::TestSnapShotMultiRowWithCompress()
{
    DocListFormatOption option(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(option);
    DoTestSnapShot(&docListFormat, PFOR_DELTA_COMPRESS_MODE);
}

void BufferedByteSlicePerfTest::TestMultiSnapShotMultiRowWithCompress()
{
    DocListFormatOption option(OPTION_FLAG_NONE);
    DocListFormat docListFormat(option);
    DoTestSnapShot(&docListFormat, PFOR_DELTA_COMPRESS_MODE);
}

void BufferedByteSlicePerfTest::TestSnapShotMultiRowWithNoCompress()
{
    DocListFormatOption option(EXPACK_OPTION_FLAG_ALL);
    DocListFormat docListFormat(option);
    DoTestSnapShot(&docListFormat, SHORT_LIST_COMPRESS_MODE);
}

void WriteBufferedByteSlice(BufferedByteSlice* postingBuffer, 
                        MultiValue* value, uint8_t compressMode)
{

#define PUSH_BACK_MACRO(valuetype, type)                                \
    case valuetype: {                                                   \
        AtomicValueTyped<type>* typedValue = dynamic_cast<AtomicValueTyped<type>*>(atomicValue); \
        typedValue->SetValue(idx % (numeric_limits<type>::max() - 1));  \
        postingBuffer->PushBack(typedValue);                            \
        break;                                                          \
    }

    size_t idx = 0;
    while (!isDone)
    {
        ScopedLock lock(mutex);
        for (size_t i = 0; i < value->GetAtomicValueSize(); ++i)
        {
            AtomicValue* atomicValue = value->GetAtomicValue(i);
            switch (atomicValue->GetType())
            {
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
        if (postingBuffer->NeedFlush())
        {
            postingBuffer->Flush(compressMode);
        }
        idx++;
    }
}

void SnapShotBufferedByteSlice(BufferedByteSlice* postingBuffer, MultiValue* value,  int* errCode)
{
    autil::mem_pool::Pool byteSlicePool(1024 * 1024 * 10);
    autil::mem_pool::RecyclePool bufferPool(1024 * 1024 * 10, 8);
    while (!isDone)
    {
        BufferedByteSlice snapShotBufferedByteSlice(&byteSlicePool, &bufferPool);

        size_t countBeforeSnapShot = 0;
        {
            ScopedLock lock(mutex);
            countBeforeSnapShot = postingBuffer->GetTotalCount();
        }

        postingBuffer->SnapShot(&snapShotBufferedByteSlice);
        if (!BufferedByteSlicePerfTest::CheckBuffer(&snapShotBufferedByteSlice, value, 
                        countBeforeSnapShot))
        {
            *errCode = -1;
            return;
        }
    }
}

void BufferedByteSlicePerfTest::DoTestSnapShot(MultiValue* value,
        uint8_t compressMode, uint32_t sec)
{
    const size_t threadNum = 10;
    int status[threadNum] = {0};
    isDone = false;

    autil::mem_pool::Pool byteSlicePool(1024 * 1024 * 10);
    autil::mem_pool::RecyclePool bufferPool(1024 * 1024 * 10, 8);

    BufferedByteSlice postingBuffer(&byteSlicePool, &bufferPool);
    postingBuffer.Init(value);

    vector<SuspendableThreadPtr> readThreads;
    for (size_t i = 0; i < threadNum; ++i)
    {
        SuspendableThreadPtr thread = SuspendableThread::createThread(
                tr1::bind(&SnapShotBufferedByteSlice, &postingBuffer, value, &(status[i])));
        readThreads.push_back(thread);
    }

    SuspendableThreadPtr writeThread = SuspendableThread::createThread(
            tr1::bind(&WriteBufferedByteSlice, &postingBuffer, value, compressMode));

    int64_t beginTime = TimeUtility::currentTimeInSeconds();
    int64_t endTime = beginTime;
    while (endTime - beginTime < sec)
    {
        usleep(100000);
        endTime = TimeUtility::currentTimeInSeconds();
    }
    isDone = true;

    for (size_t i = 0; i < threadNum; ++i)
    {
        readThreads[i].reset();
    }
    writeThread.reset();

    for (size_t i = 0; i < threadNum; ++i)
    {
        INDEXLIB_TEST_EQUAL((int)0, status[i]);
    }
}

bool BufferedByteSlicePerfTest::CheckBuffer(BufferedByteSlice* postingBuffer,
        MultiValue* value, size_t expectCount)
{
#define DECODE_MACRO(valuetype, type)                                   \
    case valuetype: {                                                   \
        if (!CheckSingleBuffer<type>(postingBuffer, &reader, idx, decodeCount)) \
        {                                                               \
            IE_LOG(ERROR, "last decode count [%lu], i %lu", lastDecodeCount, i); \
            return false;                                               \
        }                                                               \
        break;                                                          \
    }


    if (postingBuffer->mFlushInfo.GetFlushCount() == 0
        && postingBuffer->mFlushInfo.GetFlushLength() != 0)
    {
        cout << "flush count is 0 and flush length is "
             << postingBuffer->mFlushInfo.GetFlushLength() << endl;
        return false;
    }

    BufferedByteSliceReader reader;
    reader.Open(postingBuffer);

    size_t idx = 0;
    while (true)
    {
        size_t lastDecodeCount = 0;
        for (size_t i = 0; i < value->GetAtomicValueSize(); ++i)
        {
            size_t decodeCount = 0;
            AtomicValue* atomicValue = value->GetAtomicValue(i);
            switch (atomicValue->GetType())
            {
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
            if (lastDecodeCount && (lastDecodeCount != decodeCount))
            {
                return false;
            }
            lastDecodeCount = decodeCount;
            if (lastDecodeCount == 0)
            {
                break;
            }
        }
        if (lastDecodeCount == 0)
        {
            break;
        }
        idx += lastDecodeCount;
    }
    if ((size_t)idx < expectCount)
    {
        cout << "snapshot buffer length[" << idx << "] "
             << "buffer length before snapshot" << expectCount << "]" << endl;
        return false;
    }
    return true;
}

template <typename T>
bool BufferedByteSlicePerfTest::CheckSingleBuffer(BufferedByteSlice* postingBuffer, 
        BufferedByteSliceReader* reader,
        size_t idx, size_t& count)

{
    count = 0;
    T buffer[MAX_DOC_PER_RECORD];
    if (!reader->Decode(buffer, MAX_DOC_PER_RECORD, count))
    {
        return true;
    }
    for (size_t i = 0; i < count; ++i)
    {
        if ((idx % (numeric_limits<T>::max() - 1)) != (size_t)buffer[i])
        {
            uint64_t acutal = buffer[i];
            uint64_t expect = idx % (numeric_limits<T>::max() - 1);
            IE_LOG(ERROR, "actual [%lu], expect [%lu], total [%lu], start [%lu]"
                   ", total count [%lu], i [%lu]", 
                   acutal, expect, count, idx, 
                   postingBuffer->GetTotalCount(), i);
            return false;
        }
        idx++;
    }
    return true;
}
IE_NAMESPACE_END(index);

