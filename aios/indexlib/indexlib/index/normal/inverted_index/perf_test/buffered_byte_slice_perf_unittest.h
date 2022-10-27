#ifndef __INDEXLIB_POSTINGBUFFERPERFTEST_H
#define __INDEXLIB_POSTINGBUFFERPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice_reader.h"

IE_NAMESPACE_BEGIN(index);

class BufferedByteSlicePerfTest : public INDEXLIB_TESTBASE {
public:
    BufferedByteSlicePerfTest();
    ~BufferedByteSlicePerfTest();

    DECLARE_CLASS_NAME(BufferedByteSlicePerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSnapShotSingleRowWithCompress();
    void TestSnapShotMultiRowWithCompress();
    void TestSnapShotMultiRowWithNoCompress();

    void TestMultiSnapShotMultiRowWithCompress();

public:
    static bool CheckBuffer(BufferedByteSlice* postingBuffer,
                            common::MultiValue* value, size_t expectCount);
    template <typename T>
    static bool CheckSingleBuffer(BufferedByteSlice* postingBuffer,
                                  BufferedByteSliceReader* reader,
                                  size_t idx, size_t& count);

private:
    void DoTestSnapShot(common::MultiValue* value, uint8_t compressMode,
                        uint32_t sec = 5);
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BufferedByteSlicePerfTest, TestSnapShotSingleRowWithCompress);
INDEXLIB_UNIT_TEST_CASE(BufferedByteSlicePerfTest, TestSnapShotMultiRowWithCompress);
INDEXLIB_UNIT_TEST_CASE(BufferedByteSlicePerfTest, TestSnapShotMultiRowWithNoCompress);
INDEXLIB_UNIT_TEST_CASE(BufferedByteSlicePerfTest, TestMultiSnapShotMultiRowWithCompress);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTINGBUFFERPERFTEST_H
