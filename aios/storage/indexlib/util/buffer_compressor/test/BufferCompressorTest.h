#pragma once

#include "indexlib/util/buffer_compressor/BufferCompressor.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {

class BufferCompressorTest : public INDEXLIB_TESTBASE
{
public:
    BufferCompressorTest(bool supportStream = true);
    virtual ~BufferCompressorTest();

    DECLARE_CLASS_NAME(BufferCompressorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForCompress();
    void TestCaseForCompressLargeData();

private:
    virtual util::BufferCompressorPtr CreateCompressor() const = 0;

protected:
    bool _supportStream = false;
};
}} // namespace indexlib::util
