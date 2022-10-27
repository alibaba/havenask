#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/in_mem_bitmap_index_segment_reader.h"
#include <autil/mem_pool/SimpleAllocator.h>

IE_NAMESPACE_BEGIN(index);

class InMemBitmapIndexSegmentReaderTest : public INDEXLIB_TESTBASE {

public:
    InMemBitmapIndexSegmentReaderTest() {}
    ~InMemBitmapIndexSegmentReaderTest() {}

    DECLARE_CLASS_NAME(InMemBitmapIndexSegmentReaderTest);
public:
    typedef InMemBitmapIndexSegmentReader::PostingTable PostingTable;

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestGetSegmentPosting();
};

INDEXLIB_UNIT_TEST_CASE(InMemBitmapIndexSegmentReaderTest, TestGetSegmentPosting);

IE_NAMESPACE_END(index);
