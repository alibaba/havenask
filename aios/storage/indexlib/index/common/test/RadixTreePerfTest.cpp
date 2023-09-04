#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/SimpleAllocator.h"
#include "indexlib/index/common/RadixTree.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {

class RadixTreePerfTest : public INDEXLIB_TESTBASE
{
public:
    RadixTreePerfTest();
    ~RadixTreePerfTest();

    DECLARE_CLASS_NAME(RadixTreePerfTest);
    typedef RadixTreeNode::Slot Slot;

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAppendData();

private:
    void Append(RadixTree* tree, const uint32_t& value);

private:
    autil::mem_pool::Pool* _byteSlicePool;
    autil::mem_pool::SimpleAllocator _allocator;
};

INDEXLIB_UNIT_TEST_CASE(RadixTreePerfTest, TestAppendData);

////////////////////////////////////////////////////
RadixTreePerfTest::RadixTreePerfTest() { _byteSlicePool = new autil::mem_pool::Pool(&_allocator, 10240); }

RadixTreePerfTest::~RadixTreePerfTest() { delete _byteSlicePool; }

void RadixTreePerfTest::CaseSetUp() {}

void RadixTreePerfTest::CaseTearDown() { _byteSlicePool->release(); }

void RadixTreePerfTest::Append(RadixTree* tree, const uint32_t& value)
{
    uint32_t* item = (uint32_t*)tree->OccupyOneItem();
    *item = value;
}

void RadixTreePerfTest::TestAppendData()
{
    // 100000000
    // RadixTree(16, 16, x, x); 5s
    // RadixTree(8, 8, x, x); 6s
    // RadixTree(32, 32, x, x); 4s
    // RadixTree(64, 64, x, x); 3s
    // RadixTree(128, 128, x, x); 3s
    // RadixTree(16, 16*1024, x, x); 3s
    // RadixTree(128, 16*1024, x, x); 2s
    void* buffer = _byteSlicePool->allocate(sizeof(RadixTree));
    RadixTree* tree = new (buffer) RadixTree(128, 16 * 1024, _byteSlicePool, sizeof(uint32_t));
    for (uint32_t i = 0; i < 100000000; i++) {
        Append(tree, i);
    }
}
} // namespace indexlib::index
