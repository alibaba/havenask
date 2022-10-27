#ifndef __INDEXLIB_RADIXTREEPERFTEST_H
#define __INDEXLIB_RADIXTREEPERFTEST_H

#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/SimpleAllocator.h>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/radix_tree.h"

IE_NAMESPACE_BEGIN(common);

class RadixTreePerfTest : public INDEXLIB_TESTBASE {
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
    autil::mem_pool::Pool* mByteSlicePool;
    autil::mem_pool::SimpleAllocator mAllocator;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RadixTreePerfTest, TestAppendData);

////////////////////////////////////////////////////
inline void RadixTreePerfTest::Append(RadixTree* tree, const uint32_t& value)
{
    uint32_t* item = (uint32_t*)tree->OccupyOneItem();
    *item = value;
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_RADIXTREEPERFTEST_H
