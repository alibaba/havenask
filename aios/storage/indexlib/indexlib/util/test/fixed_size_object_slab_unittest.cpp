#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
//#include "indexlib/util/fixed_size_object_slab.h"

using namespace std;

namespace indexlib { namespace util {

class FixedSizeObjectSlabTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(FixedSizeObjectSlabTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForInit()
    {
        // FixedSizeObjectSlab slab;
        // const uint32_t CHUNK_SIZE = 1024;
        // Pool pool(1024);

        // slab.Init();
    }

private:
};

INDEXLIB_UNIT_TEST_CASE(FixedSizeObjectSlabTest, TestCaseForInit);
}} // namespace indexlib::util
