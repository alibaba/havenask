
#include "matchdoc/Reference.h"

#include "gtest/gtest.h"
#include <sys/time.h>

using namespace std;
using namespace autil;
using namespace matchdoc;

class ReferenceTest : public testing::Test {
public:
    void SetUp() override { _pool = std::make_shared<autil::mem_pool::Pool>(); }

protected:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
};

TEST_F(ReferenceTest, testSerialize) {
    Reference<MultiChar> ref;
    DataBuffer dataBuffer;
    ref.meta.serialize(dataBuffer);
    DataBuffer db2(dataBuffer.getData(), dataBuffer.getDataLen());
    VariableType vt;
    db2.read(vt);
    uint32_t allocateSize;
    db2.read(allocateSize);
    ASSERT_EQ(8, allocateSize);
}

TEST_F(ReferenceTest, testReferenceConstructor) {
    {
        Reference<int32_t> ref;
        ASSERT_FALSE(ref.needConstructMatchDoc());
    }
    {
        VectorStorage docStorage(_pool, sizeof(int32_t));
        Reference<int32_t> ref(&docStorage, sizeof(int32_t), "default", true);
        ASSERT_TRUE(ref.needConstructMatchDoc());
    }
    {
        VectorStorage docStorage(_pool, sizeof(int32_t));
        Reference<int32_t> ref(&docStorage, sizeof(int32_t), "default", false);
        ASSERT_FALSE(ref.needConstructMatchDoc());
    }
    {
        VectorStorage docStorage(_pool, sizeof(int32_t));
        Reference<int32_t> ref(&docStorage, 0, 0, sizeof(int32_t), "default", true);
        ASSERT_TRUE(ref.needConstructMatchDoc());
    }
    {
        VectorStorage docStorage(_pool, sizeof(int32_t));
        Reference<int32_t> ref(&docStorage, 0, 0, sizeof(int32_t), "default", false);
        ASSERT_TRUE(ref.needConstructMatchDoc());
    }
    {
        VectorStorage docStorage(_pool, sizeof(int32_t));
        Reference<int32_t> ref(&docStorage, 0, INVALID_OFFSET, sizeof(int32_t), "default", true);
        ASSERT_TRUE(ref.needConstructMatchDoc());
    }
    {
        VectorStorage docStorage(_pool, sizeof(int32_t));
        Reference<int32_t> ref(&docStorage, 0, INVALID_OFFSET, sizeof(int32_t), "default", false);
        ASSERT_FALSE(ref.needConstructMatchDoc());
    }
}
