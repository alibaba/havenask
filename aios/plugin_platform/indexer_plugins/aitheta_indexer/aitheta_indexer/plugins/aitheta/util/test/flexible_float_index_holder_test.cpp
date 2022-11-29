#include "aitheta_indexer/plugins/aitheta/util/flexible_float_index_holder.h"

#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/test/test.h"
#include <map>
#include <string>
#include <unittest/unittest.h>

using namespace std;
IE_NAMESPACE_BEGIN(aitheta_plugin);

class FlexibleFloatIndexHolderTest : public ::testing::Test {
 public:
    FlexibleFloatIndexHolderTest() {}

    virtual ~FlexibleFloatIndexHolderTest() {}

    virtual void SetUp() {}
    virtual void TearDown() {}
    EmbeddingPtr createEmbeddingInfo(float seed, int dim) {
        EmbeddingPtr ret;
        SHARED_PTR_MALLOC(ret, dim, float);
        for (int i = 0; i < dim; ++i) {
            ret.get()[i] = seed;
        }
        return ret;
    }

 protected:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(aitheta_plugin, FlexibleFloatIndexHolderTest);

TEST_F(FlexibleFloatIndexHolderTest, testMipsParams) {
    int dim = 16;
    FlexibleFloatIndexHolder indexHolder(dim);
    ASSERT_EQ(16, indexHolder.dimension());
    MipsParams params(true, 4U, 0.0F, 0.0F);
    auto embedding0 = createEmbeddingInfo(1.0f, dim);
    auto embedding1 = createEmbeddingInfo(2.0f, dim);
    indexHolder.emplace_back(1, embedding0);
    indexHolder.emplace_back(2, embedding1);
    MipsParams updatedParams;
    ASSERT_TRUE(indexHolder.mipsReform(params, updatedParams));
    ASSERT_EQ(20, indexHolder.dimension());
    ASSERT_FLOAT_EQ(8.0f, updatedParams.norm);
}

TEST_F(FlexibleFloatIndexHolderTest, testMipsParams2) {
    int dim = 16;
    FlexibleFloatIndexHolder indexHolder(dim);
    MipsParams params(false, 4U, 0.0F, 0.0F);
    auto embedding0 = createEmbeddingInfo(1.0f, dim);
    auto embedding1 = createEmbeddingInfo(2.0f, dim);
    indexHolder.emplace_back(1, embedding0);
    indexHolder.emplace_back(2, embedding1);
    MipsParams updatedParams;
    ASSERT_FALSE(indexHolder.mipsReform(params, updatedParams));
    ASSERT_EQ(16, indexHolder.dimension());
    ASSERT_FLOAT_EQ(0.0f, updatedParams.norm);
}

TEST_F(FlexibleFloatIndexHolderTest, testMipsParams3) {
    int dim = 16;
    FlexibleFloatIndexHolder indexHolder(dim);
    ASSERT_EQ(16, indexHolder.dimension());
    MipsParams params(true, 4U, 0.0F, 0.0F);
    auto embedding0 = createEmbeddingInfo(1.0f, dim);
    auto embedding1 = createEmbeddingInfo(2.0f, dim);
    indexHolder.emplace_back(1, embedding0);
    indexHolder.emplace_back(2, embedding1);
    MipsParams updatedParams;
    ASSERT_TRUE(indexHolder.mipsReform(params, updatedParams));
    ASSERT_EQ(20, indexHolder.dimension());
    ASSERT_FALSE(indexHolder.mipsReform(params, updatedParams));
}

IE_NAMESPACE_END(aitheta_plugin);
