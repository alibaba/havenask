
#include "indexlib/index/ann/aitheta2/util/params_initializer/ParamsInitializerFactory.h"

#include "indexlib/index/ann/aitheta2/util/params_initializer/HnswParamsInitializer.h"
#include "indexlib/index/ann/aitheta2/util/params_initializer/QcParamsInitializer.h"
#include "unittest/unittest.h"

using namespace std;
using namespace aitheta2;

namespace indexlibv2::index::ann {

class ParamsInitializerFactoryTest : public ::testing::Test
{
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    AUTIL_LOG_DECLARE();
};

TEST_F(ParamsInitializerFactoryTest, testGeneral)
{
    auto initializer = ParamsInitializerFactory::Create(LINEAR_SEARCHER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<LrParamsInitializer>(initializer));
    initializer = ParamsInitializerFactory::Create(LINEAR_BUILDER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<LrParamsInitializer>(initializer));

    initializer = ParamsInitializerFactory::Create(QC_SEARCHER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<QcParamsInitializer>(initializer));

    initializer = ParamsInitializerFactory::Create(QC_BUILDER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<QcParamsInitializer>(initializer));

    initializer = ParamsInitializerFactory::Create(QC_STREAMER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<QcParamsInitializer>(initializer));

    initializer = ParamsInitializerFactory::Create(GPU_QC_SEARCHER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<GpuQcParamsInitializer>(initializer));

    initializer = ParamsInitializerFactory::Create(HNSW_SEARCHER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<HnswParamsInitializer>(initializer));

    initializer = ParamsInitializerFactory::Create(HNSW_BUILDER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<HnswParamsInitializer>(initializer));

    initializer = ParamsInitializerFactory::Create(QGRAPH_SEARCHER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<QGraphParamsInitializer>(initializer));

    initializer = ParamsInitializerFactory::Create(QGRAPH_BUILDER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<QGraphParamsInitializer>(initializer));

    initializer = ParamsInitializerFactory::Create(QGRAPH_STREAMER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<QGraphParamsInitializer>(initializer));

    initializer = ParamsInitializerFactory::Create(OSWG_STREAMER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<OswgParamsInitializer>(initializer));

    initializer = ParamsInitializerFactory::Create(HNSW_STREAMER, 0);
    ASSERT_NE(nullptr, initializer);
    ASSERT_NE(nullptr, dynamic_pointer_cast<HnswParamsInitializer>(initializer));

    ASSERT_EQ(nullptr, ParamsInitializerFactory::Create("unknown", 0));
}

} // namespace indexlibv2::index::ann
