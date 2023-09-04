#include "indexlib/index/kkv/KKVIndexFactory.h"

#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/kkv/building/KKVMemIndexer.h"
#include "indexlib/index/kkv/built/KKVDiskIndexer.h"
#include "indexlib/index/kkv/config/test/KKVIndexConfigBuilder.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class KKVIndexFactoryTest : public TESTBASE
{
};

TEST_F(KKVIndexFactoryTest, TestCreateMemIndexer)
{
    KKVIndexFactory factory;

    std::string fields = "pkey:uint32;skey:uint32;value:string";
    auto [_, indexConfig] = KKVIndexConfigBuilder::MakeIndexConfig(fields, "pkey", "skey", "value", -1);
    ASSERT_TRUE(indexConfig);
    indexlibv2::index::IndexerParameter parameter;
    config::TabletOptions tmpTabletOptions;
    tmpTabletOptions.SetTabletName("test");
    parameter.tabletOptions = &tmpTabletOptions;

    {
        auto indexer = factory.CreateMemIndexer(indexConfig, parameter);
        ASSERT_FALSE(indexer);
    }

    {
        int64_t maxMemoryUseInBytes = 1024 * 1024;
        parameter.maxMemoryUseInBytes = maxMemoryUseInBytes;
        auto indexer = factory.CreateMemIndexer(indexConfig, parameter);
        ASSERT_TRUE(indexer);
        auto typedIndexer = std::dynamic_pointer_cast<KKVMemIndexer<uint32_t>>(indexer);
        ASSERT_TRUE(typedIndexer);

        maxMemoryUseInBytes = maxMemoryUseInBytes * 0.5;
        ASSERT_EQ(typedIndexer->_maxPKeyMemUse, (size_t)((maxMemoryUseInBytes)*0.1));
        ASSERT_EQ(typedIndexer->_maxSKeyMemUse, (size_t)((maxMemoryUseInBytes)*0.01));
        ASSERT_EQ(typedIndexer->_maxValueMemUse, (size_t)((maxMemoryUseInBytes)-typedIndexer->_maxSKeyMemUse));
    }
}

TEST_F(KKVIndexFactoryTest, TestCreateDiskIndexer)
{
    KKVIndexFactory factory;

    std::string fields = "pkey:uint32;skey:uint32;value:string";
    auto [_, indexConfig] = KKVIndexConfigBuilder::MakeIndexConfig(fields, "pkey", "skey", "value", -1);
    ASSERT_TRUE(indexConfig);
    indexlibv2::index::IndexerParameter parameter;
    parameter.segmentInfo.reset(new framework::SegmentInfo);
    parameter.segmentId = 0;
    {
        auto indexer = factory.CreateDiskIndexer(indexConfig, parameter);
        ASSERT_TRUE(indexer);
    }

    {
        auto indexer = factory.CreateDiskIndexer(indexConfig, parameter);
        ASSERT_TRUE(indexer);
        auto typedIndexer = std::dynamic_pointer_cast<KKVDiskIndexer<uint32_t>>(indexer);
        ASSERT_TRUE(typedIndexer);
    }
}

} // namespace indexlibv2::index
