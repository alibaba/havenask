#include "iquan/jni/IquanCache.h"

#include "unittest/unittest.h"

namespace iquan {

using RapidDocPtr = std::shared_ptr<autil::legacy::RapidDocument>;
class IquanCacheTest : public TESTBASE {};

TEST_F(IquanCacheTest, testCacheValueInfo) {
    {
        CacheValueInfo<RapidDocPtr> cacheInfo;
        ASSERT_EQ((int64_t)0, cacheInfo.updateTimeUs);
        ASSERT_EQ((int64_t)1, cacheInfo.sizeInBytes);
        ASSERT_FALSE(cacheInfo.value);
    }

    {
        RapidDocPtr value(new autil::legacy::RapidDocument);
        CacheValueInfo cacheInfo {100, 200, value};
        ASSERT_EQ((int64_t)100, cacheInfo.updateTimeUs);
        ASSERT_EQ((int64_t)200, cacheInfo.sizeInBytes);
        ASSERT_EQ((uint64_t)value.get(), (uint64_t)cacheInfo.value.get());
    }
}

TEST_F(IquanCacheTest, testCacheValueGetSizeCallBack) {
    RapidDocPtr value(new autil::legacy::RapidDocument);
    CacheValueInfo<RapidDocPtr> cacheInfo {0, 200, value};
    IquanCache<RapidDocPtr>::CacheValueGetSizeCallBack cacheValueGetSizeCallBack;
    int64_t size = cacheValueGetSizeCallBack(cacheInfo);
    ASSERT_EQ((int64_t)200, size);
}
TEST_F(IquanCacheTest, testSimpleTest) {
    CacheConfig cacheConfig;
    cacheConfig.maxSize = 100;

    std::unique_ptr<IquanCache<RapidDocPtr>> cachePtr(new IquanCache<RapidDocPtr>(cacheConfig));
    ASSERT_TRUE(cachePtr != nullptr);
    Status status = cachePtr->reset();
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_EQ((uint64_t)0, cachePtr->keyCount());
    ASSERT_EQ((int64_t)0, cachePtr->size());
    ASSERT_EQ((int64_t)100, cachePtr->capcaity());

    std::shared_ptr<autil::legacy::RapidDocument> value1(new autil::legacy::RapidDocument());
    status = cachePtr->put("1", 20, value1);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_EQ((uint64_t)1, cachePtr->keyCount());
    ASSERT_EQ((int64_t)20, cachePtr->size());
    ASSERT_EQ((int64_t)100, cachePtr->capcaity());

    std::shared_ptr<autil::legacy::RapidDocument> value2(new autil::legacy::RapidDocument());
    status = cachePtr->put("2", 30, value2);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_EQ((uint64_t)2, cachePtr->keyCount());
    ASSERT_EQ((int64_t)50, cachePtr->size());
    ASSERT_EQ((int64_t)100, cachePtr->capcaity());

    std::shared_ptr<autil::legacy::RapidDocument> value3(new autil::legacy::RapidDocument());
    status = cachePtr->put("3", 40, value3);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_EQ((uint64_t)3, cachePtr->keyCount());
    ASSERT_EQ((int64_t)90, cachePtr->size());
    ASSERT_EQ((int64_t)100, cachePtr->capcaity());

    {
        std::shared_ptr<autil::legacy::RapidDocument> value;
        status = cachePtr->get("1", value);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_TRUE(value == value1);

        status = cachePtr->get("2", value);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_TRUE(value == value2);

        status = cachePtr->get("3", value);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_TRUE(value == value3);
    }

    std::shared_ptr<autil::legacy::RapidDocument> value4(new autil::legacy::RapidDocument());
    status = cachePtr->put("4", 15, value4);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_EQ((uint64_t)3, cachePtr->keyCount());
    ASSERT_EQ((int64_t)85, cachePtr->size());
    ASSERT_EQ((int64_t)100, cachePtr->capcaity());

    {
        std::shared_ptr<autil::legacy::RapidDocument> value;
        status = cachePtr->get("1", value);
        ASSERT_FALSE(status.ok());

        status = cachePtr->get("2", value);
        ASSERT_TRUE(status.ok()) << status.errorMessage();

        status = cachePtr->get("3", value);
        ASSERT_TRUE(status.ok()) << status.errorMessage();

        status = cachePtr->get("4", value);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
    }

    std::shared_ptr<autil::legacy::RapidDocument> newValue4(new autil::legacy::RapidDocument());
    status = cachePtr->put("4", 10, newValue4);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_EQ((uint64_t)3, cachePtr->keyCount());
    ASSERT_EQ((int64_t)80, cachePtr->size());
    ASSERT_EQ((int64_t)100, cachePtr->capcaity());
}

} // namespace iquan
