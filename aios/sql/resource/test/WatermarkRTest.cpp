#include "sql/resource/WatermarkR.h"

#include "autil/result/Errors.h"
#include "indexlib/framework/Tablet.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/resource/testlib/MockTabletManagerR.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace indexlibv2::framework;

namespace sql {

class WatermarkRTest : public TESTBASE {
public:
    WatermarkRTest() {}
    ~WatermarkRTest() {}

public:
    void prepareTablet() {}

private:
    navi::NaviResourceHelper _naviRHelper;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(resource, WatermarkRTest);

TEST_F(WatermarkRTest, testDisableWatermark) {
    auto mockTabletManagerR = std::make_shared<MockTabletManagerR>();
    auto tablet = std::make_shared<Tablet>(TabletResource());
    EXPECT_CALL(*mockTabletManagerR, getTablet("table")).WillOnce(Return(tablet));
    ASSERT_TRUE(_naviRHelper.addExternalRes(mockTabletManagerR));
    _naviRHelper.kernelConfig(R"json({"table_name": "table"})json");
    auto watermarkR = _naviRHelper.getOrCreateResPtr<WatermarkR>();
    ASSERT_NE(nullptr, watermarkR);
    ASSERT_EQ(tablet, watermarkR->_tablet);
}

TEST_F(WatermarkRTest, testTabletNullError) {
    auto mockTabletManagerR = std::make_shared<MockTabletManagerR>();
    EXPECT_CALL(*mockTabletManagerR, getTablet("table")).WillOnce(Return(nullptr));
    ASSERT_TRUE(_naviRHelper.addExternalRes(mockTabletManagerR));
    _naviRHelper.kernelConfig(R"json({
        "table_name": "table",
        "target_watermark_type": 1
    })json");
    auto watermarkR = _naviRHelper.getOrCreateResPtr<WatermarkR>();
    ASSERT_EQ(nullptr, watermarkR);
}

TEST_F(WatermarkRTest, testSystemTs) {
    auto mockTabletManagerR = std::make_shared<MockTabletManagerR>();
    ASSERT_TRUE(_naviRHelper.addExternalRes(mockTabletManagerR));
    auto tablet = std::make_shared<Tablet>(TabletResource());
    EXPECT_CALL(*mockTabletManagerR, getTablet("table")).WillOnce(Return(tablet));
    EXPECT_CALL(*mockTabletManagerR, waitTabletByTargetTs("table", 1111, _, _))
        .WillRepeatedly(testing::Invoke([tablet](std::string tableName,
                                                 int64_t watermark,
                                                 TabletManagerR::CallbackFunc cb,
                                                 int64_t timeoutUs) { cb({tablet}); }));
    _naviRHelper.kernelConfig(R"json({
        "table_name": "table",
        "target_watermark": 1111,
        "target_watermark_type": 1
    })json");
    auto watermarkR = _naviRHelper.getOrCreateResPtr<WatermarkR>();
    ASSERT_NE(nullptr, watermarkR);
    ASSERT_EQ(tablet, watermarkR->_tablet);
    ASSERT_EQ(1111, watermarkR->_targetWatermark);
    ASSERT_EQ(WM_TYPE_SYSTEM_TS, watermarkR->_targetWatermarkType);
    ASSERT_NE(0, watermarkR->_metricsCollector.waitWatermarkTime);
    ASSERT_FALSE(watermarkR->_metricsCollector.waitWatermarkFailed);
}

TEST_F(WatermarkRTest, testSystemTsCurrentTime) {
    auto mockTabletManagerR = std::make_shared<MockTabletManagerR>();
    ASSERT_TRUE(_naviRHelper.addExternalRes(mockTabletManagerR));
    auto tablet = std::make_shared<Tablet>(TabletResource());
    EXPECT_CALL(*mockTabletManagerR, getTablet("table")).WillOnce(Return(tablet));
    EXPECT_CALL(*mockTabletManagerR, waitTabletByTargetTs("table", _, _, _))
        .WillRepeatedly(testing::Invoke([tablet](std::string tableName,
                                                 int64_t watermark,
                                                 TabletManagerR::CallbackFunc cb,
                                                 int64_t timeoutUs) { cb({tablet}); }));
    _naviRHelper.kernelConfig(R"json({
        "table_name": "table",
        "target_watermark_type": 1
    })json");
    auto watermarkR = _naviRHelper.getOrCreateResPtr<WatermarkR>();
    ASSERT_NE(nullptr, watermarkR);
    ASSERT_EQ(tablet, watermarkR->_tablet);
    ASSERT_NE(0, watermarkR->_targetWatermark);
    ASSERT_EQ(WM_TYPE_SYSTEM_TS, watermarkR->_targetWatermarkType);
    ASSERT_NE(0, watermarkR->_metricsCollector.waitWatermarkTime);
    ASSERT_FALSE(watermarkR->_metricsCollector.waitWatermarkFailed);
}

TEST_F(WatermarkRTest, testManualTs) {
    auto mockTabletManagerR = std::make_shared<MockTabletManagerR>();
    ASSERT_TRUE(_naviRHelper.addExternalRes(mockTabletManagerR));
    auto tablet = std::make_shared<Tablet>(TabletResource());
    EXPECT_CALL(*mockTabletManagerR, getTablet("table")).WillOnce(Return(tablet));
    EXPECT_CALL(*mockTabletManagerR, waitTabletByWatermark("table", 1111, _, _))
        .WillRepeatedly(testing::Invoke([tablet](std::string tableName,
                                                 int64_t watermark,
                                                 TabletManagerR::CallbackFunc cb,
                                                 int64_t timeoutUs) { cb({tablet}); }));
    _naviRHelper.kernelConfig(R"json({
        "table_name": "table",
        "target_watermark": 1111,
        "target_watermark_type": 2
    })json");
    auto watermarkR = _naviRHelper.getOrCreateResPtr<WatermarkR>();
    ASSERT_NE(nullptr, watermarkR);
    ASSERT_EQ(tablet, watermarkR->_tablet);
    ASSERT_NE(0, watermarkR->_targetWatermark);
    ASSERT_EQ(WM_TYPE_MANUAL, watermarkR->_targetWatermarkType);
    ASSERT_NE(0, watermarkR->_metricsCollector.waitWatermarkTime);
    ASSERT_FALSE(watermarkR->_metricsCollector.waitWatermarkFailed);
}

TEST_F(WatermarkRTest, testWaitError) {
    auto mockTabletManagerR = std::make_shared<MockTabletManagerR>();
    ASSERT_TRUE(_naviRHelper.addExternalRes(mockTabletManagerR));
    auto tablet = std::make_shared<Tablet>(TabletResource());
    EXPECT_CALL(*mockTabletManagerR, getTablet("table")).WillOnce(Return(tablet));
    EXPECT_CALL(*mockTabletManagerR, waitTabletByTargetTs("table", _, _, _))
        .WillRepeatedly(testing::Invoke(
            [](std::string tableName,
               int64_t watermark,
               TabletManagerR::CallbackFunc cb,
               int64_t timeoutUs) { cb(result::RuntimeError::make("wait failed")); }));
    _naviRHelper.kernelConfig(R"json({
        "table_name": "table",
        "target_watermark_type": 1
    })json");
    auto watermarkR = _naviRHelper.getOrCreateResPtr<WatermarkR>();
    ASSERT_NE(nullptr, watermarkR);
    ASSERT_EQ(tablet, watermarkR->_tablet);
    ASSERT_NE(0, watermarkR->_targetWatermark);
    ASSERT_EQ(WM_TYPE_SYSTEM_TS, watermarkR->_targetWatermarkType);
    ASSERT_NE(0, watermarkR->_metricsCollector.waitWatermarkTime);
    ASSERT_TRUE(watermarkR->_metricsCollector.waitWatermarkFailed);
}

} // namespace sql
