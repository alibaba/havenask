#pragma once

#include "sql/resource/TabletManagerR.h"
#include "unittest/unittest.h"

namespace sql {

class MockTabletManagerR : public TabletManagerR {
public:
    MOCK_METHOD4(
        waitTabletByWatermark,
        void(const std::string &tableName, int64_t watermark, CallbackFunc cb, int64_t timeoutUs));
    MOCK_METHOD4(
        waitTabletByTargetTs,
        void(const std::string &tableName, int64_t watermark, CallbackFunc cb, int64_t timeoutUs));
    MOCK_CONST_METHOD1(
        getTablet, std::shared_ptr<indexlibv2::framework::ITablet>(const std::string &tableName));
};

} // namespace sql
