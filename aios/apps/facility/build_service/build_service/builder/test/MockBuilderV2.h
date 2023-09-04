#pragma once

#include "gmock/gmock.h"

#include "build_service/builder/BuilderV2Impl.h"
#include "build_service/common_define.h"
#include "indexlib/framework/Locator.h"

namespace build_service { namespace builder {

class MockBuilderV2 : public BuilderV2Impl
{
public:
    MockBuilderV2(std::shared_ptr<indexlibv2::framework::ITablet> tablet = nullptr,
                  const proto::BuildId& buildId = proto::BuildId())
        : BuilderV2Impl(std::move(tablet), buildId)
    {
        ON_CALL(*this, getLastLocator()).WillByDefault(testing::Return(indexlibv2::framework::Locator()));
        ON_CALL(*this, getIncVersionTimestamp()).WillByDefault(testing::Return(0));
        ON_CALL(*this, stop(testing::_, testing::_, testing::_)).WillByDefault(testing::Return());
    }

public:
    MOCK_METHOD1(doBuild, indexlib::Status(const std::shared_ptr<indexlibv2::document::IDocumentBatch>&));
    MOCK_METHOD3(stop, void(std::optional<int64_t> stopTimestamp, bool, bool));
    MOCK_CONST_METHOD0(getLastLocator, indexlibv2::framework::Locator());
    MOCK_CONST_METHOD0(getLatestVersionLocator, indexlibv2::framework::Locator());
    MOCK_CONST_METHOD0(getIncVersionTimestamp, int64_t());
    MOCK_CONST_METHOD0(getLastFlushedLocator, indexlibv2::framework::Locator());
};

typedef ::testing::StrictMock<MockBuilderV2> StrictMockBuilderV2;
typedef ::testing::NiceMock<MockBuilderV2> NiceMockBuilderV2;

}} // namespace build_service::builder
