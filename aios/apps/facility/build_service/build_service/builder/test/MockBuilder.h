#pragma once

#include "build_service/builder/Builder.h"
#include "build_service/common_define.h"
#include "build_service/test/unittest.h"
#include "build_service/util/Log.h"
#include "indexlib/util/memory_control/QuotaControl.h"
namespace build_service { namespace builder {

class MockBuilder : public Builder
{
public:
    typedef indexlib::partition::IndexBuilderPtr IndexBuilderPtr;

public:
    MockBuilder(const IndexBuilderPtr& indexBuilder = IndexBuilderPtr(new indexlib::partition::IndexBuilder(
                    "", indexlib::index_base::ParallelBuildInfo(), indexlib::config::IndexPartitionOptions(),
                    indexlib::config::IndexPartitionSchemaPtr(new indexlib::config::IndexPartitionSchema),
                    indexlib::util::QuotaControlPtr(new indexlib::util::QuotaControl(1024)),
                    indexlib::partition::BuilderBranchHinter::Option::Test())))
        : Builder(indexBuilder)
    {
        ON_CALL(*this, getLastLocator(_)).WillByDefault(Return(true));
        ON_CALL(*this, getIncVersionTimestamp()).WillByDefault(Return(0));
    }

public:
    MOCK_METHOD1(doBuild, bool(const indexlib::document::DocumentPtr&));
    MOCK_METHOD1(doMerge, void(const indexlib::config::IndexPartitionOptions&));
    MOCK_METHOD1(stop, void(int64_t stopTimestamp));
    MOCK_CONST_METHOD1(getLastLocator, bool(common::Locator&));
    MOCK_CONST_METHOD1(getLatestVersionLocator, bool(common::Locator&));
    MOCK_CONST_METHOD0(getIncVersionTimestamp, int64_t());

    MOCK_CONST_METHOD1(getIncVersionTimestampNonBlocking, RET_STAT(int64_t& ts));
    MOCK_CONST_METHOD1(getLastLocatorNonBlocking, RET_STAT(common::Locator& locator));

    MOCK_METHOD1(getLastFlushedLocator, bool(common::Locator&));
    MOCK_METHOD0(dumpAndMerge, void());
    MOCK_METHOD0(tryDump, bool());
};

typedef ::testing::StrictMock<MockBuilder> StrictMockBuilder;
typedef ::testing::NiceMock<MockBuilder> NiceMockBuilder;

}} // namespace build_service::builder
