#ifndef ISEARCH_BS_MOCKBUILDER_H
#define ISEARCH_BS_MOCKBUILDER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/test/unittest.h"
#include "build_service/builder/Builder.h"
#include <indexlib/util/memory_control/quota_control.h>
namespace build_service {
namespace builder {

class MockBuilder : public Builder {
public:
    typedef IE_NAMESPACE(partition)::IndexBuilderPtr IndexBuilderPtr;
public:
    MockBuilder(const IndexBuilderPtr &indexBuilder =
                IndexBuilderPtr(new IE_NAMESPACE(partition)::IndexBuilder(
                                    "", IE_NAMESPACE(index_base)::ParallelBuildInfo(),
                                    IE_NAMESPACE(config)::IndexPartitionOptions(),
                                    IE_NAMESPACE(config)::IndexPartitionSchemaPtr(),
                                    IE_NAMESPACE(util)::QuotaControlPtr(
                                        new IE_NAMESPACE(util)::QuotaControl(1024)))))
    : Builder(indexBuilder) {
    ON_CALL(*this, getLastLocator(_)).WillByDefault(Return(true));
    ON_CALL(*this, getIncVersionTimestamp()).WillByDefault(Return(0));
  }

public:
    MOCK_METHOD1(doBuild, bool(const IE_NAMESPACE(document)::DocumentPtr &));
    MOCK_METHOD1(doMerge, void(const IE_NAMESPACE(config)::IndexPartitionOptions &));
    MOCK_METHOD1(stop, void(int64_t stopTimestamp));
    MOCK_CONST_METHOD1(getLastLocator, bool(common::Locator &));
    MOCK_CONST_METHOD1(getLatestVersionLocator, bool(common::Locator &));
    MOCK_CONST_METHOD0(getIncVersionTimestamp, int64_t());

    MOCK_CONST_METHOD1(getIncVersionTimestampNonBlocking, RET_STAT(int64_t& ts));
    MOCK_CONST_METHOD1(getLastLocatorNonBlocking, RET_STAT(common::Locator &locator));

    MOCK_METHOD1(getLastFlushedLocator, bool(common::Locator &));
    MOCK_METHOD0(dumpAndMerge, void());
    MOCK_METHOD0(tryDump, bool());
};

typedef ::testing::StrictMock<MockBuilder> StrictMockBuilder;
typedef ::testing::NiceMock<MockBuilder> NiceMockBuilder;

}
}

#endif //ISEARCH_BS_MOCKBUILDER_H
