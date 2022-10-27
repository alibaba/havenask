#ifndef ISEARCH_BS_MOCKREALTIMEBUILDFLOW_H
#define ISEARCH_BS_MOCKREALTIMEBUILDFLOW_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/RealtimeBuildFlow.h"
#include "build_service/workflow/test/MockFlowFactory.h"
#include <indexlib/partition/index_partition.h>

namespace build_service {
namespace workflow {

class MockRealtimeBuildFlow;
typedef ::testing::StrictMock<MockRealtimeBuildFlow> StrictMockRealtimeBuildFlow;
typedef ::testing::NiceMock<MockRealtimeBuildFlow> NiceMockRealtimeBuildFlow;

class MockRealtimeBuildFlow : public RealtimeBuildFlow
{
public:
    MockRealtimeBuildFlow(const IE_NAMESPACE(partition)::IndexPartitionPtr indexPart
                          = IE_NAMESPACE(partition)::IndexPartitionPtr())
        : RealtimeBuildFlow(indexPart)
    {
        ON_CALL(*this, createFlowFactory()).WillByDefault(
                Invoke(std::tr1::bind(&MockRealtimeBuildFlow::createMockFactory)));
    }
    ~MockRealtimeBuildFlow() {}
private:
    MockRealtimeBuildFlow(const MockRealtimeBuildFlow &);
    MockRealtimeBuildFlow& operator=(const MockRealtimeBuildFlow &);
public:
    // MOCK_CONST_METHOD0(getBuilder, builder::Builder*());
    // MOCK_CONST_METHOD0(getProcessor, processor::Processor*());
    // MOCK_CONST_METHOD0(getReader, reader::RawDocumentReader*());
    MOCK_CONST_METHOD0(createFlowFactory, BrokerFactory*());
    MOCK_METHOD2(initConfig, bool(const config::ResourceReaderPtr &, const std::string &));
private:
    static BrokerFactory *createMockFactory() {
        return new NiceMockFlowFactory();
    }
    static MockRealtimeBuildFlow *createNice() {
        return new NiceMockRealtimeBuildFlow();
    }
};

}
}

#endif //ISEARCH_BS_MOCKREALTIMEBUILDFLOW_H
