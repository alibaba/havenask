#ifndef ISEARCH_BS_MOCKPROCESSOR_H
#define ISEARCH_BS_MOCKPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/test/unittest.h"
#include "build_service/processor/Processor.h"
namespace build_service {
namespace processor {

class MockProcessor : public Processor
{
public:
    MockProcessor() {
        ON_CALL(*this, getProcessedDoc()).WillByDefault(
                Return(document::ProcessedDocumentVecPtr()));
        ON_CALL(*this, getWaitProcessCount()).WillByDefault(Return(0));
        ON_CALL(*this, getProcessedCount()).WillByDefault(Return(0));
    }
    ~MockProcessor() {}
private:
    MockProcessor(const MockProcessor &);
    MockProcessor& operator=(const MockProcessor &);
public:
    MOCK_METHOD4(start, bool(const config::ResourceReaderPtr &,
                             const proto::PartitionId &,
                             IE_NAMESPACE(misc)::MetricProviderPtr,
                             const IE_NAMESPACE(util)::CounterMapPtr &));
    MOCK_METHOD3(loadChain, bool(const config::ResourceReaderPtr &,
                                 const proto::PartitionId &,
                                 const IE_NAMESPACE(util)::CounterMapPtr&));
    MOCK_METHOD0(stop, void());

    MOCK_METHOD1(processDoc, void(const document::RawDocumentPtr &));
    MOCK_METHOD0(getProcessedDoc, document::ProcessedDocumentVecPtr());
public:
    MOCK_CONST_METHOD0(getWaitProcessCount, uint32_t());
    MOCK_CONST_METHOD0(getProcessedCount, uint32_t());
private:
    BS_LOG_DECLARE();
};

typedef ::testing::StrictMock<MockProcessor> StrictMockProcessor;
typedef ::testing::NiceMock<MockProcessor> NiceMockProcessor;

}
}

#endif //ISEARCH_BS_MOCKPROCESSOR_H
