#pragma once

#include "gmock/gmock.h"

#include "build_service/common_define.h"
#include "build_service/processor/Processor.h"
#include "build_service/util/Log.h"

namespace build_service { namespace processor {

class MockProcessor : public Processor
{
public:
    MockProcessor()
    {
        ON_CALL(*this, getProcessedDoc()).WillByDefault(testing::Return(document::ProcessedDocumentVecPtr()));
        ON_CALL(*this, getWaitProcessCount()).WillByDefault(testing::Return(0));
        ON_CALL(*this, getProcessedCount()).WillByDefault(testing::Return(0));
    }
    ~MockProcessor() {}

private:
    MockProcessor(const MockProcessor&);
    MockProcessor& operator=(const MockProcessor&);

public:
    using Processor::start;
    MOCK_METHOD4(start, bool(const config::ResourceReaderPtr&, const proto::PartitionId&,
                             indexlib::util::MetricProviderPtr, const indexlib::util::CounterMapPtr&));
    using Processor::loadChain;
    MOCK_METHOD3(loadChain, bool(const config::ResourceReaderPtr&, const proto::PartitionId&,
                                 const indexlib::util::CounterMapPtr&));
    MOCK_METHOD2(stop, void(bool, bool));

    MOCK_METHOD1(processDoc, void(const document::RawDocumentPtr&));
    MOCK_METHOD0(getProcessedDoc, document::ProcessedDocumentVecPtr());
    MOCK_METHOD1(process, document::ProcessedDocumentVecPtr(const document::RawDocumentVecPtr& rawDoc));

public:
    MOCK_CONST_METHOD0(getWaitProcessCount, uint32_t());
    MOCK_CONST_METHOD0(getProcessedCount, uint32_t());
    void doProcessDoc(const document::RawDocumentPtr& rawDoc) { return Processor::processDoc(rawDoc); }
    document::ProcessedDocumentVecPtr doGetProcessedDoc() { return Processor::getProcessedDoc(); }

private:
    BS_LOG_DECLARE();
};

typedef ::testing::StrictMock<MockProcessor> StrictMockProcessor;
typedef ::testing::NiceMock<MockProcessor> NiceMockProcessor;

}} // namespace build_service::processor
