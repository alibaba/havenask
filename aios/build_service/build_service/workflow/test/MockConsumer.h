#ifndef ISEARCH_BS_MOCKCONSUMER_H
#define ISEARCH_BS_MOCKCONSUMER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Consumer.h"

namespace build_service {
namespace workflow {

template <typename T>
class MockConsumer : public Consumer<T>
{
public:
    MockConsumer() {
        ON_CALL(*this, consume(_)).WillByDefault(Return(FE_OK));
        ON_CALL(*this, getLocator(_)).WillByDefault(Return(true));
        ON_CALL(*this, stop(_)).WillByDefault(Return(true));
    }
    ~MockConsumer() {
    }
public:
    MOCK_METHOD1_T(consume, FlowError(const T &));
    MOCK_CONST_METHOD1_T(getLocator, bool(common::Locator &));
    MOCK_METHOD1_T(stop, bool(FlowError));
public:
    static MockConsumer<T> *create() {
        return new MockConsumer<T>();
    }
    static ::testing::NiceMock<MockConsumer<T> > *createNice() {
        return new ::testing::NiceMock<MockConsumer<T> >();
    }

};

typedef MockConsumer<document::RawDocumentPtr> MockRawDocConsumer;
typedef ::testing::StrictMock<MockRawDocConsumer> StrictMockRawDocConsumer;
typedef ::testing::NiceMock<MockRawDocConsumer> NiceMockRawDocConsumer;
typedef MockConsumer<document::ProcessedDocumentVecPtr> MockProcessedDocConsumer;
typedef ::testing::StrictMock<MockProcessedDocConsumer> StrictMockProcessedDocConsumer;
typedef ::testing::NiceMock<MockProcessedDocConsumer> NiceMockProcessedDocConsumer;

}
}

#endif //ISEARCH_BS_MOCKCONSUMER_H
