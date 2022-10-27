#ifndef ISEARCH_BS_MOCKPRODUCER_H
#define ISEARCH_BS_MOCKPRODUCER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Producer.h"

namespace build_service {
namespace workflow {

template <typename ObjectType>
std::tr1::shared_ptr<ObjectType> createEmptyItemTyped(const std::tr1::shared_ptr<ObjectType> &) {
    return std::tr1::shared_ptr<ObjectType>(new ObjectType);
}

template <typename T>
T createEmptyItem() {
    return createEmptyItemTyped(T());
}

template <typename T>
class MockProducer : public Producer<T>
{
public:
    MockProducer() {
        ON_CALL(*this, produce(_)).WillByDefault(Return(FE_EOF));
        ON_CALL(*this, seek(_)).WillByDefault(Return(true));
        ON_CALL(*this, stop()).WillByDefault(Return(true));
    }
    ~MockProducer() {
    }
public:
    MOCK_METHOD1_T(produce, FlowError(T &));
    MOCK_METHOD1_T(seek, bool(const common::Locator &));
    MOCK_METHOD0_T(stop, bool());
public:
    static MockProducer<T> *create() {
        return new MockProducer<T>();
    }
    static ::testing::NiceMock<MockProducer<T> > *createNice() {
        return new ::testing::NiceMock<MockProducer<T> >();
    }

private:
    BS_LOG_DECLARE();
};

typedef MockProducer<document::RawDocumentPtr> MockRawDocProducer;
typedef ::testing::StrictMock<MockRawDocProducer> StrictMockRawDocProducer;
typedef ::testing::NiceMock<MockRawDocProducer> NiceMockRawDocProducer;
typedef MockProducer<document::ProcessedDocumentVecPtr> MockProcessedDocProducer;
typedef ::testing::StrictMock<MockProcessedDocProducer> StrictMockProcessedDocProducer;
typedef ::testing::NiceMock<MockProcessedDocProducer> NiceMockProcessedDocProducer;

}
}

#endif //ISEARCH_BS_MOCKPRODUCER_H
