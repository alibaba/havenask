#ifndef ISEARCH_BS_MOCKCOUNTERSYNCHRONIZER_H
#define ISEARCH_BS_MOCKCOUNTERSYNCHRONIZER_H

#include "build_service/test/unittest.h"
#include "build_service/test/test.h"
#include "build_service/util/Log.h"
#include "build_service/common/CounterSynchronizer.h"

namespace build_service {
namespace common {

class MockCounterSynchronizer : public CounterSynchronizer
{
public:
    MockCounterSynchronizer() {}
    ~MockCounterSynchronizer() {}

public:
    MOCK_CONST_METHOD0(sync, bool());
};

typedef ::testing::StrictMock<MockCounterSynchronizer> StrictMockCounterSynchronizer;
typedef ::testing::NiceMock<MockCounterSynchronizer> NiceMockCounterSynchronizer;

}
}

#endif //ISEARCH_BS_MOCKCOUNTERSYNCHRONIZER_H
