#include "autil/result/Result.h"
#include "suez/table/wal/WALStrategy.h"
#include "unittest/unittest.h"

namespace suez {

class MockWALStrategy : public WALStrategy {
public:
    MOCK_METHOD2(log, void(const std::vector<std::pair<uint16_t, std::string>> &, CallbackType done));
    MOCK_METHOD0(flush, void());
    MOCK_METHOD0(stop, void());
};

typedef ::testing::StrictMock<MockWALStrategy> StrictMockWALStrategy;
typedef ::testing::NiceMock<MockWALStrategy> NiceMockWALStrategy;

} // namespace suez
