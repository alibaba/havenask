#include "suez/service/ControlServiceImpl.h"

#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class ControlServiceImplTest : public TESTBASE {};

TEST_F(ControlServiceImplTest, testUpdate) {
    ControlServiceImpl controlService;
    auto indexProvider = std::make_shared<IndexProvider>();
    UpdateArgs updateArgs;
    updateArgs.indexProvider = indexProvider;
    UpdateIndications updateIndications;
    SuezErrorCollector errorCollector;
    auto ret0 = controlService.update(updateArgs, updateIndications, errorCollector);
    ASSERT_EQ(UR_REACH_TARGET, ret0);
    ASSERT_EQ(3, indexProvider.use_count());

    updateArgs.indexProvider = std::make_shared<IndexProvider>();
    auto ret1 = controlService.update(updateArgs, updateIndications, errorCollector);
    ASSERT_EQ(UR_REACH_TARGET, ret1);
    ASSERT_EQ(1, indexProvider.use_count());
}

} // namespace suez
