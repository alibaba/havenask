#include <ha3_sdk/example/final_sorter/CheatSorter.h>
#include <ha3_sdk/testlib/sorter/SorterTestHelper.h>
#include <unittest/unittest.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(sorter);

class CheatSorterTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sorter, CheatSorterTest);

TEST_F(CheatSorterTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");

    SorterTestHelper helper("CheatSorterTest.testSimpleProcess");

    // 1. fill sorter resource
    common::SorterTestParam param;
    param.requestStr = "final_sort=sort_name:CheatSorter;sort_param:sort_key#+price,cheat_ids#1|3|5|7";
    param.fakeIndex.attributes = "price: int32_t: 1,2,3,4,5,6,7,8,9,10;"
                                 "id: int32_t : 1,2,3,4,5,6,7,8,9,10";
    param.docIdStr = "0,1,2,3,4,5,6,7,8,9";
    param.scoreStr = "10,9,8,7,6,5,4,3,2,1";
    param.requiredTopK = 10;
    param.expectedDocIdStr = "0,2,4,6,1,3,5,7,8,9";

    ASSERT_TRUE(helper.init(&param));

    // 2. create sorter
    CheatSorter *sorter = new CheatSorter("CheatSorter");

    // 3. beginRequest
    ASSERT_TRUE(helper.beginRequest(sorter));

    // 4. beginSort
    ASSERT_TRUE(helper.beginSort(sorter));

    // 5. sort
    ASSERT_TRUE(helper.sort(sorter));

    // 6. endSort
    ASSERT_TRUE(helper.endSort(sorter));

    // 7. endRequest
    ASSERT_TRUE(helper.endRequest(sorter));
}

END_HA3_NAMESPACE(sorter);
