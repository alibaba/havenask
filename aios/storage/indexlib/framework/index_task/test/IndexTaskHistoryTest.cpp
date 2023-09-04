#include "indexlib/framework/index_task/IndexTaskHistory.h"

#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace framework {

class IndexTaskHistoryTest : public TESTBASE
{
public:
    IndexTaskHistoryTest();
    ~IndexTaskHistoryTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.framework, IndexTaskHistoryTest);

IndexTaskHistoryTest::IndexTaskHistoryTest() {}

IndexTaskHistoryTest::~IndexTaskHistoryTest() {}

TEST_F(IndexTaskHistoryTest, TestSimpleProcess)
{
    IndexTaskHistory his;
    std::map<std::string, std::string> desc;
    his.AddLog("merge", std::make_shared<IndexTaskLog>("optimize", 0, 1, 0, desc));
    his.AddLog("merge", std::make_shared<IndexTaskLog>("inc", 1, 2, 100, desc));
    his.AddLog("merge", std::make_shared<IndexTaskLog>("inc", 4, 5, 400, desc));
    his.AddLog("alter", std::make_shared<IndexTaskLog>("0_1", 5, 6, 500, desc));

    ASSERT_TRUE(his.DeclareTaskConfig("task1", "reclaim"));
    ASSERT_FALSE(his.DeclareTaskConfig("task1", "reclaim"));
    ASSERT_TRUE(his.DeclareTaskConfig("task2", "reclaim"));

    auto cloneHis = his.Clone().Clone();
    ASSERT_EQ(2, cloneHis.GetTaskLogs("merge").size());
    ASSERT_EQ(1, cloneHis.GetTaskLogs("alter").size());

    auto logItem = cloneHis.TEST_GetTaskLog("merge", "inc");
    ASSERT_EQ(4, logItem->GetBaseVersion());

    int64_t timestamp;
    ASSERT_FALSE(cloneHis.FindTaskConfigEffectiveTimestamp("no_exist", "reclaim", timestamp));
    ASSERT_TRUE(cloneHis.FindTaskConfigEffectiveTimestamp("task1", "reclaim", timestamp));
    ASSERT_TRUE(cloneHis.FindTaskConfigEffectiveTimestamp("task2", "reclaim", timestamp));
    int64_t gap = autil::TimeUtility::currentTimeInSeconds() - timestamp;
    ASSERT_TRUE(gap >= 0 && gap <= 2);
}

}} // namespace indexlibv2::framework
