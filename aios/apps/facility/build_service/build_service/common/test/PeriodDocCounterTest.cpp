#include "build_service/common/PeriodDocCounter.h"

#include <unistd.h>

#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "autil/WorkItem.h"
#include "build_service/test/unittest.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace testing;

BS_NAMESPACE_USE(util);

namespace build_service { namespace common {

template <typename T>
class TestWorkItem : public autil::WorkItem
{
public:
    TestWorkItem() {}
    ~TestWorkItem() {}
    void process() override;
};

class PeriodDocCounterTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
    template <typename T>
    void innerTest();

private:
    BS_LOG_DECLARE();
};

BS_LOG_SETUP(common, PeriodDocCounterTest);

void PeriodDocCounterTest::setUp() {}

void PeriodDocCounterTest::tearDown() {}

template <typename T>
void TestWorkItem<T>::process()
{
    PeriodDocCounter<T>* counter = PeriodDocCounter<T>::GetInstance();
    for (size_t i = 0; i < 100; i++) {
        T value = i;
        PeriodDocCounterType typeId = (PeriodDocCounterType)(i % 4);
        counter->count(typeId, value);
        usleep(20 * 1000); // 20ms
    }
}

template <>
void TestWorkItem<string>::process()
{
    PeriodDocCounter<string>* counter = PeriodDocCounter<string>::GetInstance();
    for (size_t i = 0; i < 100; i++) {
        string value = StringUtil::toString(i);
        PeriodDocCounterType typeId = (PeriodDocCounterType)(i % 4);
        counter->count(typeId, value);
        usleep(20 * 1000); // 20ms
    }
}

template <typename T>
void PeriodDocCounterTest::innerTest()
{
    string logFile = GET_TEMP_DATA_PATH() + "../doc_counter.log";
    ASSERT_TRUE(fslib::util::FileUtil::writeFile(logFile, ""));

    ThreadPool threadPool(4, 1024);
    ASSERT_TRUE(threadPool.start("docCounter"));
    for (size_t j = 0; j < 4; j++) {
        auto* workItem = new TestWorkItem<T>;
        usleep(200 * 1000);
        threadPool.pushWorkItem(workItem);
    }
    threadPool.stop();
    sleep(1);
    BS_LOG_FLUSH();
    string content;
    ASSERT_TRUE(fslib::util::FileUtil::readFile(logFile, content));
    vector<string> lines;
    StringUtil::split(lines, content, '\n');

    map<T, uint64_t> result;
    for (size_t i = 0; i < lines.size(); i++) {
        vector<string> data;
        StringUtil::split(data, lines[i], ' ');
        ASSERT_EQ(3, data.size());
        vector<string> params;
        StringUtil::split(params, data[2], ',');
        ASSERT_EQ(3, params.size());
        T key;
        uint64_t count;
        ASSERT_TRUE(StringUtil::fromString(params[1], key));
        ASSERT_TRUE(StringUtil::fromString(params[2], count));
        auto iter = result.find(key);
        if (iter != result.end()) {
            iter->second += count;
        } else {
            iter->second = count;
        }
    }
    for (auto iter = result.begin(); iter != result.end(); iter++) {
        ASSERT_EQ(4, iter->second);
    }
}

TEST_F(PeriodDocCounterTest, testSimple)
{
    setenv(BS_ENV_DOC_TRACE_INTERVAL.c_str(), "1", true);
    innerTest<string>();
    innerTest<float>();
    innerTest<double>();
    innerTest<int8_t>();
    innerTest<uint8_t>();
    innerTest<int16_t>();
    innerTest<uint16_t>();
    innerTest<int32_t>();
    innerTest<uint32_t>();
    innerTest<int64_t>();
    innerTest<uint64_t>();
    unsetenv(BS_ENV_DOC_TRACE_INTERVAL.c_str());
    PeriodDocCounterHelper::shutdown();
}

}} // namespace build_service::common
