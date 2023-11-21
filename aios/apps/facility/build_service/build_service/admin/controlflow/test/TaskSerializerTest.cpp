#include "build_service/admin/controlflow/TaskSerializer.h"

#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common_define.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace testing;

namespace build_service { namespace admin {

class TaskSerializerTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void TaskSerializerTest::setUp() {}

void TaskSerializerTest::tearDown() {}

template <int COUNT = 2>
class FakeTask : public TaskBase
{
public:
    FakeTask(const string& id, const string& type, const TaskResourceManagerPtr& resMgr) : TaskBase(id, type, resMgr) {}

    bool doInit(const KeyValueMap& kvMap) override
    {
        _kvMap = kvMap;
        setTaskStatus(TaskBase::ts_init);
        setProperty("count", StringUtil::toString(COUNT));
        return true;
    }

    bool doStart(const KeyValueMap& kvMap) override
    {
        KeyValueMap::const_iterator iter = _kvMap.find("auto_finish");
        if (iter != _kvMap.end() && iter->second == "true") {
            setTaskStatus(TaskBase::ts_finish);
        } else {
            setTaskStatus(TaskBase::ts_running);
        }
        return true;
    }

    bool doFinish(const KeyValueMap& kvMap) override
    {
        setTaskStatus(TaskBase::ts_finish);
        return true;
    }

    void doSyncTaskProperty(proto::WorkerNodes& workerNodes) override { setTaskStatus(TaskBase::ts_finish); }
    bool isFinished(proto::WorkerNodes& workerNodes) override { return getTaskStatus() == TaskBase::ts_finish; }
    TaskBase* clone() override
    {
        assert(false);
        return NULL;
    };

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override { json.Jsonize("status", _kvMap); }

    bool doSuspend() override
    {
        assert(false);
        return false;
    }

    bool doResume() override
    {
        assert(false);
        return false;
    }

    bool updateConfig() override
    {
        assert(false);
        return false;
    }

private:
    KeyValueMap _kvMap;
    uint64_t value[COUNT];
};

class FakeTaskFactory : public TaskFactory
{
public:
    TaskBasePtr createTaskObject(const std::string& id, const std::string& kernalType,
                                 const TaskResourceManagerPtr& resMgr) override
    {
        int count;
        if (!StringUtil::fromString(kernalType, count)) {
            return TaskBasePtr();
        }

        switch (count) {
        case 0:
            return TaskBasePtr(new FakeTask<0>(id, kernalType, resMgr));
        case 1:
            return TaskBasePtr(new FakeTask<1>(id, kernalType, resMgr));
        case 2:
            return TaskBasePtr(new FakeTask<2>(id, kernalType, resMgr));
        default:
            assert(false);
        }
        return TaskBasePtr();
    }
};

TEST_F(TaskSerializerTest, testSimple)
{
    TaskResourceManagerPtr mgr(new TaskResourceManager);
    TaskFactoryPtr factory(new FakeTaskFactory);
    KeyValueMap kvMap;
    TaskBasePtr task1 = factory->createTask("t1", "1", kvMap, mgr);
    task1->setProperty("abc", "1234");
    TaskBasePtr task2 = factory->createTask("t2", "2", kvMap, mgr);
    task2->setProperty("config", "hello");

    vector<TaskBasePtr> tasks = {task1, task2};
    TaskSerializer ts(factory, mgr);

    Jsonizable::JsonWrapper toJson;
    ts.serialize(toJson, tasks);

    tasks.clear();
    task1.reset();
    task2.reset();

    Jsonizable::JsonWrapper fromJson(ToJson(toJson.GetMap()));
    ASSERT_TRUE(ts.deserialize(fromJson, tasks));
    ASSERT_EQ(2, tasks.size());

    ASSERT_EQ(string("t1"), tasks[0]->getIdentifier());
    ASSERT_EQ(string("t2"), tasks[1]->getIdentifier());

    ASSERT_EQ(string("1"), tasks[0]->getTaskType());
    ASSERT_EQ(string("2"), tasks[1]->getTaskType());

    ASSERT_EQ(string("1234"), tasks[0]->getProperty("abc"));
    ASSERT_EQ(string("hello"), tasks[1]->getProperty("config"));

    ASSERT_EQ(string("1"), tasks[0]->getProperty("count"));
    ASSERT_EQ(string("2"), tasks[1]->getProperty("count"));
}

}} // namespace build_service::admin
