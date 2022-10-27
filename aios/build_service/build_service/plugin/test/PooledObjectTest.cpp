#include "build_service/test/unittest.h"
#include "build_service/common_define.h"
#include "build_service/plugin/PooledObject.h"
#include <autil/ThreadPool.h>

using namespace std;
using namespace autil;

namespace build_service {
namespace plugin {

class PooledObjectTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

class TestObjectBase {
public:
    TestObjectBase() {}
    virtual ~TestObjectBase() {}
public:
    virtual TestObjectBase *allocate() {
        return clone();
    }
    virtual void deallocate() {
        destroy();
    }

    virtual TestObjectBase *clone() const = 0;
    virtual void destroy() = 0;
};

typedef PooledObject<TestObjectBase> PooledTestObject;

class TestObject : public PooledTestObject {
public:
    TestObject() { a = 10; }
    TestObject(const TestObject &other)
        : PooledTestObject(other)
    {
        a = other.a;
    }
    ~TestObject() {}
public:
    /* override */ TestObjectBase *clone() const {
        return new TestObject(*this);
    }
    /* override */ void destroy() {
        delete this;
    }
public:
    int a;
};

BS_TYPEDEF_PTR(TestObject);

class AllocateObjectWorkItem : public WorkItem {
public:
    AllocateObjectWorkItem(TestObjectBase *object) {
        _object = object;
    }
    ~AllocateObjectWorkItem() {
    }
public:
    /* override */ void process() {
        for (size_t i = 0; i < 10; ++i) {
            std::vector<TestObjectBase*> objects;
            for (size_t j = 0; j < 100; ++j) {
                objects.push_back(_object->allocate());
            }
            for (size_t j = 0; j < 100; ++j) {
                objects[j]->deallocate();
            }
        }
    }
    /* override */ void destroy() {
        delete this;
    }
    /* override */ void drop() {
        process();
    }
private:
    TestObjectBase *_object;
};

void PooledObjectTest::setUp() {
}

void PooledObjectTest::tearDown() {
}

TEST_F(PooledObjectTest, testSimpleProcess) {
    BS_LOG(DEBUG, "Begin Test!");

    TestObject testObject;
    ASSERT_EQ(int(10), testObject.a);

    TestObject *clonedObject = dynamic_cast<TestObject*>(testObject.allocate());
    ASSERT_TRUE(clonedObject);
    ASSERT_EQ(int(10), clonedObject->a);
    clonedObject->a = 20;
    ASSERT_EQ(int(10), testObject.a);

    clonedObject->deallocate();
}

TEST_F(PooledObjectTest, testCloneObject) {
    BS_LOG(DEBUG, "Begin Test!");

    TestObject testObject;
    ASSERT_EQ(int(10), testObject.a);

    TestObject *clonedObject = dynamic_cast<TestObject*>(testObject.clone());
    ASSERT_TRUE(clonedObject);
    ASSERT_EQ(int(10), clonedObject->a);
    clonedObject->a = 20;
    ASSERT_EQ(int(10), testObject.a);

    clonedObject->deallocate();

    TestObject *clonedObject2 = dynamic_cast<TestObject*>(testObject.clone());
    clonedObject2->destroy();
}

TEST_F(PooledObjectTest, testOrgObjectDeallocate) {
    TestObject *testObject = new TestObject;
    ASSERT_EQ(int(10), testObject->a);
    testObject->deallocate();
}

TEST_F(PooledObjectTest, testMultiThread) {
    TestObjectPtr testObject(new TestObject);
    ThreadPool threadPool(10);
    threadPool.start();
    for (size_t i = 0; i < 100; ++i) {
        threadPool.pushWorkItem(new AllocateObjectWorkItem(testObject.get()));
    }
    threadPool.stop();
}

}
}
