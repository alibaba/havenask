#include "build_service/test/unittest.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/plugin/ModuleFactory.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/test/DummyObj.h"
#include <string>

using namespace std;
using namespace build_service::config;

namespace build_service {
namespace plugin {

class PlugInManagerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    std::string getValueByKey(const KeyValueMap &map, const std::string &key);
protected:
    PlugInManager *_plugInManager;
};

void PlugInManagerTest::setUp() {
    string configRoot = string(TEST_DATA_PATH);
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
    _plugInManager = new PlugInManager(resourceReaderPtr, resourceReaderPtr->getPluginPath(), "");
}

void PlugInManagerTest::tearDown() {
//    PlugInManager::destroy();
    DELETE_AND_SET_NULL(_plugInManager);
}

class TestFactory : public plugin::ModuleFactory
{
public:
    class MyNewDummyObj : public DummyObj {
    public:
        MyNewDummyObj(const string &name) : _name(name) {
            _name = "MyNewDummyObj: " + _name;
        }
        string getName() {return _name;}
    private:
        string _name;
    };
    void destroy() {
        delete this;
    }
    DummyObj* createObj(const std::string &name) {
        return new MyNewDummyObj(name);
    }
};

extern "C"
ModuleFactory* createFactory_Test() {
    return new TestFactory;
}

extern "C"
void destroyFactory_Test(ModuleFactory *factory) {
    factory->destroy();
}

TEST_F(PlugInManagerTest, testGetModule) {
    string configRoot = string(TEST_DATA_PATH);
    ResourceReaderPtr resourceReaderPtr(new ResourceReader(configRoot));
    PlugInManagerPtr plugInManagerPtr(new PlugInManager(resourceReaderPtr, 
                    resourceReaderPtr->getPluginPath(), "_Test"));
    Module* module = plugInManagerPtr->getModule();
    ASSERT_TRUE(module);
    TestFactory *factory =
        dynamic_cast<TestFactory*>(module->getModuleFactory());
    ASSERT_TRUE(factory);
    DummyObj *obj = factory->createObj("Obj");
    unique_ptr<DummyObj> obj_ptr(obj);
    ASSERT_TRUE(obj);
    ASSERT_EQ(string("MyNewDummyObj: Obj"), obj->getName());
    Module* module2 = plugInManagerPtr->getModule();
    ASSERT_TRUE(module == module2);
}

TEST_F(PlugInManagerTest, testCreateDummyObj) {
    BS_LOG(DEBUG, "Begin Test!");
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = "MyDummyObj";
    moduleInfo.modulePath = "libdummy.so";
    _plugInManager->addModule(moduleInfo);
    Module* module = _plugInManager->getModule("MyDummyObj");
    ASSERT_TRUE(module);

    DummyObjFactory *factory =
        dynamic_cast<DummyObjFactory*>(module->getModuleFactory());
    ASSERT_TRUE(factory);
    DummyObj *obj = factory->createObj("Obj");
    unique_ptr<DummyObj> obj_ptr(obj);
    ASSERT_TRUE(obj);
    ASSERT_EQ(string("MyDummyObj: Obj"), obj->getName());
}

TEST_F(PlugInManagerTest, testAddModuleFailedBeacuseOfDupName) {
    BS_LOG(DEBUG, "Begin Test!");
    ASSERT_TRUE(_plugInManager);

    ModuleInfo moduleInfo;
    moduleInfo.moduleName = "module1";
    moduleInfo.modulePath = "/not/exist/path/module1.so";
    ASSERT_TRUE(_plugInManager->addModule(moduleInfo));

    moduleInfo.moduleName = "module1";
    moduleInfo.modulePath = "/not/exist/path/module2.so";
    ASSERT_TRUE(!_plugInManager->addModule(moduleInfo));
}

string PlugInManagerTest::getValueByKey(const KeyValueMap &map,
                                        const string &key)
{
    KeyValueMap::const_iterator it = map.find(key);
    if (it == map.end()) {
        return "";
    }
    return it->second;
}


}
}
