#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/plugin/ModuleFactory.h"
#include "indexlib/plugin/Module.h"
#include "indexlib/plugin/test/DummyObj.h"
#include <string>

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(plugin);

class PluginManagerTest : public INDEXLIB_TESTBASE {
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
protected:
    std::string getValueByKey(const KeyValueMap &map, const std::string &key);
protected:
    PluginManager* _pluginManager;
};

void PluginManagerTest::CaseSetUp() {
    string configRoot = string(TEST_DATA_PATH);
    _pluginManager = new PluginManager("");
}

void PluginManagerTest::CaseTearDown() {
    DELETE_AND_SET_NULL(_pluginManager);
}

TEST_F(PluginManagerTest, testCreateDummyObj) {
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = "MyDummyObj";
    moduleInfo.modulePath = "libdummy.so";
    _pluginManager->addModule(moduleInfo);
    Module* module = _pluginManager->getModule("MyDummyObj");
    ASSERT_TRUE(module);

    DummyObjFactory *factory =
        dynamic_cast<DummyObjFactory*>(module->getModuleFactory("Factory"));
    ASSERT_TRUE(factory);
    DummyObj *obj = factory->createObj("Obj");
    unique_ptr<DummyObj> obj_ptr(obj);
    ASSERT_TRUE(obj);
    ASSERT_EQ(string("MyDummyObj: Obj"), obj->getName());
}

TEST_F(PluginManagerTest, testAddModuleFailedBeacuseOfDupName) {
    ASSERT_TRUE(_pluginManager);
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = "module1";
    moduleInfo.modulePath = "/not/exist/path/module1.so";
    ASSERT_TRUE(_pluginManager->addModule(moduleInfo));

    moduleInfo.moduleName = "module1";
    moduleInfo.modulePath = "/not/exist/path/module2.so";
    ASSERT_TRUE(!_pluginManager->addModule(moduleInfo));
}

string PluginManagerTest::getValueByKey(const KeyValueMap &map,
                                        const string &key)
{
    KeyValueMap::const_iterator it = map.find(key);
    if (it == map.end()) {
        return "";
    }
    return it->second;
}

IE_NAMESPACE_END(plugin);

