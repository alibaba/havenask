#include "build_service/test/unittest.h"
#include <string>
#include "build_service/plugin/Module.h"
#include "build_service/plugin/test/NamedFactoryModule.h"
#include "build_service/config/ResourceReader.h"

using namespace std;
using namespace build_service::config;
namespace build_service {
namespace plugin {

class ModuleTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};


void ModuleTest::setUp() {
}

void ModuleTest::tearDown() {
}

TEST_F(ModuleTest, testModuleInitSuccess) {
    BS_LOG(DEBUG, "Begin Test!");
    string pluginPath = TEST_DATA_PATH"plugins/";
    string modulePath = "libdummy.so";
    Module module(modulePath, pluginPath);
    KeyValueMap map;
    ASSERT_TRUE(module.init(map, ResourceReaderPtr()));
}

TEST_F(ModuleTest, testModuleDlopenFailed) {
    BS_LOG(DEBUG, "Begin Test!");
    string pluginPath = "/not/existed/";
    string modulePath = "lib.so";
    Module module(modulePath, pluginPath);
    KeyValueMap map;
    bool ret = module.init(map, ResourceReaderPtr());
    ASSERT_TRUE(!ret);
}

TEST_F(ModuleTest, testModuleDlsymFailed) {
    BS_LOG(DEBUG, "Begin Test!");
    string pluginPath = TEST_DATA_PATH"plugins/";
    string modulePath = "libnosym.so";
    Module module(modulePath, pluginPath);
    KeyValueMap map;
    bool ret = module.init(map, ResourceReaderPtr());
    ASSERT_TRUE(!ret);
}

TEST_F(ModuleTest, testGetDlOpenMode) {
    KeyValueMap map;
    string pluginPath = TEST_DATA_PATH"plugins/";
    string modulePath = "libnosym.so";
    Module module(modulePath, pluginPath);
    int mode = 0;
    map["dl_open_mode"] = "RTLD_LAZY|RTLD_NOW|RTLD_GLOBAL|RTLD_LOCAL|RTLD_NODELETE|RTLD_NOLOAD|RTLD_DEEPBIND";
    ASSERT_TRUE(module.getDlOpenMode(map, mode));
    ASSERT_EQ(RTLD_LAZY|RTLD_NOW|RTLD_GLOBAL|RTLD_LOCAL|RTLD_NODELETE|RTLD_NOLOAD|RTLD_DEEPBIND, mode);

    map["dl_open_mode"] = "";
    ASSERT_TRUE(module.getDlOpenMode(map, mode));
    ASSERT_EQ(RTLD_NOW, mode);

    KeyValueMap map1;
    ASSERT_TRUE(module.getDlOpenMode(map1, mode));
    ASSERT_EQ(RTLD_NOW, mode);

    map["dl_open_mode"] = "RTLD_LAZY|RTLD_NOW|RTLD_GLOBAL|RTLD_LOCAL|RTLD_NODELETE|RTLD_NOLOAD|RTLD_DEEPBIND1";
    ASSERT_FALSE(module.getDlOpenMode(map, mode));
}
    
TEST_F(ModuleTest, testNamedFactoryModule) {
    BS_LOG(DEBUG, "Begin Test!");
    string pluginPath = TEST_DATA_PATH"plugins/";
    string modulePath = "libnamed_factory.so";
    {
        Module module(modulePath, pluginPath, "_TestSuffix");
        KeyValueMap map;
        bool ret = module.init(map, ResourceReaderPtr());
        ASSERT_TRUE(ret);
        NamedFactoryModule *factory = dynamic_cast<NamedFactoryModule*>(
                module.getModuleFactory());
        ASSERT_TRUE(factory);
        ASSERT_EQ(string("TestSuffix"), factory->getSuffix());
    }
    {
        Module module(modulePath, pluginPath, "_OtherTest");
        KeyValueMap map;
        bool ret = module.init(map, ResourceReaderPtr());
        ASSERT_TRUE(ret);
        NamedFactoryModule *factory = dynamic_cast<NamedFactoryModule*>(
                module.getModuleFactory());
        ASSERT_TRUE(factory);
        ASSERT_EQ(string("TestSuffix2"), factory->getSuffix());
    }
    {
        Module module(modulePath, pluginPath, "not_exist");
        KeyValueMap map;
        bool ret = module.init(map, ResourceReaderPtr());
        ASSERT_TRUE(ret);
        NamedFactoryModule *factory = dynamic_cast<NamedFactoryModule*>(
                module.getModuleFactory());
        ASSERT_TRUE(factory);
        ASSERT_EQ(string("Normal"), factory->getSuffix());
    }
    {
        Module module(modulePath, pluginPath, "");
        KeyValueMap map;
        bool ret = module.init(map, ResourceReaderPtr());
        ASSERT_TRUE(ret);
        NamedFactoryModule *factory = dynamic_cast<NamedFactoryModule*>(
                module.getModuleFactory());
        ASSERT_TRUE(factory);
        ASSERT_EQ(string("Normal"), factory->getSuffix());
    }
}


}
}
