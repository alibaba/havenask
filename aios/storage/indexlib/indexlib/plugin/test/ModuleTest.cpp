#include "indexlib/plugin/Module.h"

#include <string>

#include "indexlib/plugin/test/NamedFactoryModule.h"
#include "indexlib/test/unittest.h"

using namespace std;

namespace indexlib { namespace plugin {

class ModuleTest : public INDEXLIB_TESTBASE
{
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
};

void ModuleTest::CaseSetUp() {}

void ModuleTest::CaseTearDown() {}

TEST_F(ModuleTest, testModuleInitSuccess)
{
    string pluginPath = GET_PRIVATE_TEST_DATA_PATH() + "plugins/";
    string modulePath = "libdummy.so";
    Module module(modulePath, pluginPath);
    ASSERT_TRUE(module.init());
    ModuleFactory* factory = module.getModuleFactory("Factory");
    ASSERT_TRUE(factory);

    ModuleFactory* factory1 = module.getModuleFactory("Factory");
    ASSERT_TRUE(factory1);

    ASSERT_EQ((void*)factory, (void*)factory1);
}

TEST_F(ModuleTest, testModuleDlopenFailed)
{
    string pluginPath = "/not/existed/";
    string modulePath = "lib.so";
    Module module(modulePath, pluginPath);
    bool ret = module.init();
    ASSERT_TRUE(!ret);
}

TEST_F(ModuleTest, testModuleDlsymFailed)
{
    string pluginPath = GET_PRIVATE_TEST_DATA_PATH() + "plugins/";
    string modulePath = "libnosym.so";
    Module module(modulePath, pluginPath);
    bool ret = module.init();
    ASSERT_TRUE(ret);

    ModuleFactory* factory = module.getModuleFactory("Factory");
    ASSERT_FALSE(factory);
}

TEST_F(ModuleTest, testNamedFactoryModule)
{
    string pluginPath = GET_PRIVATE_TEST_DATA_PATH() + "plugins/";
    string modulePath = "libnamed_factory.so";
    {
        Module module(modulePath, pluginPath);
        bool ret = module.init();
        ASSERT_TRUE(ret);
        NamedFactoryModule* factory = dynamic_cast<NamedFactoryModule*>(module.getModuleFactory("Factory_TestSuffix"));
        ASSERT_TRUE(factory);
        ASSERT_EQ(string("TestSuffix"), factory->getSuffix());
    }
    {
        Module module(modulePath, pluginPath);
        bool ret = module.init();
        ASSERT_TRUE(ret);
        NamedFactoryModule* factory = dynamic_cast<NamedFactoryModule*>(module.getModuleFactory("Factory_OtherTest"));
        ASSERT_TRUE(factory);
        ASSERT_EQ(string("TestSuffix2"), factory->getSuffix());
    }
    {
        Module module(modulePath, pluginPath);
        bool ret = module.init();
        ASSERT_TRUE(ret);
        NamedFactoryModule* factory = dynamic_cast<NamedFactoryModule*>(module.getModuleFactory("not_exist"));
        ASSERT_FALSE(factory);
    }
    {
        Module module(modulePath, pluginPath);
        bool ret = module.init();
        ASSERT_TRUE(ret);
        NamedFactoryModule* factory = dynamic_cast<NamedFactoryModule*>(module.getModuleFactory("Factory"));
        ASSERT_TRUE(factory);
        ASSERT_EQ(string("Normal"), factory->getSuffix());
    }
    {
        Module module(modulePath, pluginPath);
        bool ret = module.init();
        ASSERT_TRUE(ret);
        {
            NamedFactoryModule* factory =
                dynamic_cast<NamedFactoryModule*>(module.getModuleFactory("Factory_TestSuffix"));
            ASSERT_TRUE(factory);
            ASSERT_EQ(string("TestSuffix"), factory->getSuffix());
        }
        {
            NamedFactoryModule* factory =
                dynamic_cast<NamedFactoryModule*>(module.getModuleFactory("Factory_OtherTest"));
            ASSERT_TRUE(factory);
            ASSERT_EQ(string("TestSuffix2"), factory->getSuffix());
        }
        {
            NamedFactoryModule* factory = dynamic_cast<NamedFactoryModule*>(module.getModuleFactory("not_exist"));
            ASSERT_FALSE(factory);
        }
    }
}

}} // namespace indexlib::plugin
