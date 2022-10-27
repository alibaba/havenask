#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/tvf/TvfFuncManager.h"
#include <ha3/sql/ops/tvf/builtin/PrintTableTvfFunc.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(sql);

class TvfFuncManagerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    build_service::config::ResourceReaderPtr _resource;
    config::SqlTvfPluginConfig _config;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, TvfFuncManagerTest);

void TvfFuncManagerTest::setUp() {
    _resource.reset(new build_service::config::ResourceReader(
                    GET_TEST_DATA_PATH() + "/sql_tvf/"));
    std::string content;
    _resource->getFileContent(content, "sql.json");
    FromJsonString(_config, content);
}

void TvfFuncManagerTest::tearDown() {
}

class ErrorTvfFuncCreator2 : public TvfFuncCreator {
public:
    ErrorTvfFuncCreator2()
        : TvfFuncCreator("")
    {
    }
private:
    TvfFunc *createFunction(const HA3_NS(config)::SqlTvfProfileInfo &info) override {
        return nullptr;
    }
public:
    bool init(HA3_NS(config)::ResourceReader *resourceReader) override {
        return false;
    }
};

class ErrorInitTvfCreator : public PrintTableTvfFuncCreator {
private:
    virtual bool initTvfModel(iquan::TvfModel &tvfModel,
                              const HA3_NS(config)::SqlTvfProfileInfo &info)
    {
        return false;
    }
};

TEST_F(TvfFuncManagerTest, testBuiltin) {
    TvfFuncManager manager;
    ASSERT_TRUE(manager.initBuiltinFactory());
    ASSERT_EQ(8, manager._funcToCreator.size());
}

TEST_F(TvfFuncManagerTest, testInit) {
    TvfFuncManager manager;
    ASSERT_TRUE(manager.init(GET_TEST_DATA_PATH() + "/sql_tvf/", _config));
    ASSERT_EQ(9, manager._funcToCreator.size());
    ASSERT_EQ(9, manager._tvfNameToCreator.size());
}

TEST_F(TvfFuncManagerTest, testCreateTvfFunction) {
    PrintTableTvfFuncCreator *creator = new PrintTableTvfFuncCreator();
    creator->setName("printTableTvf");
    TvfFuncManager manager;
    manager._tvfNameToCreator["printTableTvf"] = creator;
    auto tvfFunc = manager.createTvfFunction("printTableTvf");
    ASSERT_TRUE(tvfFunc);
    delete tvfFunc;
    ASSERT_FALSE(manager.createTvfFunction("xxx"));
    delete creator;
}

TEST_F(TvfFuncManagerTest, testGetTvfResourceContainer) {
    TvfFuncManager manager;
    ASSERT_TRUE(manager.getTvfResourceContainer());
}

TEST_F(TvfFuncManagerTest, testRegTvfModels) {
    navi::NaviLoggerProvider provider;
    {
        PrintTableTvfFuncCreator *creator = new PrintTableTvfFuncCreator();
        creator->setName("printTableTvf");
        TvfFuncManager manager;
        manager._funcToCreator["printTableTvf"] = creator;
        iquan::TvfModels tvfModels;
        ASSERT_TRUE(manager.regTvfModels(tvfModels));
        delete creator;
    }
    {
        ErrorInitTvfCreator *creator = new ErrorInitTvfCreator();
        creator->setName("errInitTvf");
        TvfFuncManager manager;
        manager._funcToCreator["errInitTvf"]= creator;
        iquan::TvfModels tvfModels;
        ASSERT_FALSE(manager.regTvfModels(tvfModels));
        delete creator;
        ASSERT_EQ("init tvf model [printTableTvf] failed", provider.getErrorMessage());
    }
}

TEST_F(TvfFuncManagerTest, testAddDefaultTvfInfo) {
    {
        BuiltinTvfFuncCreatorFactory factory;
        ASSERT_TRUE(factory.registerTvfFunctions());
        TvfFuncManager manager;
        ASSERT_TRUE(manager.addFunctions(&factory));
        ASSERT_TRUE(manager.addDefaultTvfInfo());
        ASSERT_TRUE(manager._tvfNameToCreator.size() != 0);
        ASSERT_TRUE(manager._tvfNameToCreator.find("printTableTvf") !=
                    manager._tvfNameToCreator.end());

        ASSERT_FALSE(manager.addDefaultTvfInfo());
    }
    {
        BuiltinTvfFuncCreatorFactory factory;
        ASSERT_TRUE(factory.registerTvfFunctions());
        TvfFuncManager manager;
        ASSERT_TRUE(manager.addFunctions(&factory));
        manager._funcToCreator["printTableTvf"]->_sqlTvfProfileInfos = {};
        ASSERT_TRUE(manager.addDefaultTvfInfo());
        ASSERT_TRUE(manager._tvfNameToCreator.size() != 0);
        ASSERT_TRUE(manager._tvfNameToCreator.find("printTableTvf") ==
                    manager._tvfNameToCreator.end());
    }
}

TEST_F(TvfFuncManagerTest, testInitPluginFactory) {
    TvfFuncManager manager;
    manager._resourceReaderPtr = _resource;
    ASSERT_TRUE(manager.initPluginFactory(_resource->getConfigPath(), _config));
    ASSERT_EQ(1, manager._funcToCreator.size());
    ASSERT_TRUE(manager._funcToCreator.end() != manager._funcToCreator.find("echoTvf"));
}

TEST_F(TvfFuncManagerTest, testInitPluginFactoryAddModuleError) {
    navi::NaviLoggerProvider provider;
    TvfFuncManager manager;
    manager._resourceReaderPtr = _resource;
    std::string content;
    _resource->getFileContent(content, "sql_add_modules_error.json");
    FromJsonString(_config, content);
    ASSERT_FALSE(manager.initPluginFactory(_resource->getConfigPath(), _config));
    string errorMsg("load sql tvf function modules failed");
    ASSERT_TRUE(provider.getErrorMessage().find(errorMsg) != std::string::npos);
}

TEST_F(TvfFuncManagerTest, testInitPluginFactoryGetModuleError) {
    navi::NaviLoggerProvider provider;
    TvfFuncManager manager;
    manager._resourceReaderPtr = _resource;
    std::string content;
    _resource->getFileContent(content, "sql_get_modules_error.json");
    FromJsonString(_config, content);
    ASSERT_FALSE(manager.initPluginFactory(_resource->getConfigPath(), _config));
    ASSERT_EQ("Get module [tvf] failed", provider.getErrorMessage());
}

TEST_F(TvfFuncManagerTest, testInitPluginFactoryRegisterError) {
    navi::NaviLoggerProvider provider;
    TvfFuncManager manager;
    manager._resourceReaderPtr = _resource;
    std::string content;
    _resource->getFileContent(content, "sql_register_error.json");
    FromJsonString(_config, content);
    ASSERT_FALSE(manager.initPluginFactory(_resource->getConfigPath(), _config));
    ASSERT_EQ("register tvf function failed, module[tvf]", provider.getErrorMessage());
}

TEST_F(TvfFuncManagerTest, testInitPluginFactoryAddFunctionsError) {
    navi::NaviLoggerProvider provider;
    TvfFuncManager manager;
    manager._resourceReaderPtr = _resource;
    manager._funcToCreator["echoTvf"] = nullptr;
    ASSERT_FALSE(manager.initPluginFactory(_resource->getConfigPath(), _config));
    ASSERT_EQ("init tvf function plugins failed: conflict function name[echoTvf].",
              provider.getErrorMessage());
}

TEST_F(TvfFuncManagerTest, testInitTvfProfile) {
    navi::NaviLoggerProvider provider;
    TvfFuncManager manager;
    SqlTvfProfileInfos infos;

    infos.emplace_back(SqlTvfProfileInfo("print", "printTableTvf"));
    ASSERT_FALSE(manager.initTvfProfile(infos));
    ASSERT_EQ("can not find tvf [printTableTvf]", provider.getErrorMessage());

    PrintTableTvfFuncCreator *creator = new PrintTableTvfFuncCreator();
    creator->setName("printTableTvf");
    manager._funcToCreator["printTableTvf"] = creator;
    ASSERT_TRUE(manager.initTvfProfile(infos));

    infos.emplace_back(SqlTvfProfileInfo("print", "xxxxTvf"));
    ASSERT_FALSE(manager.initTvfProfile(infos));
    ASSERT_EQ(1, provider.getTrace("conflict").size());
    delete creator;
}

TEST_F(TvfFuncManagerTest, testInitTvfProfileFailed) {
    navi::NaviLoggerProvider provider;
    TvfFuncManager manager;
    SqlTvfProfileInfos infos;

    ErrorTvfFuncCreator2 *errCreator = new ErrorTvfFuncCreator2();
    errCreator->setName("ErrorTvf");
    manager._funcToCreator["ErrorTvf"] = errCreator;
    infos.emplace_back(SqlTvfProfileInfo("error", "ErrorTvf"));
    ASSERT_FALSE(manager.initTvfProfile(infos));
    ASSERT_EQ("init tvf function creator [ErrorTvf] failed", provider.getErrorMessage());
    delete errCreator;
}

TEST_F(TvfFuncManagerTest, testAddFunctions) {
    BuiltinTvfFuncCreatorFactory factory;
    ASSERT_TRUE(factory.registerTvfFunctions());
    TvfFuncManager manager;
    ASSERT_TRUE(manager.addFunctions(&factory));
    ASSERT_TRUE(manager._funcToCreator.size() != 0);

    ASSERT_FALSE(manager.addFunctions(&factory));
}

TEST_F(TvfFuncManagerTest, testInitTvfFuncCreator) {
    navi::NaviLoggerProvider provider;
    {
        TvfFuncManager manager;
        ASSERT_TRUE(manager.initTvfFuncCreator());
    }
    {
        TvfFuncManager manager;
        manager._funcToCreator["print"] = new PrintTableTvfFuncCreator();
        ASSERT_TRUE(manager.initTvfFuncCreator());
        delete manager._funcToCreator["print"];
    }
    {
        TvfFuncManager manager;
        manager._funcToCreator["error"] = new ErrorTvfFuncCreator2();
        ASSERT_FALSE(manager.initTvfFuncCreator());
        ASSERT_EQ("init tvf function creator [error] failed", provider.getErrorMessage());
        delete manager._funcToCreator["error"];
    }
}


END_HA3_NAMESPACE();
