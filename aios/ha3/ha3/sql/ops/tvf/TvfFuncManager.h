#pragma once
#include <ha3/sql/ops/tvf/builtin/BuiltinTvfFuncCreatorFactory.h>
#include <ha3/config/SqlTvfPluginConfig.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/sql/ops/tvf/TvfFuncCreatorFactory.h>
#include <ha3/sql/ops/tvf/TvfResourceBase.h>

BEGIN_HA3_NAMESPACE(sql);

class TvfFuncManager
{
public:
    TvfFuncManager();
    ~TvfFuncManager();
private:
    TvfFuncManager(const TvfFuncManager &);
    TvfFuncManager& operator=(const TvfFuncManager &);
public:
    bool init(const std::string &configPath,
              const HA3_NS(config)::SqlTvfPluginConfig &tvfPluginConfig);
    TvfFunc *createTvfFunction(const std::string &funcName);
    TvfResourceContainer* getTvfResourceContainer() { return &_resourceContainer; }
    bool regTvfModels(iquan::TvfModels &tvfModels);
private:
    bool initBuiltinFactory();
    bool addDefaultTvfInfo();
    bool initPluginFactory(const std::string &configPath,
                           const HA3_NS(config)::SqlTvfPluginConfig &tvfPluginConfig);
    bool initTvfProfile(const HA3_NS(config)::SqlTvfProfileInfos &sqlTvfProfileInfos);
    bool addFunctions(TvfFuncCreatorFactory *factory);
    bool initTvfFuncCreator();
private:
    build_service::plugin::PlugInManager *_pluginManager;
    BuiltinTvfFuncCreatorFactory _builtinFactory;
    std::vector<std::pair<std::string, TvfFuncCreatorFactory *> > _pluginsFactory;
    std::map<std::string, TvfFuncCreator *> _funcToCreator;
    std::map<std::string, TvfFuncCreator *> _tvfNameToCreator;
    TvfResourceContainer _resourceContainer;
    HA3_NS(config)::ResourceReaderPtr _resourceReaderPtr;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TvfFuncManager);

END_HA3_NAMESPACE(sql);
