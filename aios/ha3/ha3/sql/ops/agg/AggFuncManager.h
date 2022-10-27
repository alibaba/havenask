#pragma once
#include <ha3/sql/ops/agg/builtin/BuiltinAggFuncCreatorFactory.h>
#include <ha3/config/SqlAggPluginConfig.h>
#include <build_service/plugin/PlugInManager.h>
#include <build_service/config/ResourceReader.h>

BEGIN_HA3_NAMESPACE(sql);

class AggFuncManager
{
public:
    AggFuncManager();
    ~AggFuncManager();
private:
    AggFuncManager(const AggFuncManager &);
    AggFuncManager& operator=(const AggFuncManager &);
public:
    bool init(const std::string &configPath,
              const HA3_NS(config)::SqlAggPluginConfig &aggPluginConfig);
    AggFunc *createAggFunction(const std::string &aggFuncName,
                               const std::vector<ValueType> &inputTypes,
                               const std::vector<std::string> &inputFields,
                               const std::vector<std::string> &outputFields,
                               bool isLocal);
    bool registerFunctionInfos();
    bool getAccSize(const std::string &name, size_t &size) {
        if (_accSize.find(name) == _accSize.end()) {
            return false;
        }
        size = _accSize[name];
        return true;
    }
    iquan::FunctionModels getFunctionModels() { return _functionModels; }
private:
    bool initBuiltinFactory();
    bool initPluginFactory(const std::string &configPath,
                           const HA3_NS(config)::SqlAggPluginConfig &aggPluginConfig);
    bool addFunctions(AggFuncCreatorFactory *factory);
private:
    build_service::plugin::PlugInManagerPtr _pluginManagerPtr;
    BuiltinAggFuncCreatorFactory _builtinFactory;
    std::vector<std::pair<std::string, AggFuncCreatorFactory *>> _pluginsFactory;
    std::map<std::string, AggFuncCreator *> _funcToCreator;
    std::map<std::string, size_t> _accSize;
    iquan::FunctionModels _functionModels;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AggFuncManager);

END_HA3_NAMESPACE(sql);
