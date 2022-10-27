#pragma once
#include <build_service/plugin/ModuleFactory.h>
#include <ha3/sql/ops/agg/AggFuncCreator.h>
#include <iquan_common/catalog/json/FunctionModel.h>
#include <iquan_common/catalog/json/FunctionDef.h>

BEGIN_HA3_NAMESPACE(sql);

#define REGISTER_AGG_FUNC_CREATOR(AggFuncName, AggFuncCreatorName)      \
    do {                                                                \
        AggFuncCreatorName *creator = new AggFuncCreatorName();         \
        if (!registerAggFunction(AggFuncName, creator)) {               \
            delete creator;                                             \
            SQL_LOG(ERROR, "registered agg func [%s] failed",           \
                      AggFuncName);                                     \
            return false;                                               \
        }                                                               \
    } while(false)

#define REGISTER_AGG_FUNC_INFO(AggFuncName, AccTypes, ParamTypes, ReturnType) \
    do {                                                                \
        SQL_LOG(INFO, "register agg func info\n"                        \
                "Functiom Name = %s\nAcc Types = %s\n"                  \
                "Param Types = %s\nReturn Type = %s\n",                 \
                AggFuncName, AccTypes, ParamTypes, ReturnType);         \
        if (!registerAggFuncInfo(AggFuncName, AccTypes,                 \
                        ParamTypes, ReturnType))                        \
        {                                                               \
            SQL_LOG(ERROR, "register [%s] agg func info failed", AggFuncName); \
            return false;                                               \
        }                                                               \
    } while(false)

static const std::string MODULE_SQL_AGG_FUNCTION = "_Sql_Agg_Function";

class AggFuncCreatorFactory : public build_service::plugin::ModuleFactory
{
public:
    AggFuncCreatorFactory();
    virtual ~AggFuncCreatorFactory();
private:
    AggFuncCreatorFactory(const AggFuncCreatorFactory &);
    AggFuncCreatorFactory &operator=(const AggFuncCreatorFactory &);
public:
    virtual bool registerAggFunctions() = 0;
    virtual bool registerAggFunctionInfos() = 0;
public:
    typedef std::map<std::string, AggFuncCreator *> AggFuncCreatorMap;
public:
    AggFuncCreatorMap &getAggFuncCreators() { return _funcCreatorMap; }
    AggFuncCreator *getAggFuncCreator(const std::string &name) {
        auto it = _funcCreatorMap.find(name);
        if (it != _funcCreatorMap.end()) {
            return it->second;
        }
        return nullptr;
    }
    bool registerFunctionInfos(iquan::FunctionModels &functionModels,
                               std::map<std::string, size_t> &accSize);
    void destroy() override {};
protected:
    bool registerAggFunction(const std::string &name, AggFuncCreator *creator) {
        if (_funcCreatorMap.find(name) != _funcCreatorMap.end()) {
            return false;
        }
        SQL_LOG(INFO, "start register agg func[%s]", name.c_str());
        _funcCreatorMap[name] = creator;
        return true;
    }
    bool registerAggFuncInfo(const std::string &name,
                             const std::string &accTypes,
                             const std::string &paramTypes,
                             const std::string &returnTypes);
    bool registerAccSize(const std::string &name, size_t i);
private:
    static bool parseIquanParamDef(const std::string &params,
                                   std::vector<iquan::ParamTypeDef> &paramsDef);
protected:
    AggFuncCreatorMap _funcCreatorMap;
    iquan::FunctionModels _functionInfosDef;
    std::map<std::string, size_t> _accSize;
    AUTIL_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AggFuncCreatorFactory);

END_HA3_NAMESPACE(sql);
