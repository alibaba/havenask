#pragma once
#include <build_service/plugin/ModuleFactory.h>
#include <ha3/sql/ops/tvf/TvfFuncCreator.h>
#include <iquan_common/catalog/json/FunctionModel.h>
#include <iquan_common/catalog/json/TvfFunctionModel.h>
#include <iquan_common/catalog/json/FunctionDef.h>
#include "ha3/sql/ops/tvf/TvfResourceBase.h"

BEGIN_HA3_NAMESPACE(sql);

#define REGISTER_TVF_FUNC_CREATOR(creatorName, TvfFuncCreatorName)      \
    do {                                                                \
        TvfFuncCreatorName *creator = new TvfFuncCreatorName();         \
        creator->setName(creatorName);                                  \
        if (!addTvfFunctionCreator(creator)) {                          \
            SQL_LOG(ERROR, "registered tvf func [%s] failed",           \
                    creator->getName().c_str());                        \
            delete creator;                                             \
            return false;                                               \
        }                                                               \
    } while(false)

static const std::string MODULE_SQL_TVF_FUNCTION = "_Sql_Tvf_Function";

class TvfFuncCreatorFactory : public build_service::plugin::ModuleFactory
{
public:
    TvfFuncCreatorFactory();
    virtual ~TvfFuncCreatorFactory();
private:
    TvfFuncCreatorFactory(const TvfFuncCreatorFactory &);
    TvfFuncCreatorFactory &operator=(const TvfFuncCreatorFactory &);
public:
    virtual bool registerTvfFunctions() = 0;
public:
    virtual bool init(const KeyValueMap &parameters) override;
    virtual bool init(const KeyValueMap &parameters,
                      const build_service::config::ResourceReaderPtr &resourceReaderPtr) override;
    virtual bool init(const KeyValueMap &parameters,
                      const build_service::config::ResourceReaderPtr &resourceReaderPtr,
                      TvfResourceContainer *resourceContainer);
    TvfFuncCreatorMap &getTvfFuncCreators();
    void destroy() override {
        delete this;
    };
public:
    TvfFuncCreator *getTvfFuncCreator(const std::string &name);
protected:
    bool addTvfFunctionCreator(TvfFuncCreator *creator);

protected:
    TvfFuncCreatorMap _funcCreatorMap;
    AUTIL_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(TvfFuncCreatorFactory);

END_HA3_NAMESPACE(sql);
