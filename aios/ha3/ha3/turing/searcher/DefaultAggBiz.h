#ifndef ISEARCH_DEFAULTAGGBIZ_H
#define ISEARCH_DEFAULTAGGBIZ_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/search/Biz.h>
#include <suez/search/BizMeta.h>
#include <ha3/config/ConfigAdapter.h>

BEGIN_HA3_NAMESPACE(turing);

class DefaultAggBiz: public suez::turing::Biz
{
public:
    DefaultAggBiz(const std::string &aggGraphPrefix);
    ~DefaultAggBiz();
private:
    DefaultAggBiz(const DefaultAggBiz &);
    DefaultAggBiz& operator=(const DefaultAggBiz &);
public:
    static bool isSupportAgg(const std::string &defaultAgg);
protected:
    bool getDefaultBizJson(std::string &defaultBizJson) override;
    tensorflow::Status preloadBiz() override;
    std::string getBizInfoFile() override;
private:
    std::string convertFunctionConf();
private:
    config::ConfigAdapterPtr _configAdapter;
    std::string _aggGraphPrefix;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<DefaultAggBiz> DefaultAggBizPtr;

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SEARCHERBIZ_H
