#ifndef ISEARCH_PARASEARCHBIZ_H
#define ISEARCH_PARASEARCHBIZ_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/turing/searcher/SearcherBiz.h>

BEGIN_HA3_NAMESPACE(turing);

class ParaSearchBiz: public SearcherBiz
{
public:
    ParaSearchBiz();
    ~ParaSearchBiz();
protected:
    bool getDefaultBizJson(std::string &defaultBizJson) override;
private:
    ParaSearchBiz(const ParaSearchBiz &) = delete;
    ParaSearchBiz& operator=(const ParaSearchBiz &) = delete;
private:
    std::string getDefaultGraphDefPath() override;
    std::string getConfigZoneBizName(const std::string &zoneName) override;
    std::string getOutConfName(const std::string &confName) override;
private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<ParaSearchBiz> ParaSearchBizPtr;

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_PARASEARCHBIZ_H
