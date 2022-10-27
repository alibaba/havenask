#ifndef ISEARCH_REQUESTCACHEKEYGENERATOR_H
#define ISEARCH_REQUESTCACHEKEYGENERATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>

BEGIN_HA3_NAMESPACE(common);

typedef std::map<std::string, std::string> ClauseOptionKVMap;

/*
  NOTE: this class is supposed to be used on qrs only
 */
class RequestCacheKeyGenerator
{
public:
    RequestCacheKeyGenerator(const RequestPtr &requestPtr);
    ~RequestCacheKeyGenerator();
private:
    RequestCacheKeyGenerator(const RequestCacheKeyGenerator &);
    RequestCacheKeyGenerator& operator=(const RequestCacheKeyGenerator &);
public:
    uint64_t generateRequestCacheKey() const;
    std::string generateRequestCacheString() const;
public:
    void disableClause(const std::string &clauseName);
    /*
      Now, we only support options in ConfigClause, KVPairClause 
      and VirtualAttributeClause
     */
    void disableOption(const std::string &clauseName, 
                       const std::string &optionName);
    bool setClauseOption(const std::string &clauseName, 
                         const std::string &optionName,
                         const std::string &optionValue);
    std::string getClauseOption(const std::string &clauseName, 
                                const std::string &optionName) const;
private:
    void convertConfigClauseToKVMap(const ConfigClause *configClause);
    void convertKVClauseToKVMap(const ConfigClause *configClause);
    void convertVirtualAttributeClauseToKVMap(
            const VirtualAttributeClause *vaClause);
private:
    std::map<std::string, ClauseOptionKVMap> _clauseKVMaps;
    std::map<std::string, std::string> _clauseCacheStrs;
private:
    friend class RequestCacheKeyGeneratorTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RequestCacheKeyGenerator);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_REQUESTCACHEKEYGENERATOR_H
