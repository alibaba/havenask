#ifndef ISEARCH_ACCESSCOUNTERLOG_H
#define ISEARCH_ACCESSCOUNTERLOG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(common);

class AccessCounterLog
{
private:
    static const uint32_t LOG_INTERVAL = 30 * 60; // 30 min
public:
    AccessCounterLog();
    ~AccessCounterLog();
private:
    AccessCounterLog(const AccessCounterLog &);
    AccessCounterLog& operator=(const AccessCounterLog &);
public:
    bool canReport();
    void reportIndexCounter(const std::string &indexName, uint32_t count);
    void reportAttributeCounter(const std::string &attributeName, 
                                uint32_t count);
private:
    void log(const std::string &prefix, const std::string &name, uint32_t count);
private:
    int64_t _lastLogTime;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AccessCounterLog);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_ACCESSCOUNTERLOG_H
