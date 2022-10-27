#pragma once

#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <string>

BEGIN_HA3_NAMESPACE(common);

class SearcherAccessLog
{
public:
    SearcherAccessLog();
    ~SearcherAccessLog();
public:
    void setPayload(const std::string &payload) {
        _payload = payload;
    }
private:
    void log();
private:
    int64_t _entryTime;
    int64_t _exitTime;
    std::string _payload;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearcherAccessLog);

END_HA3_NAMESPACE(common);
