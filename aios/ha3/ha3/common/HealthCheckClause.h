#ifndef ISEARCH_HEALTHCHECKCLAUSE_H
#define ISEARCH_HEALTHCHECKCLAUSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

class HealthCheckClause : public ClauseBase
{
public:
    HealthCheckClause();
    ~HealthCheckClause();
private:
    HealthCheckClause(const HealthCheckClause &);
    HealthCheckClause& operator=(const HealthCheckClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    bool isCheck() const {return _check;}
    void setCheck(bool check) {_check = check;}
    int32_t getCheckTimes() const {return _checkTimes;}
    void setCheckTimes(int32_t checkTimes) {_checkTimes = checkTimes;}
    void setRecoverTime(int64_t t) {_recoverTime = t;}
    int64_t getRecoverTime() const {return _recoverTime;}
private:
    bool _check;
    int32_t _checkTimes;
    int64_t _recoverTime;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(HealthCheckClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_HEALTHCHECKCLAUSE_H
