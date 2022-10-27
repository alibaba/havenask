#ifndef ISEARCH_CONFIGCLAUSEVALIDATOR_H
#define ISEARCH_CONFIGCLAUSEVALIDATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ErrorDefine.h>
#include <ha3/common/ConfigClause.h>

BEGIN_HA3_NAMESPACE(qrs);

class ConfigClauseValidator
{
public:
    ConfigClauseValidator(uint32_t retHitsLimit);
    ~ConfigClauseValidator();
public:

    ErrorCode getErrorCode();
    bool validate(const common::ConfigClause* configClause);

private:
    bool validateRetHitsLimit(const common::ConfigClause* configClause);
    bool validateResultFormat(const common::ConfigClause* configClause);

private:
    HA3_LOG_DECLARE();
    ErrorCode _errorCode;
    uint32_t _retHitsLimit;

    friend class ConfigClauseValidatorTest;
};


HA3_TYPEDEF_PTR(ConfigClauseValidator);

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_CONFIGCLAUSEVALIDATOR_H
