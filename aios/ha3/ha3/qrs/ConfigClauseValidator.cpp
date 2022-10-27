#include <ha3/qrs/ConfigClauseValidator.h>
#include <ha3/config/TypeDefine.h>

using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(qrs);
HA3_LOG_SETUP(qrs, ConfigClauseValidator);

ConfigClauseValidator::ConfigClauseValidator(uint32_t retHitsLimit)
    : _errorCode(ERROR_NONE) 
{
    _retHitsLimit = retHitsLimit;
}

ConfigClauseValidator::~ConfigClauseValidator() { 
}

bool ConfigClauseValidator::validate(const ConfigClause* configClause)
{
    if (NULL == configClause)
    {
        _errorCode = ERROR_NO_CONFIG_CLAUSE;
        return false;
    }
    
    return (validateRetHitsLimit(configClause) &&
            validateResultFormat(configClause));
}

bool ConfigClauseValidator::validateRetHitsLimit(
        const ConfigClause* configClause)
{
    uint32_t hits = configClause->getStartOffset() + 
                    configClause->getHitCount();
    HA3_LOG(DEBUG, "hits:[%d], hitsLimit:[%d]", hits, _retHitsLimit);
    if (hits < configClause->getStartOffset()
        || hits > _retHitsLimit)
    {
        _errorCode = ERROR_OVER_RETURN_HITS_LIMIT;
        return false;
    }

    return true;
}

bool ConfigClauseValidator::validateResultFormat(
        const ConfigClause* configClause)
{
    const string &format = configClause->getResultFormatSetting();
    if (format != RESULT_FORMAT_XML && 
        format != RESULT_FORMAT_PROTOBUF &&
        (format != RESULT_FORMAT_FB_SUMMARY))
    {
        _errorCode = ERROR_UNSUPPORT_RESULT_FORMAT;
        return false;
    }

    return true;
}

ErrorCode ConfigClauseValidator::getErrorCode()
{
    return _errorCode;
}

END_HA3_NAMESPACE(qrs);

