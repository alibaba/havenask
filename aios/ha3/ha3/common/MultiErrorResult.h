#ifndef ISEARCH_MULTIERRORRESULT_H
#define ISEARCH_MULTIERRORRESULT_H

#include <vector>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ErrorResult.h>

BEGIN_HA3_NAMESPACE(common);

class MultiErrorResult;

HA3_TYPEDEF_PTR(MultiErrorResult);
typedef std::vector<ErrorResult> ErrorResults;

class MultiErrorResult
{
public:
    MultiErrorResult() {}
    ~MultiErrorResult() {}
public:
    bool hasError() const { 
        return _mErrorResult.size() != 0;
    }
    bool hasErrorCode(ErrorCode ec) const {
        for (size_t i = 0; i < _mErrorResult.size(); ++i) {
            const ErrorResult& error = _mErrorResult[i];
            ErrorCode errorCode = error.getErrorCode();
            if (ec == errorCode) {
                return true;
            }
        }
        return false;
    }

    size_t getErrorCount() const {
        return _mErrorResult.size();        
    }
    const ErrorResults& getErrorResults() const {
        return _mErrorResult;
    }

    void resetAllHostInfo(const std::string& partitionID, 
                          const std::string& hostName = "");
    void setHostInfo(const std::string& partitionID, 
                     const std::string& hostName = "");
    void addError(const ErrorCode& errorCode, const std::string& errorMsg = "");
    void addErrorResult(const MultiErrorResultPtr &multiError);
    void addErrorResult(const ErrorResult &error);

    std::string toXMLString();
    std::string toDebugString() const;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

    void resetErrors() {
        _mErrorResult.clear();
    }

private:
    std::string collectErrorString();
private:
    ErrorResults _mErrorResult;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_MULTIERRORRESULT_H
