#ifndef ISEARCH_ERRORRESULT_H
#define ISEARCH_ERRORRESULT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/ErrorDefine.h>
#include <unistd.h>

BEGIN_HA3_NAMESPACE(common);
#define CDATA_BEGIN_STRING "<![CDATA["
#define CDATA_END_STRING "]]>"

class ErrorResult
{
public:
    ErrorResult(ErrorCode errorCode = ERROR_NONE, 
                const std::string& errMsg = "",
                const std::string& partitionID = "",
                const std::string& hostName = "");

    ~ErrorResult();

    ErrorResult& operator = (const ErrorResult &other);
public:
    bool hasError() const { 
        return _errorCode != ERROR_NONE; 
    }
    
    void setHostInfo(const std::string& partitionID, 
                     const std::string& hostName) 
    {
        _partitionID = partitionID;
        _hostName = hostName;
    }

    void setHostInfo(const std::string& partitionID) {
        _partitionID = partitionID;
        char tmpHostname[256];
        //don't care whether error happen
        gethostname(tmpHostname, sizeof(tmpHostname));
        _hostName = tmpHostname;
    }

    const std::string& getPartitionID() const {
        return _partitionID;
    }
    
    void setHostName(std::string hostName)
    {
        _hostName = hostName;
    }

    const std::string& getHostName() const {
        return _hostName;
    }

    void setErrorCode(ErrorCode errorCode) {_errorCode = errorCode;}
    ErrorCode getErrorCode() const {return _errorCode;}

    void setErrorMsg(const std::string& errorMsg) {_errorMsg = errorMsg;}
    const std::string& getErrorMsg() const {return _errorMsg;}

    void resetError(ErrorCode errorCode, const std::string &errMsg = "");
    std::string getErrorDescription() const;

    std::string toXMLString();
    std::string toJsonString();

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

protected:
    ErrorCode _errorCode;
    std::string _errorMsg;
    std::string _partitionID;
    std::string _hostName;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ErrorResult);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_ERRORRESULT_H
