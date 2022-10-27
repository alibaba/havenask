#include <ha3/common/ErrorResult.h>
#include <ha3/common/XMLResultFormatter.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, ErrorResult);
using namespace std;

ErrorResult::ErrorResult(ErrorCode errorCode,
                         const string& errMsg,
                         const string& partitionID,
                         const string& hostName) :
    _errorCode(errorCode), _errorMsg(errMsg),
    _partitionID(partitionID), _hostName(hostName)
{
}

ErrorResult& ErrorResult::operator = (const ErrorResult &other) {
    if (this != &other) {
        _errorCode = other._errorCode;
        _errorMsg = other._errorMsg;
        _partitionID = other._partitionID;
        _hostName = other._hostName;
    }
    return *this;
}

ErrorResult::~ErrorResult() {
}

void ErrorResult::resetError(ErrorCode errorCode, const string &errMsg) {
    _errorCode = errorCode;
    _errorMsg  = errMsg;
}

string ErrorResult::getErrorDescription() const {
    const string &msg = haErrorCodeToString(getErrorCode());
    if (_errorMsg.find(msg) == 0) {
        return _errorMsg;
    }

    if (_errorMsg.empty()) {
        return msg;
    }

    return msg + " " + _errorMsg;
}

string ErrorResult::toXMLString() {
    stringstream ss;
    ss << "<Error>" << endl
       << "\t<PartitionID>" << _partitionID <<"</PartitionID>" << endl
       << "\t<HostName>" << _hostName <<"</HostName>" << endl
       << "\t<ErrorCode>" << getErrorCode() << "</ErrorCode>" << endl
       << "\t<ErrorDescription>" << CDATA_BEGIN_STRING
       << getErrorDescription() << CDATA_END_STRING
       << "</ErrorDescription>" << endl
       << "</Error>" << endl;
    return ss.str();
}

string ErrorResult::toJsonString() {
    return "{\"Error\":" + getErrorDescription() + "}";
}

void ErrorResult::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_errorCode);
    dataBuffer.write(_errorMsg);
    dataBuffer.write(_partitionID);
    dataBuffer.write(_hostName);
}

void ErrorResult::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_errorCode);
    dataBuffer.read(_errorMsg);
    dataBuffer.read(_partitionID);
    dataBuffer.read(_hostName);
}

END_HA3_NAMESPACE(common);
