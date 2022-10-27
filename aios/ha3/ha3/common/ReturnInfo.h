#ifndef ISEARCH_RETURNINFO_H
#define ISEARCH_RETURNINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ErrorDefine.h>

BEGIN_HA3_NAMESPACE(common);

class ReturnInfo
{
public:
    ReturnInfo(ErrorCode c = ERROR_NONE, const std::string &msg = "");
    ~ReturnInfo();
public:
    operator bool() const {
        return code == ERROR_NONE;
    }
    bool operator! () const {
        return code != ERROR_NONE;
    }
public:
    void fail(const std::string &message = "",
              ErrorCode c = ERROR_NORMAL_FAILED)
    {
        if (!message.empty()) {
            errorMsg += message;
        }
        code = c;
    }
public:
    ErrorCode code;
    std::string errorMsg;
};

#define HA3_LOG_AND_FAILD(ret, errorMsg)         \
    string errorMessage = errorMsg;              \
    ret.fail(errorMessage);                      \
    HA3_LOG(ERROR, "%s", errorMessage.c_str())

END_HA3_NAMESPACE(common);

#endif //ISEARCH_RETURNINFO_H
