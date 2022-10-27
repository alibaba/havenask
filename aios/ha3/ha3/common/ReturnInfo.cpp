#include <ha3/common/ReturnInfo.h>

BEGIN_HA3_NAMESPACE(common);

ReturnInfo::ReturnInfo(ErrorCode c, const std::string &msg)
    : code(c)
    , errorMsg(msg)
{
}

ReturnInfo::~ReturnInfo() { 
}

END_HA3_NAMESPACE(common);

