#pragma once

#include "navi/engine/NaviUserResult.h"

namespace navi {

class NaviFuncClosure : public NaviUserResultClosure {
public:
    NaviFuncClosure(std::function<void(NaviUserResultPtr)> func);
public:
    void run(NaviUserResultPtr userResult) override;
private:
    std::function<void(NaviUserResultPtr)> _func;
};

}
