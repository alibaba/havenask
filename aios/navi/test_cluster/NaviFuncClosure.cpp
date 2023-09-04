#include "navi/test_cluster/NaviFuncClosure.h"


namespace navi {

NaviFuncClosure::NaviFuncClosure(std::function<void(NaviUserResultPtr)> func)
    : _func(std::move(func)) {}

void NaviFuncClosure::run(NaviUserResultPtr userResult) {
    _func(std::move(userResult));
}

}
