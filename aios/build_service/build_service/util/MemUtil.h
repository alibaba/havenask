#ifndef ISEARCH_BS_MEMUTIL_H
#define ISEARCH_BS_MEMUTIL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace util {

class MemUtil
{
public:
    static uint64_t getMachineTotalMem();
};

}
}

#endif //ISEARCH_BS_MEMUTIL_H
