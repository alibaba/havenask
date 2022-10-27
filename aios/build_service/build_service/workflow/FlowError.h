#ifndef ISEARCH_BS_FLOWERROR_H
#define ISEARCH_BS_FLOWERROR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace workflow {

enum FlowError {
    FE_OK = 0,
    FE_EXCEPTION = 1,
    FE_EOF = 2,
    FE_WAIT = 3,
    FE_FATAL = 4,
    FE_SKIP = 5
};

}
}

#endif //ISEARCH_BS_FLOWERROR_H
