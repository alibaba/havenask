#ifndef ISEARCH_BS_LOCATOR_H
#define ISEARCH_BS_LOCATOR_H

#include <indexlib/common/index_locator.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace common {

typedef IE_NAMESPACE(common)::IndexLocator Locator;

extern const Locator INVALID_LOCATOR;

}
}

#endif //ISEARCH_BS_LOCATOR_H
