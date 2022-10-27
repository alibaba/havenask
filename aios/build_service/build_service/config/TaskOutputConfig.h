#ifndef ISEARCH_BS_TASKOUTPUTCONFIG_H
#define ISEARCH_BS_TASKOUTPUTCONFIG_H

#include <autil/legacy/jsonizable.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/TaskInputConfig.h"

namespace build_service {
namespace config {

using TaskOutputConfig = TaskInputConfig;
BS_TYPEDEF_PTR(TaskOutputConfig);

}
}

#endif //ISEARCH_BS_TASKOUTPUTCONFIG_H
