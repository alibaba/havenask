#ifndef ISEARCH_FUNCCONFIG_H
#define ISEARCH_FUNCCONFIG_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>
#include <build_service/plugin/ModuleInfo.h>
#include <ha3/config/FunctionInfo.h>
#include <suez/turing/expression/function/FuncConfig.h>
BEGIN_HA3_NAMESPACE(config);

typedef suez::turing::FuncConfig FuncConfig; 

END_HA3_NAMESPACE(config);

#endif //ISEARCH_FUNCCONFIG_H
