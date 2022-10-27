#ifndef ISEARCH_FUNCTIONMAP_H
#define ISEARCH_FUNCTIONMAP_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/expression/function/FunctionMap.h>

BEGIN_HA3_NAMESPACE(config);

typedef suez::turing::FunctionProtoInfo FunctionProtoInfo; 
typedef std::map<std::string, FunctionProtoInfo> FuncInfoMap;
typedef std::map<std::string, FuncInfoMap> ClusterFuncMap;

END_HA3_NAMESPACE(config);

#endif //ISEARCH_FUNCTIONMAP_H
