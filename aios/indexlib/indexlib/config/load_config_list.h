#ifndef __INDEXLIB_LOAD_CONFIG_LIST_H
#define __INDEXLIB_LOAD_CONFIG_LIST_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/load_config/load_config_list.h"

IE_NAMESPACE_BEGIN(config);

typedef file_system::LoadConfigList LoadConfigList;
DEFINE_SHARED_PTR(LoadConfigList);

typedef file_system::LoadConfig LoadConfig;
DEFINE_SHARED_PTR(LoadConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_LOAD_CONFIG_LIST_H
