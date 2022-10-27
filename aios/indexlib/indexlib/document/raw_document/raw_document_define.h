#ifndef __INDEXLIB_RAW_DOCUMENTDEFINE_H
#define __INDEXLIB_RAW_DOCUMENTDEFINE_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(document);

static const std::string CMD_TAG = "CMD";
static const std::string CMD_ADD = "add";
static const std::string CMD_DELETE = "delete";
static const std::string CMD_DELETE_SUB = "delete_sub";
static const std::string CMD_UPDATE_FIELD = "update_field";
static const std::string CMD_SKIP = "skip";

static const std::string BUILTIN_KEY_TRACE_ID = "__HA3_DOC_TRACE_ID__";

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_RAW_DOCUMENTDEFINE_H
