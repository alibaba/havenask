/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "cava/common/Common.h"

namespace cava {

const std::hash<std::string> StringHasher;
const std::hash<int> IntHasher;

const std::string PREFIX_AST_REWRITER = "";
const std::string PREFIX_TYPE_BUILDER = "";
const std::string PREFIX_SEMA_CHECKER = "";
const std::string PREFIX_NOT_EXISTS = "";

const std::string CAVA_PACKAGE_NAME = "";
const std::string LANG_PACKAGE_NAME = "";
const std::string CSTRING_CLASS_NAME = "";

const std::string CAVACTX_FORMAL_NAME = "";
const std::string THIS_FORMAL_NAME = "";
const std::string CAVACTX_CLASS_NAME = "";
const std::string NULL_CLASS_NAME = "";
const std::string UNSAFE_PACKAGE_NAME = "";
const std::string ANY_CLASS_NAME = "";
const std::string CAVAARRAY_CLASS_NAME = "";
const std::string CAVACTX_EXCEPTION_NAME = "";
const std::string CAVACTX_STACK_DEPTH_NAME = "";
const std::string CAVACTX_RUN_TIMES_NAME = "";

const std::string CAVA_ALLOC_NAME = "";
const std::string ARRAY_LENGTH_FIELD_NAME = "";
const std::string ARRAY_DATA_FIELD_NAME = "";

const std::string CAVA_CXA_ATEXIT = "";
const std::string CAVA_DSO_HANDLE = "";
const std::string CAVA_DSO_HANDLE_CTOR = "";

const std::string CAVA_CTX_APPEND_STACK_INFO = "";
const std::string CAVA_CTX_CHECK_FORCE_STOP = "";
}
