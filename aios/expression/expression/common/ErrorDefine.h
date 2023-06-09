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
#ifndef ISEARCH_EXPRESSION_ERRORDEFINE_H
#define ISEARCH_EXPRESSION_ERRORDEFINE_H

#include "expression/common.h"

namespace expression {

enum ErrorCode {
    ERROR_NONE,
    ERROR_ATTRIBUTE_NOT_EXIST,
    ERROR_AMBIGUOUS_ATTRIBUTE,
    ERROR_EXPRESSION_LR_ALL_CONST_VALUE,
    ERROR_CONST_EXPRESSION_TYPE,
    ERROR_EXPRESSION_LR_TYPE_NOT_MATCH,
    ERROR_LOGICAL_EXPR_LR_TYPE_ERROR,
    ERROR_CONST_EXPRESSION_VALUE,
    ERROR_SUB_DOC_NOT_SUPPORT,
    ERROR_EXPRESSION_WITH_MULTI_VALUE,
    ERROR_FUNCTION_NOT_DEFINED,
    ERROR_CREATE_FUNCTION_FAIL,
    ERROR_FUNCTION_ARGCOUNT,
    ERROR_BIT_UNSUPPORT_TYPE_ERROR,
};

}

#endif //ISEARCH_EXPRESSION_ERRORDEFINE_H
