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
#ifndef ISEARCH_EXPRESSION_TYPEINFO_H
#define ISEARCH_EXPRESSION_TYPEINFO_H

namespace expression {

enum ExpressionType {
    ET_UNKNOWN = 0,
    ET_ATOMIC = 1,
    ET_ARGUMENT = 2,
    ET_BINARY = 3,
    ET_FUNCTION = 4,
    ET_CONST = 5,
    ET_ANY = 6,
    ET_INPACK = 7,
    ET_PACK = 8,
    ET_CONTEXT = 9,
    ET_REFERENCE = 10
};

enum ExprValueType {
    EVT_UNKNOWN = 0,
    EVT_INT8 = 1,
    EVT_UINT8 = 2,
    EVT_INT16 = 3,
    EVT_UINT16 = 4,
    EVT_INT32 = 5,
    EVT_UINT32 = 6,
    EVT_INT64 = 7,
    EVT_UINT64 = 8,
    EVT_FLOAT = 9,
    EVT_DOUBLE = 10,
    EVT_STRING = 11,
    EVT_BOOL = 12, // for logical operator
    EVT_VOID = 13
};

#define NUMERIC_EXPR_VALUE_TYPE_MACRO_HELPER(MY_MACRO)         \
    MY_MACRO(EVT_INT8);                                        \
    MY_MACRO(EVT_INT16);                                       \
    MY_MACRO(EVT_INT32);                                       \
    MY_MACRO(EVT_INT64);                                       \
    MY_MACRO(EVT_UINT8);                                       \
    MY_MACRO(EVT_UINT16);                                      \
    MY_MACRO(EVT_UINT32);                                      \
    MY_MACRO(EVT_UINT64);                                      \
    MY_MACRO(EVT_FLOAT);                                       \
    MY_MACRO(EVT_DOUBLE)

#define NUMERIC_EXPR_VALUE_TYPE_MACRO_HELPER_WITH_ARGS(MY_MACRO, args...) \
    MY_MACRO(EVT_INT8, ##args);                                           \
    MY_MACRO(EVT_INT16, ##args);                                          \
    MY_MACRO(EVT_INT32, ##args);                                          \
    MY_MACRO(EVT_INT64, ##args);                                          \
    MY_MACRO(EVT_UINT8, ##args);                                          \
    MY_MACRO(EVT_UINT16, ##args);                                         \
    MY_MACRO(EVT_UINT32, ##args);                                         \
    MY_MACRO(EVT_UINT64, ##args);                                         \
    MY_MACRO(EVT_FLOAT, ##args);                                          \
    MY_MACRO(EVT_DOUBLE, ##args)

#define NUMERIC_EXPR_VALUE_TYPE_MACRO_WITH_STRING_HELPER(MY_MACRO)      \
    NUMERIC_EXPR_VALUE_TYPE_MACRO_HELPER(MY_MACRO);                     \
    MY_MACRO(EVT_STRING)    

#define EXPR_VALUE_TYPE_MACRO_HELPER(MY_MACRO)          \
    NUMERIC_EXPR_VALUE_TYPE_MACRO_HELPER(MY_MACRO);     \
    MY_MACRO(EVT_BOOL);                                 \
    MY_MACRO(EVT_STRING)

}

#endif //ISEARCH_EXPRESSION_TYPEINFO_H
