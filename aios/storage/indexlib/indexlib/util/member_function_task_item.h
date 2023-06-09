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
#ifndef __INDEXLIB_MEMBER_FUNCTION_TASK_ITEM_H
#define __INDEXLIB_MEMBER_FUNCTION_TASK_ITEM_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/TaskItem.h"

namespace indexlib { namespace util {

template <class X, void (X::*p)()>
class MemberFunctionTaskItem : public TaskItem
{
public:
    MemberFunctionTaskItem(X& ref) : mRef(ref) {}
    ~MemberFunctionTaskItem() {}

public:
    void Run() override { (mRef.*p)(); }

private:
    X& mRef;
};
}} // namespace indexlib::util

#endif //__INDEXLIB_MEMBER_FUNCTION_TASK_ITEM_H
