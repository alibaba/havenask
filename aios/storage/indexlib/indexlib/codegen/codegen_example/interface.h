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
#ifndef __INDEXLIB_INTERFACE_H
#define __INDEXLIB_INTERFACE_H

#include <memory>

#include "indexlib/codegen/codegen_object.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace codegen {

class Interface : public CodegenObject
{
public:
    Interface();
    virtual ~Interface();

public:
    // void Init(const std::string& className) = 0;
    virtual void Run() = 0;
    virtual CodegenObject* CreateCodegenObject() = 0;
    virtual void Init(const std::string& name) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Interface);
}} // namespace indexlib::codegen

#endif //__INDEXLIB_INTERFACE_H
