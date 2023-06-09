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
#pragma once

#include <stddef.h>
#include <string>
#include <vector>

#include "autil/ConstString.h"
#include "cava/common/Common.h"
#include "cava/runtime/CavaArrayType.h"

class CavaCtx;

namespace cava {
namespace lang {

// the string canot be changed
class CString
{
public: 
    CString(CavaByteArray data, std::string *str) {

    } 
    std::string getStr() {
        return "";
    }
    int size() { return 0; }
    char *data() { return nullptr; }
private:
    CString(const CString &);
    CString& operator=(CString &);
public:
    // internal use
    byte *getData() { return nullptr; }
    int getOffset() { return 0;}

};


} // end namespace lang
} // end namespace cava
