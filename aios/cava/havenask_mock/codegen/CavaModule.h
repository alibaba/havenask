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

#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/Transforms/Utils/ValueMapper.h>
#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <deque>
#include <list>
#include <memory>
#include <set>
#include <unordered_set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "cava/ast/ASTContext.h"
#include "cava/common/Common.h"

namespace llvm {
class LLVMContext;
}  // namespace llvm

namespace cava {

enum CavaModuleState {
    CMS_UNKNOWN,
};


class CavaModule
{
public:
    ASTContext &getASTContext() { return _astCtx; }
private:
    CavaModule(const CavaModule &);
    CavaModule& operator=(CavaModule &);
    ASTContext _astCtx;

};


} // end namespace cava
