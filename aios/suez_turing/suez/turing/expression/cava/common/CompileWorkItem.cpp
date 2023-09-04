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
#include "suez/turing/expression/cava/common/CompileWorkItem.h"

#include <iosfwd>
#include <memory>

#include "autil/Log.h"
#include "cava/codegen/CavaJitModule.h"
#include "suez/turing/expression/cava/common/CavaJitWrapper.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, CompileWorkItem);

namespace suez {
namespace turing {

void SingleCompileWorkItem::process() {
    _cavaJitWrapper->compile(_moduleName, {_fileName}, {_code}, _hashKey, _options.get());
}

void CompileWorkItem::process() {
    auto cavaJitModule = _cavaJitWrapper->compile(_moduleName, _fileNames, _codes, _hashKey, _options.get(), _tracer);
    if (cavaJitModule && _cavaJitModulesWrapper) {
        _cavaJitModulesWrapper->addCavaJitModule(_hashKey, cavaJitModule);
    }
    _closure->run();
}

} // namespace turing
} // namespace suez
