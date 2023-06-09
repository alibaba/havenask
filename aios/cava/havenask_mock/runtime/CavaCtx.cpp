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
#include "cava/runtime/CavaCtx.h"

#include <stdio.h>
#include <sstream>

#include "autil/TimeUtility.h"

using namespace std;
using namespace cava;
using namespace autil;

namespace cava {
}

extern "C" {
    void _cava_ctx_append_stack_info(CavaCtx *ctx, std::string *file,
            int beginLineno, int beginColumn,
            int endLineno, int endLineColumn)
    {
    }

    void _cava_ctx_check_force_stop(CavaCtx *ctx)
    {
    }
}

