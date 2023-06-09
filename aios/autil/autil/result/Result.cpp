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
#include <sstream>
#include <deque>
#include <experimental/source_location>
#include <string>
#include <typeinfo>

#include "autil/Demangle.h"
#include "autil/result/Result.h"

namespace autil::result {

std::string Error::get_stack_trace() const {
    std::ostringstream oss;
    oss << get_runtime_type_name() << ": " << message() << "\n";
    _locs.for_each([&](auto &loc) {
        oss << "  -> " << loc.function_name() << "() [" << loc.file_name() << ":"
            << loc.line() << "]\n";
    });
    return oss.str();
}

std::string Error::get_runtime_type_name() const {
    return demangle(typeid(*this).name());
}

}    // namespace autil::result
