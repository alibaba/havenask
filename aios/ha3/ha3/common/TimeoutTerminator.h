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

#include <memory>
#include <string>

#include "autil/TimeoutTerminator.h"
#include "autil/result/Result.h"

namespace isearch {
namespace common {

typedef autil::TimeoutTerminator TimeoutTerminator;
typedef autil::TimeoutTerminatorPtr TimeoutTerminatorPtr;

class TimeoutError : public autil::result::Error {
public:
    std::string operation;

    TimeoutError(std::string &&operation)
        : operation {std::move(operation)} {}

    std::unique_ptr<autil::result::Error> clone() const override {
        return std::make_unique<TimeoutError>(*this);
    }

    std::string message() const override {
        return "timeout at operation: [" + operation + "]";
    }

    static std::unique_ptr<autil::result::Error> make(std::string &&operation) {
        return std::make_unique<TimeoutError>(std::move(operation));
    }
};

} //namespace common
} //namespace isearch
