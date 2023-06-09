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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/qrs/QrsProcessor.h"

namespace isearch {
namespace qrs {

class CheckSummaryProcessor : public QrsProcessor
{
public:
    CheckSummaryProcessor();
    ~CheckSummaryProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr,
                         common::ResultPtr &resultPtr);

    virtual QrsProcessorPtr clone();
public:
    std::string getName() const {
        return "CheckSummaryProcessor";
    }
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CheckSummaryProcessor> CheckSummaryProcessorPtr;

} // namespace qrs
} // namespace isearch
