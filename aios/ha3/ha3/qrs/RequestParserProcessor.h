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

#include <string>

#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/qrs/QrsProcessor.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace qrs {

class RequestParserProcessor : public QrsProcessor
{
public:
    RequestParserProcessor(const config::ClusterConfigMapPtr &clusterConfigMapPtr);
    virtual ~RequestParserProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr, 
                         common::ResultPtr &resultPtr);
    virtual QrsProcessorPtr clone();
public:
    std::string getName() const {
        return "RequestParserProcessor";
    }
private:
    config::ClusterConfigMapPtr _clusterConfigMapPtr;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace qrs
} // namespace isearch

