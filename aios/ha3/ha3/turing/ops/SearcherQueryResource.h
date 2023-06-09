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

#include "suez/turing/common/QueryResource.h"

#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/search/SearcherCacheInfo.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace turing {

class Ha3BizMeta;

class SearcherQueryResource : public suez::turing::QueryResource
{
public:
    SearcherQueryResource() {}
    ~SearcherQueryResource() {}
private:
    SearcherQueryResource(const SearcherQueryResource &);
    SearcherQueryResource& operator=(const SearcherQueryResource &);
public:
    autil::TimeoutTerminator *getSeekTimeoutTerminator() const { return _seekTimeoutTerminator.get(); }
    void setSeekTimeoutTerminator(const autil::TimeoutTerminatorPtr &timeoutTerminator) {
        _seekTimeoutTerminator = timeoutTerminator;
    }

public:
    isearch::monitor::SessionMetricsCollector *sessionMetricsCollector = nullptr;
    std::vector<search::IndexPartitionReaderWrapperPtr> paraSeekReaderWrapperVec;
    search::IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper;
    search::SearchCommonResourcePtr commonResource;
    search::SearchPartitionResourcePtr partitionResource;
    search::SearchRuntimeResourcePtr runtimeResource;
    search::SearcherCacheInfoPtr searcherCacheInfo;
    Ha3BizMeta *ha3BizMeta = nullptr;

private:
    common::TimeoutTerminatorPtr _seekTimeoutTerminator;
private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<SearcherQueryResource> SearcherQueryResourcePtr;

} // namespace turing
} // namespace isearch
