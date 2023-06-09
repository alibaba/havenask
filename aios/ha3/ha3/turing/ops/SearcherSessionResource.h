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

#include "suez/turing/common/SessionResource.h"

#include "ha3/config/ConfigAdapter.h"
#include "ha3/isearch.h"
#include "ha3/service/SearcherResource.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/util/NetFunction.h"

namespace suez {
namespace turing {
class LocalSearchService;
typedef std::shared_ptr<LocalSearchService> LocalSearchServicePtr;
}
}

namespace isearch {
namespace turing {

class SearcherSessionResource: public tensorflow::SessionResource
{
public:
    SearcherSessionResource(uint32_t count)
        : SessionResource(count)
    {
        if (cacheSnapshotTime < 0) {
            cacheSnapshotTime = 500 * 1000; // cache snapshot 500ms
        }
    }
    ~SearcherSessionResource(){}
private:
    SearcherSessionResource(const SearcherSessionResource &);
    SearcherSessionResource& operator=(const SearcherSessionResource &);
public:
    isearch::service::SearcherResourcePtr searcherResource;
    isearch::config::ConfigAdapterPtr configAdapter;
    std::string mainTableSchemaName;
    FullIndexVersion mainTableFullVersion = 0;
    std::string ip;
    suez::turing::LocalSearchServicePtr localSearchService;

public:
    uint32_t getIp() const {
        return util::NetFunction::encodeIp(ip);
    }

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherSessionResource> SearcherSessionResourcePtr;

} // namespace turing
} // namespace isearch
