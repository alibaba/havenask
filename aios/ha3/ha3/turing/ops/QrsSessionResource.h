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

#include <stdint.h>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/plugin/SorterManager.h"

namespace isearch {
namespace turing {

class QrsSessionResource: public tensorflow::SessionResource
{
public:
    QrsSessionResource(uint32_t count)
        : SessionResource(count){};
    ~QrsSessionResource() {};
private:
    QrsSessionResource(const QrsSessionResource &);
    QrsSessionResource& operator=(const QrsSessionResource &);
public:
    suez::turing::ClusterSorterManagersPtr _clusterSorterManagers;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QrsSessionResource> QrsSessionResourcePtr;

} // namespace turing
} // namespace isearch

