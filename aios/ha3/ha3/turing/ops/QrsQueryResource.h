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

#include "suez/turing/common/QueryResource.h"

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace service {
class QrsSearchConfig;
}  // namespace service
}  // namespace isearch

namespace isearch {
namespace turing {

class QrsQueryResource: public suez::turing::QueryResource
{
public:
    QrsQueryResource()
        :_srcCount(1u)
    {};
    ~QrsQueryResource() {};
public:
    uint32_t _srcCount;
    service::QrsSearchConfig *qrsSearchConfig = nullptr;
private:
    QrsQueryResource(const QrsQueryResource &);
    QrsQueryResource& operator=(const QrsQueryResource &);
private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<QrsQueryResource> QrsQueryResourcePtr;

} // namespace turing
} // namespace isearch

