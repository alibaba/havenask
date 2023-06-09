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

#include "autil/Log.h"

namespace isearch {
namespace sql {
class ScanResource;
class SqlBizResource;
class SqlQueryResource;
class MetaInfoResource;
class ObjectPoolResource;
class SqlConfigResource;
class TabletManagerR;
} // namespace sql
} // namespace isearch
namespace navi {
class GraphMemoryPoolResource;
class GigQuerySessionR;
} // namespace navi

namespace isearch {
namespace sql {

class ScanKernelUtil {
public:
    ScanKernelUtil();
    ~ScanKernelUtil();

private:
    ScanKernelUtil(const ScanKernelUtil &);
    ScanKernelUtil &operator=(const ScanKernelUtil &);

public:
    static bool convertResource(SqlBizResource *sqlBizResource,
                                SqlQueryResource *sqlQueryResource,
                                navi::GraphMemoryPoolResource *memoryPoolResource,
                                navi::GigQuerySessionR *naviQuerySessionR,
                                ObjectPoolResource *objectPoolResource,
                                MetaInfoResource *metaInfoResource,
                                ScanResource &scanResource,
                                SqlConfigResource *sqlConfigResource = nullptr,
                                TabletManagerR *tabletManagerR = nullptr);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ScanKernelUtil> ScanKernelUtilPtr;
} // namespace sql
} // namespace isearch
