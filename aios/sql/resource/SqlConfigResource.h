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
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/config/SwiftWriteConfig.h"
#include "sql/config/TableWriteConfig.h"
#include "sql/resource/SqlConfig.h"

namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

class SqlConfigResource : public navi::Resource {
public:
    SqlConfigResource();
    SqlConfigResource(const SqlConfig &sqlConfig);
    SqlConfigResource(const SqlConfig &sqlConfig,
                      const SwiftWriteConfig &swiftWriterConfig,
                      const TableWriteConfig &tableWriterConfig);
    ~SqlConfigResource() = default;
    SqlConfigResource(const SqlConfigResource &) = delete;
    SqlConfigResource &operator=(const SqlConfigResource &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    const SqlConfig &getSqlConfig() const {
        return _sqlConfig;
    }
    const SwiftWriteConfig &getSwiftWriterConfig() const {
        return _swiftWriterConfig;
    }
    const TableWriteConfig &getTableWriterConfig() const {
        return _tableWriterConfig;
    }

public:
    static const std::string RESOURCE_ID;

private:
    SqlConfig _sqlConfig;
    SwiftWriteConfig _swiftWriterConfig;
    TableWriteConfig _tableWriterConfig;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlConfigResource> SqlConfigResourcePtr;

} // namespace sql
