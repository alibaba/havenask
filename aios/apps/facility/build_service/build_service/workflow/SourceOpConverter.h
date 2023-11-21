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

#include "build_service/common_define.h"
#include "build_service/config/SrcNodeConfig.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"

namespace build_service { namespace workflow {

class SourceOpConverter
{
public:
    SourceOpConverter(config::SrcNodeConfig* config);
    ~SourceOpConverter();
    enum SolvedDocType {
        SDT_UNKNOWN = 0,
        SDT_PROCESSED_DOC = 1,
        SDT_UNPROCESSED_DOC = 2,
        SDT_ORDER_PRESERVE_FAILED = 3,
        SDT_UPDATE_FAILED = 4,
        SDT_FAILED_DOC = 5,
    };

private:
    SourceOpConverter(const SourceOpConverter&);
    SourceOpConverter& operator=(const SourceOpConverter&);

public:
    SolvedDocType convert(const DocOperateType& docOp) const noexcept;

private:
    config::SrcNodeConfig* _config;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SourceOpConverter);

}} // namespace build_service::workflow
