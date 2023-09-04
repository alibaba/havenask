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
#include "autil/Log.h"
#include "sql/ops/scan/udf_to_query/UdfToQueryCreatorFactory.h"

namespace autil {
class InterfaceManager;
} // namespace autil

namespace sql {

class BuiltinUdfToQueryCreatorFactory : public UdfToQueryCreatorFactory {
public:
    BuiltinUdfToQueryCreatorFactory() {}
    ~BuiltinUdfToQueryCreatorFactory() {}

public:
    bool registerUdfToQuery(autil::InterfaceManager *manager) override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
