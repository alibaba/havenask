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
#include "navi/common.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "turing_ops_util/variant/Tracer.h"

namespace sql {

class TracerAdapter : public suez::turing::Tracer {
public:
    TracerAdapter();
    ~TracerAdapter();
    TracerAdapter(const TracerAdapter &) = delete;
    TracerAdapter &operator=(const TracerAdapter &) = delete;

public:
    virtual void trace(const char *traceInfo) override;
    void setNaviLevel(navi::LogLevel level);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TracerAdapter> TracerAdapterPtr;
} // namespace sql
