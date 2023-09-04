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

#include "autil/Diagnostic.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wsuggest-override")
#include <aitheta2/index_logger.h>
DIAGNOSTIC_POP

#include "autil/Log.h"

namespace indexlibv2::index::ann {

class CustomizedAiThetaLogger : public aitheta2::IndexLogger
{
public:
    CustomizedAiThetaLogger() {}
    ~CustomizedAiThetaLogger() {}

public:
    int init(const aitheta2::IndexParams& params) override;
    int cleanup(void) override;
    void log(int level, const char* file, int line, const char* format, va_list args) override;

public:
    static void RegisterIndexLogger();
    static constexpr const size_t BUFFER_SIZE = 8192;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
