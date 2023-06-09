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
#ifndef __AITHETA_PLUGIN_CUSTOM_LOGGER_H__
#define __AITHETA_PLUGIN_CUSTOM_LOGGER_H__
#include <aitheta/index_logger.h>
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"

namespace indexlib {
namespace aitheta_plugin {

class CustomLogger : public aitheta::IndexLogger {
 public:
    CustomLogger() {}
    ~CustomLogger() {}

 public:
    int init(const aitheta::LoggerParams &params) override;

    //! Cleanup Logger
    int cleanup(void) override;

    //! Log Message
    void log(int level, const char *file, int line, const char *format, va_list args) override;

 private:
    IE_LOG_DECLARE();
};

void RegisterGlobalIndexLogger();

}
}

#endif  // __AITHETA_PLUGIN_CUSTOM_LOGGER_H__
