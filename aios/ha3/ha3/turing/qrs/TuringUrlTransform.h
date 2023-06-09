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
#include <string>

#include "autil/Log.h" // IWYU pragma: keep

namespace suez {
namespace turing {
class GraphRequest;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace turing {

class TuringUrlTransform
{
public:
    bool init();
    suez::turing::GraphRequest* transform(const std::string& url) const;

    void setBizNameKey(const std::string& key);
    const std::string& getBizNameKey() const;

    void setSrcKey(const std::string& key);
    const std::string& getSrcKey() const;

    void setTimeout(int64_t t);
    int64_t getTimeout() const;
private:
    std::string _bizNameKey{"s"};
    std::string _srcKey{"src"};
    int64_t _timeout{500};
private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch

