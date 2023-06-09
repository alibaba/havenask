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

#include <map>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/turing/common/ModelConfig.h"

namespace tensorflow {
class TensorProto;
} // namespace tensorflow

namespace isearch {
namespace common {
class Query;
} // namespace common
} // namespace isearch
namespace suez {
namespace turing {
class Tracer;
}
} // namespace suez

namespace isearch {
namespace sql {
class AithetaQueryCreator {
public:
    static common::Query *createQuery(const std::string &modelBiz,
                                      const std::map<std::string, std::string> &kvPairs,
                                      const std::map<std::string, std::string> &embedding,
                                      isearch::turing::ModelConfigMap *modelConfigMap);
    static bool genAithetaQuery(const tensorflow::TensorProto &tensorProto,
                                std::vector<std::string> &tensorStrs,
                                suez::turing::Tracer *tracer = nullptr);

private:
    static std::vector<std::string> makeQuery(const std::map<std::string, std::string> &kvPairs,
                                              const std::string &modelBiz,
                                              const std::vector<std::string> &tensorStrs);
    static void reset(std::vector<common::Query *> &allQuery);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
} // namespace isearch
