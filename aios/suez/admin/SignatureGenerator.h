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

#include <stddef.h>
#include <string>
#include <vector>

#include "suez/sdk/JsonNodeRef.h"

namespace suez {

class SignatureGenerator {
public:
    static std::vector<std::string> defaultSignatureRule(bool tableIncNeedRolling,
                                                         bool bizConfigNeedRolling,
                                                         bool indexRootNeedRolling,
                                                         bool serviceInfoRolling);
    static bool generateSignature(const std::vector<std::string> &nodePaths,
                                  const JsonNodeRef::JsonMap *root,
                                  std::string &signatureStr);

private:
    static bool
    generateSignature(const std::string &nodePath, const JsonNodeRef::JsonMap *root, std::vector<std::string> &results);
    static bool generateSignature(const std::vector<std::string> &nodePath,
                                  size_t level,
                                  const JsonNodeRef::JsonMap *root,
                                  std::vector<std::string> &keyStack,
                                  std::vector<std::string> &results);

private:
    SignatureGenerator();
    ~SignatureGenerator();
};

} // namespace suez
