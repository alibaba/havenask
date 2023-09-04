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
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "indexlib/indexlib.h"
#include "suez/turing/expression/util/IndexInfoHelper.h"

namespace suez {
namespace turing {
class IndexInfos;

class LegacyIndexInfoHelper : public IndexInfoHelper {
public:
    LegacyIndexInfoHelper();
    LegacyIndexInfoHelper(const suez::turing::IndexInfos *indexInfos);
    virtual ~LegacyIndexInfoHelper();

public:
    virtual indexid_t getIndexId(const std::string &indexName) const;
    virtual int32_t getFieldPosition(const std::string &indexName, const std::string &fieldname) const;
    virtual std::vector<std::string> getFieldList(const std::string &indexName) const;
    void setIndexInfos(const suez::turing::IndexInfos *indexInfos);

private:
    const IndexInfos *_indexInfos;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
