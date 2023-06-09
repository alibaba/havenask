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

#include "autil/StringView.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::index {
class PackAttributeFormatter;
}

namespace suez {

// maybe move to indexlib
class BinaryRecord {
public:
    BinaryRecord(indexlibv2::index::PackAttributeFormatter *packAttrFormatter,
                 autil::mem_pool::Pool *pool,
                 const autil::StringView &data);
    ~BinaryRecord();

public:
    size_t getFieldCount() const;
    autil::StringView getFieldName(size_t idx) const;
    bool getFieldValue(size_t idx, std::string &value) const;

private:
    indexlibv2::index::PackAttributeFormatter *_packAttrFormatter;
    autil::mem_pool::Pool *_pool;
    autil::StringView _data;
};

} // namespace suez
