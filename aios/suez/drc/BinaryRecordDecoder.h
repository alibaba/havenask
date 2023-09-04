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

#include "autil/StringView.h"
#include "suez/drc/BinaryRecord.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::index {
class PackAttributeFormatter;
}

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace suez {

class BinaryRecordDecoder {
public:
    BinaryRecordDecoder();
    ~BinaryRecordDecoder();

public:
    bool init(const indexlibv2::config::KVIndexConfig &indexConfig);
    BinaryRecord decode(const autil::StringView &data, autil::mem_pool::Pool *pool);

private:
    std::unique_ptr<indexlibv2::index::PackAttributeFormatter> _packAttrFormatter;
};

} // namespace suez
