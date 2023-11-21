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
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "indexlib/indexlib.h"
#include "indexlib/table/normal_table/NormalTabletReader.h"


namespace suez {
namespace turing {
class FieldMetaReaderWrapper;
}
}

namespace indexlib {
namespace index {
class FieldMetaReader;
}
}

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
}


typedef std::shared_ptr<indexlib::index::FieldMetaReader> FieldMetaReaderPtr;
typedef std::shared_ptr<std::map<std::string, FieldMetaReaderPtr>> FieldMetaReadersMapPtr;

namespace suez {
namespace turing {

class FieldMetaReaderWrapper {
public:
    // 为了性能，在一次sql查询中保存已经查询过的meta信息
    FieldMetaReaderWrapper(autil::mem_pool::Pool *pool, FieldMetaReadersMapPtr fieldMetaReadersMap);
    virtual ~FieldMetaReaderWrapper() {}

public:
    bool getFieldLengthOfDoc(int64_t docId, int64_t& totalFieldLength, std::string fieldName);
    bool getFieldAvgLength(double& avgFieldLength, std::string fieldName);

private:
    autil::mem_pool::Pool *_pool;
    FieldMetaReadersMapPtr _fieldMetaReadersMapPtr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<suez::turing::FieldMetaReaderWrapper> FieldMetaReaderWrapperPtr;

} // namespace turing
} // namespace suez
