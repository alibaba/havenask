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
#include <string>

#include "autil/Log.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "indexlib/base/FieldType.h"

namespace indexlibv2 {
namespace config {
class ITabletSchema;
}
}

namespace isearch {
namespace sql {

class AggIndexKeyCollector {
public:
    AggIndexKeyCollector();
    ~AggIndexKeyCollector();

public:
    bool init(const std::string &indexName,
              const std::shared_ptr<indexlibv2::config::ITabletSchema> &tableSchema,
              const matchdoc::MatchDocAllocatorPtr &mountedAllocator);

    size_t count() const;
    matchdoc::ReferenceBase* getRef(size_t idx) const;
    FieldType getFieldType(size_t idx) const;
    
private:
    std::vector<matchdoc::ReferenceBase*> _refs;
    std::vector<FieldType> _types;
    
private:
    AUTIL_LOG_DECLARE();
};

}
}
