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
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/MountInfo.h"
#include "matchdoc/ValueType.h"

namespace autil {

namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace index {
class KKVIterator;
} // namespace index
} // namespace indexlib
namespace indexlibv2::config {
class ITabletSchema;
class ValueConfig;
} // namespace indexlibv2::config
namespace matchdoc {
class ReferenceBase;
} // namespace matchdoc

namespace isearch {
namespace sql {

////////////////////////////////////////////////////////////////////////
class KeyCollector {
public:
    KeyCollector();
    ~KeyCollector();

public:
    bool init(const std::shared_ptr<indexlibv2::config::ITabletSchema> &tableSchema,
              const matchdoc::MatchDocAllocatorPtr &mountedAllocator);
    
    matchdoc::BuiltinType getPkeyBuiltinType() const {
        return _pkeyBuiltinType;
    }
    matchdoc::ReferenceBase *getPkeyRef() {
        return _pkeyRef;
    }

private:
    bool extractInfo(const std::shared_ptr<indexlibv2::config::ITabletSchema> &tableSchema,
                     FieldType *pkFieldType,
                     std::string *pkFieldName,
                     std::shared_ptr<indexlibv2::config::ValueConfig> *valueConfig);
    bool isFieldInValue(const std::string &fieldName,
                        const std::shared_ptr<indexlibv2::config::ValueConfig> &valueConfig);

private:
    matchdoc::BuiltinType _pkeyBuiltinType;
    matchdoc::ReferenceBase *_pkeyRef;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<KeyCollector> KeyCollectorPtr;
////////////////////////////////////////////////////////////////////////
class ValueCollector {
private:
    typedef std::vector<matchdoc::ReferenceBase *> ValueReferences;

public:
    ValueCollector();
    ~ValueCollector();

public:
    bool init(const indexlib::config::IndexPartitionSchemaPtr &schema,
              const matchdoc::MatchDocAllocatorPtr &mountedAllocator);
    // v2 interface
    // only support kv and aggregation table
    bool init(const std::shared_ptr<indexlibv2::config::ValueConfig> &valueConfig,        
              const matchdoc::MatchDocAllocatorPtr &mountedAllocator,
              const std::string &tableType);
    
    // for kkv collect
    void collectFields(matchdoc::MatchDoc matchDoc, indexlib::index::KKVIterator *iter);
    // for kv collect
    void collectFields(matchdoc::MatchDoc matchDoc, const autil::StringView &value);

private:
    void setValue(matchdoc::MatchDoc doc, matchdoc::ReferenceBase *ref, const char *value);
private:
    matchdoc::ReferenceBase *_skeyRef;
    ValueReferences _valueRefs;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<ValueCollector> ValueCollectorPtr;
////////////////////////////////////////////////////////////////////////
class CollectorUtil {
public:
    static const std::string MOUNT_TABLE_GROUP;

public:
    static matchdoc::MatchDocAllocatorPtr
    createMountedMatchDocAllocator(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                                   const autil::mem_pool::PoolPtr &pool);

    static matchdoc::ReferenceBase *
    declareReference(const matchdoc::MatchDocAllocatorPtr &mountedAllocator,
                     FieldType fieldType,
                     bool isMulti,
                     const std::string &valueName,
                     const std::string &groupName = MOUNT_TABLE_GROUP);

    static void moveToNextVaildDoc(indexlib::index::KKVIterator *iter,
                                   matchdoc::MatchDoc &matchDoc,
                                   const matchdoc::MatchDocAllocatorPtr &allocator,
                                   const ValueCollectorPtr &valueCollector,
                                   int &matchDocCount);
    static bool isVaildMatchDoc(const matchdoc::MatchDoc &matchDoc) {
        return matchDoc.getDocId() != matchdoc::INVALID_MATCHDOC.getDocId();
    }

    static void insertPackAttributeMountInfo(
        const matchdoc::MountInfoPtr &singleMountInfo,
        const std::shared_ptr<indexlibv2::config::PackAttributeConfig> &packAttrConf,
        const std::string &tableName,
        uint32_t mountId);
public:
    CollectorUtil() {}
    ~CollectorUtil() {}

private:
    CollectorUtil(const CollectorUtil &);
    CollectorUtil &operator=(const CollectorUtil &);

private:
    static matchdoc::MountInfoPtr
    createPackAttributeMountInfo(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);
    static void
    insertPackAttributeMountInfo(const matchdoc::MountInfoPtr &singleMountInfo,
                                 const indexlib::config::IndexPartitionSchemaPtr &schema,
                                 const std::string &tableName,
                                 uint32_t &beginMountId);
    static void
    insertPackAttributeMountInfoV2(const matchdoc::MountInfoPtr &singleMountInfo,
                                   const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema,
                                   const std::string &tableName,
                                   uint32_t &beginMountId);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
} // namespace isearch
