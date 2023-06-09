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

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/common/KeyHasherWrapper.h"
#include "indexlib/index/common/PrimaryKeyHashType.h"
#include "indexlib/index/operation_log/OperationLogConfig.h"
#include "indexlib/index/primary_key/PrimaryKeyHashConvertor.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/util/HashString.h"

namespace indexlib::index {
class OperationBase;

class OperationCreator
{
public:
    OperationCreator(const std::shared_ptr<OperationLogConfig>& opConfig);
    virtual ~OperationCreator();

public:
    virtual bool Create(const indexlibv2::document::NormalDocument* doc, autil::mem_pool::Pool* pool,
                        OperationBase** operation) = 0;

protected:
    InvertedIndexType GetPkIndexType() const;
    void GetPkHash(const std::shared_ptr<document::IndexDocument>& indexDoc, autil::uint128_t& pkHash);

protected:
    FieldType _fieldType;
    PrimaryKeyHashType _primaryKeyHashType;
    std::shared_ptr<OperationLogConfig> _config;

private:
    friend class OperationFactoryTest;

private:
    AUTIL_LOG_DECLARE();
};

inline OperationCreator::OperationCreator(const std::shared_ptr<OperationLogConfig>& opConfig) : _config(opConfig)
{
    auto pkConfig =
        std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(_config->GetPrimaryKeyIndexConfig());
    assert(pkConfig);
    auto fieldConfig = pkConfig->GetFieldConfig();
    _fieldType = fieldConfig->GetFieldType();
    _primaryKeyHashType = pkConfig->GetPrimaryKeyHashType();
}

inline OperationCreator::~OperationCreator() {}

inline InvertedIndexType OperationCreator::GetPkIndexType() const
{
    assert(_config->GetPrimaryKeyIndexConfig());

    InvertedIndexType pkFieldType = _config->GetPrimaryKeyIndexConfig()->GetInvertedIndexType();
    assert(pkFieldType == it_primarykey64 || pkFieldType == it_primarykey128);
    return pkFieldType;
}

inline void OperationCreator::GetPkHash(const std::shared_ptr<document::IndexDocument>& indexDoc,
                                        autil::uint128_t& pkHash)
{
    assert(indexDoc);
    assert(!indexDoc->GetPrimaryKey().empty());
    const std::string& pkString = indexDoc->GetPrimaryKey();

    InvertedIndexType pkFieldType = GetPkIndexType();
    if (pkFieldType == it_primarykey64) {
        uint64_t hashValue;
        KeyHasherWrapper::GetHashKey(_fieldType, _primaryKeyHashType, pkString.c_str(), pkString.size(), hashValue);
        PrimaryKeyHashConvertor::ToUInt128(hashValue, pkHash);
    } else {
        KeyHasherWrapper::GetHashKey(pkString.c_str(), pkString.size(), pkHash);
    }
}

} // namespace indexlib::index
