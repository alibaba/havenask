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

#include "indexlib/index/primary_key/PrimaryKeyPair.h"
#include "indexlib/util/HashMap.h"

namespace indexlibv2 { namespace index {

template <typename Key>
class PrimaryKeyFileWriter
{
public:
    typedef indexlib::util::HashMap<Key, docid_t> HashMapType;
    typedef std::shared_ptr<HashMapType> HashMapTypePtr;

public:
    PrimaryKeyFileWriter() {}
    virtual ~PrimaryKeyFileWriter() {}

public:
    virtual void Init(size_t docCount, size_t pkCount, const std::shared_ptr<indexlib::file_system::FileWriter>& file,
                      autil::mem_pool::PoolBase* pool) = 0;
    virtual Status AddPKPair(Key key, docid_t docid) = 0;
    virtual Status AddSortedPKPair(Key key, docid_t docid) = 0;
    virtual Status Close() = 0;
    virtual int64_t EstimateDumpTempMemoryUse(size_t docCount) = 0;

private:
    AUTIL_LOG_DECLARE();
};
}} // namespace indexlibv2::index
