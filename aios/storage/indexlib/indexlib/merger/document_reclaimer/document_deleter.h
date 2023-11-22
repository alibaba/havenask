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

#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class DocumentDeleter
{
public:
    DocumentDeleter(const config::IndexPartitionSchemaPtr& schema);
    ~DocumentDeleter();

public:
    void Init(const index_base::PartitionDataPtr& partitionData);
    bool Delete(docid_t docId);
    void Dump(const file_system::DirectoryPtr& directory);
    bool IsDirty() const;
    uint32_t GetMaxTTL() const;

private:
    void GetSubDocIdRange(docid_t mainDocId, docid_t& subDocIdBegin, docid_t& subDocIdEnd) const;

private:
    typedef std::shared_ptr<autil::mem_pool::Pool> PoolPtr;
    PoolPtr mPool;
    index_base::PartitionDataPtr mPartitionData;
    config::IndexPartitionSchemaPtr mSchema;
    index::DeletionMapWriterPtr mDeletionMapWriter;
    index::DeletionMapWriterPtr mSubDeletionMapWriter;
    index::JoinDocidAttributeReaderPtr mMainJoinAttributeReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentDeleter);
}} // namespace indexlib::merger
