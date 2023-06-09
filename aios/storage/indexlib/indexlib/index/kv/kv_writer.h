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
#ifndef __INDEXLIB_KV_WRITER_H
#define __INDEXLIB_KV_WRITER_H

#include <map>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, AttributeWriter);
DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);
DECLARE_REFERENCE_CLASS(framework, SegmentGroupMetrics);
namespace autil { namespace mem_pool {
class PoolBase;
}} // namespace autil::mem_pool

namespace indexlib { namespace index {

class KVWriter
{
public:
    KVWriter() : mCompressRatio(1) {}

    virtual ~KVWriter() {}

public:
    virtual void Init(const config::KVIndexConfigPtr& kvIndexConfig, const file_system::DirectoryPtr& segmentDir,
                      bool isOnline, uint64_t maxMemoryUse,
                      const std::shared_ptr<framework::SegmentGroupMetrics>& groupMetrics) = 0;
    virtual bool Add(const keytype_t& key, const autil::StringView& value, uint32_t timestamp, bool isDeleted,
                     regionid_t regionId) = 0;
    virtual void Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* pool) = 0;

    virtual void FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                    const std::string& groupName) = 0;

    virtual size_t GetDumpTempMemUse() const { return 0; }

    // NOTE: memory allocated by file system
    // so return 0 for fix or no-compress var writer
    virtual size_t GetDumpFileExSize() const { return 0; }

    void SetValueCompressRatio(double ratio) { mCompressRatio = ratio; }

    virtual void SetHashKeyValueMap(std::map<index::keytype_t, autil::StringView>* hashKeyValueMap) = 0;
    virtual void SetAttributeWriter(index::AttributeWriterPtr& writer) = 0;

public:
    virtual size_t GetMemoryUse() const = 0;
    virtual bool IsFull() const = 0;

protected:
    double mCompressRatio;
};

DEFINE_SHARED_PTR(KVWriter);
}} // namespace indexlib::index

#endif //__INDEXLIB_KV_WRITER_H
