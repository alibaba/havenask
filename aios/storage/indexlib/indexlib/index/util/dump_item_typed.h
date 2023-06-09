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
#ifndef __INDEXLIB_DUMP_ITEM_H
#define __INDEXLIB_DUMP_ITEM_H

#include <memory>

#include "autil/WorkItem.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/common/dump_item.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index, IndexWriter);
DECLARE_REFERENCE_CLASS(index, AttributeWriter);
DECLARE_REFERENCE_CLASS(index, SummaryWriter);
DECLARE_REFERENCE_CLASS(index, SourceWriter);
DECLARE_REFERENCE_CLASS(index, KKVIndexDumperBase);
DECLARE_REFERENCE_CLASS(index, KVWriter);
DECLARE_REFERENCE_CLASS(index, AttributeUpdater);

namespace indexlib { namespace index {

template <typename WriterPointerType>
class DumpItemTyped : public common::DumpItem
{
public:
    DumpItemTyped(const file_system::DirectoryPtr& directory, const WriterPointerType& writer);
    ~DumpItemTyped();

public:
    void process(autil::mem_pool::PoolBase* pool) override;
    void destroy() override;
    void drop() override;

private:
    file_system::DirectoryPtr mDirectory;
    WriterPointerType mWriter;

private:
    IE_LOG_DECLARE();
};

template <typename WriterPointerType>
DumpItemTyped<WriterPointerType>::DumpItemTyped(const file_system::DirectoryPtr& directory,
                                                const WriterPointerType& writer)
    : mDirectory(directory)
    , mWriter(writer)
{
}

template <typename WriterPointerType>
DumpItemTyped<WriterPointerType>::~DumpItemTyped()
{
}

template <typename WriterPointerType>
void DumpItemTyped<WriterPointerType>::process(autil::mem_pool::PoolBase* pool)
{
    mWriter->Dump(mDirectory, pool);
}

template <typename WriterPointerType>
void DumpItemTyped<WriterPointerType>::destroy()
{
    delete this;
}

template <typename WriterPointerType>
void DumpItemTyped<WriterPointerType>::drop()
{
    destroy();
}

typedef DumpItemTyped<IndexWriter*> IndexDumpItem;
typedef DumpItemTyped<AttributeWriterPtr> AttributeDumpItem;
typedef DumpItemTyped<SummaryWriterPtr> SummaryDumpItem;
typedef DumpItemTyped<SourceWriterPtr> SourceDumpItem;
typedef DumpItemTyped<KKVIndexDumperBasePtr> KKVIndexDumpItem;
typedef DumpItemTyped<KVWriterPtr> KVIndexDumpItem;
}} // namespace indexlib::index

#endif //__INDEXLIB_DUMP_ITEM_H
