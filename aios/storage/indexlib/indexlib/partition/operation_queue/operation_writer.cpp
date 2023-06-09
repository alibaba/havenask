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
#include "indexlib/partition/operation_queue/operation_writer.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/util/dump_item_typed.h"
#include "indexlib/partition/operation_queue/operation_dumper.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::document;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OperationWriter);

OperationWriter::OperationWriter()
    : mMaxBlockSize(0)
    , mReleaseBlockAfterDump(true)
    , mEnableFlushToDisk(false)
    , mBuildResourceMetricsNode(NULL)
{
}

void OperationWriter::Init(const IndexPartitionSchemaPtr& schema, size_t maxBlockSize, bool enableFlushToDisk)
{
    mSchema = schema;
    mMaxBlockSize = maxBlockSize;
    mReleaseBlockAfterDump = true;
    mEnableFlushToDisk = enableFlushToDisk;
    mBuildResourceMetrics.reset(new util::BuildResourceMetrics());
    mBuildResourceMetrics->Init();
    mBuildResourceMetricsNode = mBuildResourceMetrics->AllocateNode();
    mOperationFactory = CreateOperationFactory(mSchema);
    CreateNewBlock(mMaxBlockSize);
}

bool OperationWriter::AddOperation(const DocumentPtr& document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    if (!mSchema->GetIndexSchema()->HasPrimaryKeyIndex()) {
        return true;
    }
    InvertedIndexType pkIndexType = mSchema->GetIndexSchema()->GetPrimaryKeyIndexType();
    if (pkIndexType != it_primarykey64 && pkIndexType != it_primarykey128) {
        return true;
    }

    if (!doc->HasPrimaryKey()) {
        IE_LOG(DEBUG, "doc has no primary key when schema has primary key index");
        return false;
    }

    ScopedLock lock(mDataLock);
    OperationBase* op = nullptr;
    if (!mOperationFactory->CreateOperation(doc, mBlockInfo.GetPool(), &op)) {
        return false;
    }
    DoAddOperation(op);
    return true;
}

bool OperationWriter::DoAddOperation(OperationBase* operation)
{
    if (unlikely(!operation)) {
        return false;
    }

    size_t opDumpSize = OperationDumper::GetDumpSize(operation);
    mOperationMeta.Update(opDumpSize);

    assert(mBlockInfo.mCurBlock);
    assert(mBlockInfo.mCurBlock->Size() < mMaxBlockSize);

    mBlockInfo.mCurBlock->AddOperation(operation);
    if (NeedCreateNewBlock()) {
        IE_LOG(DEBUG, "Need CreateNewBlock, current block count: %lu", mOpBlocks.size());
        assert(!mOpBlocks.empty());
        CreateNewBlock(mMaxBlockSize);
    }
    UpdateBuildResourceMetrics();
    return true;
}

void OperationWriter::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode) {
        return;
    }
    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, GetTotalMemoryUse());
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, GetMaxOperationSerializeSize());
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, GetDumpSize());
}

void OperationWriter::CreateNewBlock(size_t maxBlockSize)
{
    FlushBuildingOperationBlock();
    OperationBlockPtr newBlock(new OperationBlock(maxBlockSize));
    mOpBlocks.push_back(newBlock);
    ResetBlockInfo(mOpBlocks);
}

void OperationWriter::ResetBlockInfo(const OperationBlockVec& opBlocks)
{
    assert(!opBlocks.empty());
    size_t noLastBlockOpCount = 0;
    size_t noLastBlockMemUse = 0;
    for (size_t i = 0; i < mOpBlocks.size() - 1; ++i) {
        noLastBlockOpCount += mOpBlocks[i]->Size();
        noLastBlockMemUse += mOpBlocks[i]->GetTotalMemoryUse();
    }

    OperationBlockInfo blockInfo(noLastBlockOpCount, noLastBlockMemUse, *opBlocks.rbegin());
    mBlockInfo = blockInfo;
}

OperationFactoryPtr OperationWriter::CreateOperationFactory(const IndexPartitionSchemaPtr& schema)
{
    OperationFactoryPtr opFactory(new OperationFactory);
    opFactory->Init(schema);
    return opFactory;
}

void OperationWriter::CreateDumpItems(const DirectoryPtr& directory,
                                      vector<std::unique_ptr<common::DumpItem>>& dumpItems)
{
    ScopedLock lock(mDataLock);
    if (!IsDirty()) {
        return;
    }

    FlushBuildingOperationBlock();
    OperationMeta dumpOpMeta = GetOperationMeta();
    OperationDumperPtr opDumper(new OperationDumper(dumpOpMeta));
    opDumper->Init(mOpBlocks, mReleaseBlockAfterDump, mEnableFlushToDisk);
    DirectoryPtr opDirectory = directory->MakeDirectory(OPERATION_DIR_NAME);
    dumpItems.push_back(std::make_unique<OperationDumpItem>(opDirectory, opDumper));

    // if mReleaseBlockAfterDump == true,
    // opDumper will explicit trigger release operation blocks one by one.
    // in reopen thread, it will set mReleaseBlockAfterDump to false,
    // so that dumped block will not be released and can be accessed
    if (mReleaseBlockAfterDump) {
        mOpBlocks.clear();
        mOperationMeta = OperationMeta();
    }
}

void OperationWriter::Dump(const DirectoryPtr& directory)
{
    ScopedLock lock(mDataLock);
    if (!IsDirty()) {
        return;
    }

    FlushBuildingOperationBlock();
    OperationMeta dumpOpMeta = GetOperationMeta();
    OperationDumper opDumper(dumpOpMeta);
    opDumper.Init(mOpBlocks, mReleaseBlockAfterDump, mEnableFlushToDisk);
    DirectoryPtr opDirectory = directory->MakeDirectory(OPERATION_DIR_NAME);
    opDumper.Dump(opDirectory);

    if (mReleaseBlockAfterDump) {
        mOpBlocks.clear();
        mOperationMeta = OperationMeta();
    }
}

InMemSegmentOperationIteratorPtr OperationWriter::CreateSegmentOperationIterator(segmentid_t segId, size_t offset,
                                                                                 int64_t timestamp)
{
    ScopedLock lock(mDataLock);
    if (offset >= mOperationMeta.GetOperationCount()) {
        return InMemSegmentOperationIteratorPtr();
    }

    // build thread may release operation blocks (in dump).
    // make cloned op blocks to prevent invalid memory access
    OperationBlockVec opBlocks;
    for (size_t i = 0; i < mOpBlocks.size(); ++i) {
        opBlocks.push_back(OperationBlockPtr(mOpBlocks[i]->Clone()));
    }

    InMemSegmentOperationIteratorPtr inMemSegOpIter(
        new InMemSegmentOperationIterator(mSchema, mOperationMeta, segId, offset, timestamp));
    inMemSegOpIter->Init(opBlocks);
    return inMemSegOpIter;
}

void OperationWriter::FlushBuildingOperationBlock()
{
    if (mBlockInfo.mCurBlock) {
        mOperationMeta.EndOneBlock(mBlockInfo.mCurBlock, (int64_t)mOpBlocks.size() - 1);
        UpdateBuildResourceMetrics();
    }
}

const util::BuildResourceMetricsPtr& OperationWriter::GetBuildResourceMetrics() const { return mBuildResourceMetrics; }
}} // namespace indexlib::partition
