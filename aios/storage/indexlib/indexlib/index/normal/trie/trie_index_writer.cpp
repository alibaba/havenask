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
#include "indexlib/index/normal/trie/trie_index_writer.h"

#include "indexlib/config/index_config.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/normal/trie/in_mem_trie_segment_reader.h"
#include "indexlib/index/normal/trie/trie_index_define.h"
#include "indexlib/index_define.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, TrieIndexWriter);

TrieIndexWriter::TrieIndexWriter() : mKVMap(less<StringView>(), pool_allocator<pair<StringView, docid_t>>(&_simplePool))
{
}

void TrieIndexWriter::UpdateBuildResourceMetrics()
{
    if (!_buildResourceMetricsNode) {
        return;
    }
    int64_t poolSize = _byteSlicePool->getUsedBytes() + _simplePool.getUsedBytes();

    // TODO: adopt more accurate estimation
    int64_t dumpTempBufferSize =
        (DoubleArrayTrie::EstimateMemoryUseFactor() + sizeof(KVPairVector::value_type)) * mKVMap.size();
    int64_t dumpExpandBufferSize = 0;
    int64_t dumpFileSize = poolSize * 3.5;
    _buildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    _buildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTempBufferSize);
    _buildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, dumpExpandBufferSize);
    _buildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

void TrieIndexWriter::EndDocument(const document::IndexDocument& indexDocument)
{
    StringView key = autil::MakeCString(indexDocument.GetPrimaryKey(), _byteSlicePool.get());
    docid_t docId = indexDocument.GetDocId();
    {
        ScopedReadWriteLock lock(mDataLock, 'W');
        mKVMap[key] = docId;
    }
    UpdateBuildResourceMetrics();
}

void TrieIndexWriter::DumpTrieIndex(const KVPairVector& sortedVector, const DirectoryPtr& indexDirectory,
                                    PoolBase* dumpPool)
{
    DoubleArrayTrie trie(dumpPool);
    if (!trie.Build(sortedVector)) {
        IE_LOG(ERROR, "build trie index failed");
        ERROR_COLLECTOR_LOG(ERROR, "build trie index failed");
        return;
    }
    IE_LOG(INFO, "Compression Ratio: %f%%", trie.CompressionRatio() * 100.0);

    const StringView& data = trie.Data();
    file_system::FileWriterPtr fileWriter = indexDirectory->CreateFileWriter(PRIMARY_KEY_DATA_FILE_NAME);
    IE_LOG(INFO, "index pool size [%ld], trie index size [%ld]", _byteSlicePool->getUsedBytes(), data.size());
    fileWriter->Write(&TRIE_INDEX_VERSION, sizeof(TRIE_INDEX_VERSION)).GetOrThrow();
    fileWriter->Write(data.data(), data.size()).GetOrThrow();
    fileWriter->Close().GetOrThrow();
}

void TrieIndexWriter::DumpRawData(const KVPairVector& sortedVector, const DirectoryPtr& indexDirectory)
{
    file_system::FileWriterPtr fileWriter = indexDirectory->CreateFileWriter(TRIE_ORIGINAL_DATA);
    uint32_t dataNum = sortedVector.size();
    fileWriter->Write(&dataNum, sizeof(dataNum)).GetOrThrow();
    for (size_t i = 0; i < dataNum; ++i) {
        const StringView& key = sortedVector[i].first;
        uint32_t keyLength = key.size();
        docid_t docid = sortedVector[i].second;
        fileWriter->Write((void*)&docid, sizeof(docid)).GetOrThrow();
        fileWriter->Write((void*)&keyLength, sizeof(keyLength)).GetOrThrow();
        fileWriter->Write((void*)key.data(), key.size()).GetOrThrow();
    }
    fileWriter->Close().GetOrThrow();
}

void TrieIndexWriter::Dump(const DirectoryPtr& directory, PoolBase* dumpPool)
{
    IE_LOG(DEBUG, "Start Dumping index: [%s]", _indexConfig->GetIndexName().c_str());
    KVPairVector::allocator_type allocator(dumpPool);
    KVPairVector sortedVector(allocator);
    {
        ScopedReadWriteLock lock(mDataLock, 'R');
        sortedVector.assign(mKVMap.begin(), mKVMap.end());
    }
    file_system::DirectoryPtr indexDirectory = directory->MakeDirectory(_indexConfig->GetIndexName());

    DumpRawData(sortedVector, indexDirectory);
    DumpTrieIndex(sortedVector, indexDirectory, dumpPool);

    IE_LOG(DEBUG, "Finish Dumping index: [%s]", _indexConfig->GetIndexName().c_str());
    UpdateBuildResourceMetrics();
}

uint32_t TrieIndexWriter::GetDistinctTermCount() const
{
    ScopedReadWriteLock lock(mDataLock, 'R');
    return mKVMap.size();
}

index::IndexSegmentReaderPtr TrieIndexWriter::CreateInMemReader()
{
    index::IndexSegmentReaderPtr inMemReader(new InMemTrieSegmentReader(&mDataLock, &mKVMap));
    return inMemReader;
}

void TrieIndexWriter::GetDumpEstimateFactor(std::priority_queue<double>& factors,
                                            std::priority_queue<size_t>& minMemUses) const
{
    factors.push(DoubleArrayTrie::EstimateMemoryUseFactor() + sizeof(KVPairVector::value_type));
    minMemUses.push(DoubleArrayTrie::EstimateMemoryUse(0));
}
}} // namespace indexlib::index
