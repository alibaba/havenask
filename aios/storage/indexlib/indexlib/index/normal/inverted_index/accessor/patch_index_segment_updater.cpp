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
#include "indexlib/index/normal/inverted_index/accessor/patch_index_segment_updater.h"

#include "indexlib/document/index_document/normal_document/modified_tokens.h"
#include "indexlib/file_system/file/SnappyCompressFileWriter.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/format/PatchFormat.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Algorithm.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(Index, PatchIndexSegmentUpdater);

PatchIndexSegmentUpdater::PatchIndexSegmentUpdater(
    util::BuildResourceMetrics* buildResourceMetrics, segmentid_t segId,
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
    : _buildResourceMetrics(buildResourceMetrics)
    , _buildResourceMetricsNode(nullptr)
    , _segmentId(segId)
    , _indexConfig(indexConfig)
    , _hashMap(MapAllocator(&_simplePool))
    , _nullTermDocs(VectorAllocator(&_simplePool))
    , _docCount(0)
{
    if (_buildResourceMetrics) {
        _buildResourceMetricsNode = _buildResourceMetrics->AllocateNode();
        IE_LOG(INFO,
               "allocate build resource node [id:%d] for PatchIndexSegmentUpdater[%s] in "
               "segment[%d]",
               _buildResourceMetricsNode->GetNodeId(), _indexConfig->GetIndexName().c_str(), segId);
    }
}

void PatchIndexSegmentUpdater::Update(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    assert(docId >= 0);
    for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
        uint64_t termKey = modifiedTokens[i].first;
        auto iter = _hashMap.find(termKey);
        if (iter == _hashMap.end()) {
            iter = _hashMap.insert(iter, {termKey, DocVector(VectorAllocator(&_simplePool))});
        }
        if (modifiedTokens[i].second == document::ModifiedTokens::Operation::ADD) {
            iter->second.push_back(ComplexDocId(docId, /*remove=*/false));
        } else if (modifiedTokens[i].second == document::ModifiedTokens::Operation::REMOVE) {
            iter->second.push_back(ComplexDocId(docId, /*remove=*/true));
        } else {
        }
        ++_docCount;
    }

    document::ModifiedTokens::Operation nullTermOp;
    if (modifiedTokens.GetNullTermOperation(&nullTermOp)) {
        if (nullTermOp == document::ModifiedTokens::Operation::REMOVE) {
            _nullTermDocs.push_back(ComplexDocId(docId, /*remove*/ true));
            ++_docCount;
        } else if (nullTermOp == document::ModifiedTokens::Operation::ADD) {
            _nullTermDocs.push_back(ComplexDocId(docId, /*remove*/ false));
            ++_docCount;
        } else {
        }
    }
    UpdateBuildResourceMetrics();
}

void PatchIndexSegmentUpdater::Update(docid_t docId, index::DictKeyInfo termKey, bool isDelete)
{
    assert(docId >= 0);
    if (termKey.IsNull()) {
        _nullTermDocs.push_back(ComplexDocId(docId, /*remove*/ isDelete));
    } else {
        auto iter = _hashMap.find(termKey.GetKey());
        if (iter == _hashMap.end()) {
            iter = _hashMap.insert(iter, {termKey.GetKey(), DocVector(VectorAllocator(&_simplePool))});
        }
        iter->second.push_back(ComplexDocId(docId, /*remove=*/isDelete));
        ++_docCount;
    }
}

void PatchIndexSegmentUpdater::UpdateBuildResourceMetrics()
{
    if (!_buildResourceMetricsNode) {
        return;
    }
    int64_t totalMemUse = _simplePool.getUsedBytes();
    _buildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, totalMemUse);
    int64_t dumpTemMemUse = 0;
    dumpTemMemUse += sizeof(uint64_t) * _hashMap.size(); // sortedTermKeys
    if (_indexConfig->IsPatchCompressed()) {
        // file writer buffer
        dumpTemMemUse += file_system::SnappyCompressFileWriter::DEFAULT_COMPRESS_BUFF_SIZE * 2;
    }
    _buildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, dumpTemMemUse);

    int64_t dumpFileSize = 0;
    dumpFileSize = (sizeof(/*termkey*/ uint64_t) + sizeof(/*docCount*/ uint32_t)) * _hashMap.size() +
                   sizeof(ComplexDocId) * _docCount;
    if (_indexConfig->IsPatchCompressed()) {
        // TODO(hanyao): snappy compress ratio
    }
    _buildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}

// [token:8B][docCount:4B][ComplexDocList];[token][docCount][ComplexDocList];...;[NonNullTermCount:8B]
void PatchIndexSegmentUpdater::Dump(const file_system::DirectoryPtr& indexDir, segmentid_t srcSegment)
{
    file_system::DirectoryPtr dir = indexDir->MakeDirectory(_indexConfig->GetIndexName());
    DumpToFieldDirectory(dir, srcSegment);
}

void PatchIndexSegmentUpdater::DumpToFieldDirectory(const file_system::DirectoryPtr& indexFieldDir,
                                                    segmentid_t srcSegment)
{
    std::vector<uint64_t> sortedTermKeys;
    util::Algorithm::GetSortedKeys(_hashMap, &sortedTermKeys);
    if (sortedTermKeys.empty() and _nullTermDocs.empty()) {
        // not dirty
        return;
    }

    bool enableCompress = _indexConfig->IsPatchCompressed();
    std::string patchFileName;
    file_system::FileWriterPtr patchFileWriter = CreateFileWriter(
        indexFieldDir, PatchIndexSegmentUpdaterBase::GetPatchFileName(srcSegment, /*dstSegment=*/_segmentId),
        enableCompress);

    IE_LOG(DEBUG, "Begin dumping index patch to files : %s", patchFileWriter->DebugString().c_str());
    for (size_t i = 0; i < sortedTermKeys.size(); ++i) {
        uint64_t termKey = sortedTermKeys[i];
        auto iter = _hashMap.find(termKey);
        assert(iter != _hashMap.end());
        DocVector& docList = iter->second;
        docList.erase(util::Algorithm::SortAndUnique(docList.begin(), docList.end()), docList.end());
        PatchFormat::WriteDocListForTermToPatchFile(patchFileWriter, termKey, docList);
    }
    if (!_nullTermDocs.empty()) {
        PatchFormat::WriteDocListForTermToPatchFile(patchFileWriter, index::DictKeyInfo::NULL_TERM.GetKey(),
                                                    _nullTermDocs);
    }
    PatchFormat::PatchMeta meta;
    meta.nonNullTermCount = sortedTermKeys.size();
    meta.hasNullTerm = (_nullTermDocs.empty() ? 0 : 1);
    patchFileWriter->Write(&meta, sizeof(meta)).GetOrThrow();

    patchFileWriter->Close().GetOrThrow();
    IE_LOG(DEBUG, "Finish dumping index patch to file : %s", patchFileWriter->DebugString().c_str());
}

std::string PatchIndexSegmentUpdaterBase::GetPatchFileName(segmentid_t srcSegment, segmentid_t dstSegment)
{
    std::stringstream ss;
    assert(srcSegment != INVALID_SEGMENTID);
    ss << srcSegment << "_" << dstSegment << "." << PATCH_FILE_NAME;
    return ss.str();
}

file_system::FileWriterPtr PatchIndexSegmentUpdater::CreateFileWriter(const file_system::DirectoryPtr& directory,
                                                                      const std::string& fileName,
                                                                      bool hasPatchCompress)
{
    file_system::FileWriterPtr fileWriter = directory->CreateFileWriter(fileName);
    if (!hasPatchCompress) {
        return fileWriter;
    }
    file_system::SnappyCompressFileWriterPtr compressWriter(new file_system::SnappyCompressFileWriter);
    compressWriter->Init(fileWriter, file_system::SnappyCompressFileWriter::DEFAULT_COMPRESS_BUFF_SIZE);
    return compressWriter;
}
}} // namespace indexlib::index
