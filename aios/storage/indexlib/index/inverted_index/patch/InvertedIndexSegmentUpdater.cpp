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
#include "indexlib/index/inverted_index/patch/InvertedIndexSegmentUpdater.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/SnappyCompressFileWriter.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/patch/PatchFileInfo.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/PatchFormat.h"
#include "indexlib/util/Algorithm.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, InvertedIndexSegmentUpdater);

InvertedIndexSegmentUpdater::InvertedIndexSegmentUpdater(segmentid_t segId,
                                                         const std::shared_ptr<InvertedIndexConfig>& indexConfig)
    : IInvertedIndexSegmentUpdater(segId, indexConfig)
    , _hashMap(MapAllocator(&_simplePool))
    , _nullTermDocs(VectorAllocator(&_simplePool))
    , _docCount(0)
{
}

void InvertedIndexSegmentUpdater::Update(docid_t docId, const document::ModifiedTokens& modifiedTokens)
{
    assert(docId >= 0);
    std::unique_lock<std::mutex> lock(_dataMutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        AUTIL_LOG(DEBUG, "Skip update doc [%d] for the segment has been dumped", docId);
        return;
    }
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
}

void InvertedIndexSegmentUpdater::Update(docid_t localDocId, index::DictKeyInfo termKey, bool isDelete)
{
    assert(localDocId >= 0);
    std::unique_lock<std::mutex> lock(_dataMutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        AUTIL_LOG(DEBUG, "Skip update doc [%d] for the segment has been dumped", localDocId);
        return;
    }
    if (termKey.IsNull()) {
        _nullTermDocs.push_back(ComplexDocId(localDocId, /*remove*/ isDelete));
    } else {
        auto iter = _hashMap.find(termKey.GetKey());
        if (iter == _hashMap.end()) {
            iter = _hashMap.insert(iter, {termKey.GetKey(), DocVector(VectorAllocator(&_simplePool))});
        }
        iter->second.push_back(ComplexDocId(localDocId, /*remove=*/isDelete));
        ++_docCount;
    }
}

// [token:8B][docCount:4B][ComplexDocList];[token][docCount][ComplexDocList];...;[NonNullTermCount:8B]
Status InvertedIndexSegmentUpdater::Dump(const std::shared_ptr<file_system::IDirectory>& indexesDir,
                                         segmentid_t srcSegment)
{
    auto [mkdirStatus, indexDir] =
        indexesDir->MakeDirectory(_indexConfig->GetIndexName(), file_system::DirectoryOption()).StatusWith();
    RETURN_IF_STATUS_ERROR(mkdirStatus, "make index directory failed.");
    auto [status, patchFileInfo] = DumpToIndexDir(indexDir, srcSegment, nullptr);
    return status;
}

std::pair<Status, std::shared_ptr<indexlibv2::index::PatchFileInfo>>
InvertedIndexSegmentUpdater::DumpToIndexDir(const std::shared_ptr<file_system::IDirectory>& indexDir,
                                            segmentid_t srcSegment, const std::vector<docid_t>* old2NewDocId)
{
    std::lock_guard<std::mutex> guard(_dataMutex);
    std::vector<uint64_t> sortedTermKeys;
    util::Algorithm::GetSortedKeys(_hashMap, &sortedTermKeys);
    if (sortedTermKeys.empty() and _nullTermDocs.empty()) {
        // not dirty
        return {Status::OK(), nullptr};
    }

    bool enableCompress = _indexConfig->IsPatchCompressed();
    std::string patchFileName = IInvertedIndexSegmentUpdater::GetPatchFileName(srcSegment, /*dstSegment=*/_segmentId);
    auto [status, patchFileWriter] = CreateFileWriter(indexDir, patchFileName, enableCompress);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "create patch file writer failed for [%s].", patchFileName.c_str());

    AUTIL_LOG(DEBUG, "Begin dumping index patch to files : %s", patchFileWriter->DebugString().c_str());
    for (size_t i = 0; i < sortedTermKeys.size(); ++i) {
        uint64_t termKey = sortedTermKeys[i];
        auto iter = _hashMap.find(termKey);
        assert(iter != _hashMap.end());
        DocVector& docList = iter->second;
        RewriteDocList(old2NewDocId, docList);
        PatchFormat::WriteDocListForTermToPatchFile(patchFileWriter, termKey, docList);
    }
    if (!_nullTermDocs.empty()) {
        RewriteDocList(old2NewDocId, _nullTermDocs);
        PatchFormat::WriteDocListForTermToPatchFile(patchFileWriter, index::DictKeyInfo::NULL_TERM.GetKey(),
                                                    _nullTermDocs);
    }
    PatchFormat::PatchMeta meta;
    meta.nonNullTermCount = sortedTermKeys.size();
    meta.hasNullTerm = (_nullTermDocs.empty() ? 0 : 1);
    patchFileWriter->Write(&meta, sizeof(meta)).GetOrThrow();

    patchFileWriter->Close().GetOrThrow();
    AUTIL_LOG(DEBUG, "Finish dumping index patch to file : %s", patchFileWriter->DebugString().c_str());
    return {Status::OK(),
            std::make_shared<indexlibv2::index::PatchFileInfo>(srcSegment, _segmentId, indexDir, patchFileName)};
}

void InvertedIndexSegmentUpdater::RewriteDocList(const std::vector<docid_t>* old2NewDocId, DocVector& docList)
{
    if (old2NewDocId) {
        for (size_t j = 0; j < docList.size(); ++j) {
            docList[j] = ComplexDocId(old2NewDocId->at(docList[j].DocId()), docList[j].IsDelete());
        }
    }
    docList.erase(util::Algorithm::SortAndUnique(docList.begin(), docList.end()), docList.end());
}

std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
InvertedIndexSegmentUpdater::CreateFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                              const std::string& fileName, bool hasPatchCompress)
{
    auto [status, patchFileWriter] = directory->CreateFileWriter(fileName, file_system::WriterOption()).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "create patch file writer failed for [%s].", fileName.c_str());
    if (!hasPatchCompress) {
        return std::make_pair(Status::OK(), patchFileWriter);
    }
    auto compressWriter = std::make_shared<file_system::SnappyCompressFileWriter>();
    compressWriter->Init(patchFileWriter, file_system::SnappyCompressFileWriter::DEFAULT_COMPRESS_BUFF_SIZE);
    return std::make_pair(Status::OK(), compressWriter);
}

} // namespace indexlib::index
