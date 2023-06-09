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
#include "indexlib/index/inverted_index/patch/SingleFieldIndexSegmentPatchIterator.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/inverted_index/patch/SingleTermIndexSegmentPatchIterator.h"

using namespace indexlib::index;

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SingleFieldIndexSegmentPatchIterator);

SingleFieldIndexSegmentPatchIterator::~SingleFieldIndexSegmentPatchIterator()
{
    while (!_patchHeap.empty()) {
        IndexPatchFileReader* patchReader = _patchHeap.top();
        _patchHeap.pop();
        delete patchReader;
    }
}

Status SingleFieldIndexSegmentPatchIterator::AddPatchFile(const std::shared_ptr<file_system::IDirectory>& directory,
                                                          const std::string& fileName, segmentid_t srcSegmentId,
                                                          segmentid_t targetSegmentId)
{
    AUTIL_LOG(INFO, "Add patch file [%s] in dir [%s] to patch iterator", fileName.c_str(),
              directory->DebugString().c_str());
    IndexPatchFileReader* patchReader =
        new IndexPatchFileReader(srcSegmentId, targetSegmentId, _indexConfig->IsPatchCompressed());
    auto status = patchReader->Open(directory, fileName);
    RETURN_IF_STATUS_ERROR(status, "open patch file [%s] failed", fileName.c_str());
    if (patchReader->HasNext()) {
        _patchLoadExpandSize += patchReader->GetPatchLoadExpandSize();
        _patchItemCount += patchReader->GetPatchItemCount();
        patchReader->Next();
        _patchHeap.push(patchReader);
    }
    return Status::OK();
}

bool SingleFieldIndexSegmentPatchIterator::Empty() const { return _patchHeap.empty(); }

std::unique_ptr<SingleTermIndexSegmentPatchIterator> SingleFieldIndexSegmentPatchIterator::NextTerm()
{
    bool found = false;
    while (!_patchHeap.empty()) {
        IndexPatchFileReader* patchReader = _patchHeap.top();
        if (patchReader->GetCurrentTermKey() != _currentTermKey || _initialState) {
            _currentTermKey = patchReader->GetCurrentTermKey();
            _initialState = false;
            found = true;
            break;
        } else {
            assert(!_initialState);
            _patchHeap.pop();
            patchReader->SkipCurrentTerm();
            if (patchReader->HasNext()) {
                patchReader->Next();
                _patchHeap.push(patchReader);
            } else {
                delete patchReader;
            }
        }
    }
    if (!found) {
        return nullptr;
    }
    auto indexId = _indexConfig->GetParentIndexId();
    if (indexId == INVALID_INDEXID) {
        indexId = _indexConfig->GetIndexId();
    }
    auto res =
        std::make_unique<SingleTermIndexSegmentPatchIterator>(&_patchHeap, _currentTermKey, _targetSegmentId, indexId);
    return res;
}
} // namespace indexlib::index
