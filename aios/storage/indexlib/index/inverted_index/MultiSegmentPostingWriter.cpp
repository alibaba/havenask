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
#include "indexlib/index/inverted_index/MultiSegmentPostingWriter.h"

#include "indexlib/file_system/file/InterimFileWriter.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/MultiSegmentPostingIterator.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/util/NumericUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, MultiSegmentPostingWriter);

MultiSegmentPostingWriter::~MultiSegmentPostingWriter() {}

void MultiSegmentPostingWriter::Dump(
    const index::DictKeyInfo& key,
    const std::vector<std::shared_ptr<IndexOutputSegmentResource>>& indexOutputSegmentResources,
    termpayload_t termPayload)
{
    assert(indexOutputSegmentResources.size() == _targetSegments.size());
    for (size_t i = 0; i < indexOutputSegmentResources.size(); i++) {
        DumpPosting(key, indexOutputSegmentResources[i], _postingWriters[i], termPayload);
    }
}

std::shared_ptr<PostingIterator> MultiSegmentPostingWriter::CreatePostingIterator(termpayload_t termPayload)
{
    std::vector<MultiSegmentPostingIterator::PostingIteratorInfo> postingIterators;
    std::vector<std::shared_ptr<file_system::InterimFileWriter>> fileWriters;
    std::vector<segmentid_t> segmentIdxs;
    int64_t baseDocid = 0;
    for (int32_t i = 0; i < (int32_t)_postingWriters.size(); i++) {
        auto postingWriter = _postingWriters[i];
        if (postingWriter->GetDF() > 0) {
            auto fileWriterPtr = std::make_shared<file_system::InterimFileWriter>();
            std::shared_ptr<PostingIterator> postingIter =
                CreateSinglePostingIterator(postingWriter, termPayload, fileWriterPtr);
            postingIterators.push_back(std::make_pair(baseDocid, postingIter));
            fileWriters.push_back(fileWriterPtr);
            segmentIdxs.push_back(i);
        }
        baseDocid += _targetSegments[i]->segmentInfo->docCount;
    }

    auto iter = std::make_shared<MultiSegmentPostingIterator>();
    iter->Init(postingIterators, segmentIdxs, fileWriters);
    iter->Reset();
    return iter;
}

std::shared_ptr<PostingIterator>
MultiSegmentPostingWriter::CreateSinglePostingIterator(std::shared_ptr<PostingWriter>& postingWriter,
                                                       termpayload_t termPayload,
                                                       std::shared_ptr<file_system::InterimFileWriter>& fileWriterPtr)
{
    fileWriterPtr->Init(GetDumpLength(postingWriter, termPayload));
    auto postingIter = CreatePostingIterator(fileWriterPtr, postingWriter, termPayload);
    return postingIter;
}

dictvalue_t MultiSegmentPostingWriter::GetDictValue(const std::shared_ptr<PostingWriter>& postingWriter,
                                                    uint64_t offset)
{
    uint64_t inlinePostingValue;
    bool isDocList = false;
    if (postingWriter->GetDictInlinePostingValue(inlinePostingValue, isDocList)) {
        return ShortListOptimizeUtil::CreateDictInlineValue(inlinePostingValue, isDocList, true /*dfFirst*/);
    }
    uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(postingWriter->GetDF(), postingWriter->GetTotalTF(),
                                                                  _postingFormatOption.GetDocListCompressMode());
    return ShortListOptimizeUtil::CreateDictValue(compressMode, offset);
}

std::shared_ptr<PostingIterator>
MultiSegmentPostingWriter::CreatePostingIterator(std::shared_ptr<file_system::InterimFileWriter>& fileWriterPtr,
                                                 std::shared_ptr<PostingWriter>& postingWriter,
                                                 termpayload_t termPayload)
{
    uint64_t dictInlineValue;
    auto segPostings = std::make_shared<SegmentPostingVector>();
    bool isDocList = false;
    bool dfFirst = true;
    if (postingWriter->GetDictInlinePostingValue(dictInlineValue, isDocList)) {
        SegmentPosting segPosting(_postingFormatOption);
        segPosting.Init((docid_t)0, (uint64_t)0,
                        ShortListOptimizeUtil::CreateDictInlineValue(dictInlineValue, isDocList, dfFirst), isDocList,
                        dfFirst);
        segPostings->push_back(std::move(segPosting));
    } else {
        DoDumpPosting(postingWriter, fileWriterPtr, termPayload);
        uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(
            postingWriter->GetDF(), postingWriter->GetTotalTF(), _postingFormatOption.GetDocListCompressMode());
        SegmentPosting segPosting(_postingFormatOption);
        segPosting.Init(compressMode, fileWriterPtr->GetByteSliceList(true), 0, 0);
        segPostings->push_back(std::move(segPosting));
    }

    BufferedPostingIterator* bufferedPostingIter =
        new BufferedPostingIterator(_postingFormatOption, /*sessionPool*/ nullptr, /*tracer*/ nullptr);
    bufferedPostingIter->Init(segPostings, nullptr, 1);
    std::shared_ptr<PostingIterator> postingIter(bufferedPostingIter);
    return postingIter;
}

uint64_t MultiSegmentPostingWriter::GetDumpLength(const std::shared_ptr<PostingWriter>& postingWriter,
                                                  termpayload_t termPayload) const
{
    TermMeta termMeta(postingWriter->GetDF(), postingWriter->GetTotalTF(), termPayload);
    TermMetaDumper tmDumper(_postingFormatOption);
    uint64_t length = tmDumper.CalculateStoreSize(termMeta) + postingWriter->GetDumpLength();
    if (_postingFormatOption.IsCompressedPostingHeader()) {
        length += VByteCompressor::GetVInt32Length(length);
    } else {
        length += sizeof(uint32_t);
    }
    return length;
}

void MultiSegmentPostingWriter::DumpPosting(const index::DictKeyInfo& key,
                                            const std::shared_ptr<IndexOutputSegmentResource>& resource,
                                            std::shared_ptr<PostingWriter>& postingWriter, termpayload_t termPayload)
{
    if (postingWriter->GetDF() <= 0) {
        return;
    }
    std::shared_ptr<IndexDataWriter> termDataWriter = resource->GetIndexDataWriter(SegmentTermInfo::TM_NORMAL);
    auto postingFile = termDataWriter->postingWriter;
    dictvalue_t dictValue = GetDictValue(postingWriter, postingFile->GetLogicLength());
    termDataWriter->dictWriter->AddItem(key, dictValue);
    bool isDocList = false;
    bool dfFirst = true;
    if (ShortListOptimizeUtil::IsDictInlineCompressMode(dictValue, isDocList, dfFirst)) {
        return;
    }

    // dump posting
    DoDumpPosting(postingWriter, postingFile, termPayload);
}

void MultiSegmentPostingWriter::DoDumpPosting(std::shared_ptr<PostingWriter>& postingWriter,
                                              const std::shared_ptr<file_system::FileWriter>& postingFile,
                                              termpayload_t termPayload)
{
    TermMeta termMeta(postingWriter->GetDF(), postingWriter->GetTotalTF(), termPayload);
    TermMetaDumper tmDumper(_postingFormatOption);

    uint32_t totalLen = tmDumper.CalculateStoreSize(termMeta) + postingWriter->GetDumpLength();
    if (_postingFormatOption.IsCompressedPostingHeader()) {
        postingFile->WriteVUInt32(totalLen).GetOrThrow();
    } else {
        postingFile->Write((const void*)&totalLen, sizeof(uint32_t)).GetOrThrow();
    }
    tmDumper.Dump(postingFile, termMeta);
    postingWriter->Dump(postingFile);
}

} // namespace indexlib::index
