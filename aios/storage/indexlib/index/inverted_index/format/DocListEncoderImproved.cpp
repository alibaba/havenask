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
#include "indexlib/index/inverted_index/format/DocListEncoderImproved.h"

#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/index/common/numeric_compress/NewPfordeltaCompressor.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/skiplist/InMemPairValueSkipListReader.h"
#include "indexlib/index/inverted_index/format/skiplist/InMemTriValueSkipListReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, DocListEncoderImproved);

DocListEncoderImproved::DocListEncoderImproved(const DocListFormatOption& docListFormatOption,
                                               util::SimplePool* simplePool, autil::mem_pool::Pool* byteSlicePool,
                                               autil::mem_pool::RecyclePool* bufferPool, DocListFormat* docListFormat,
                                               CompressMode compressMode, format_versionid_t formatVersion)
    : _docListBuffer(byteSlicePool, bufferPool)
    , _ownDocListFormat(false)
    , _isDocIdContinuous(true)
    , _docListFormatOption(docListFormatOption)
    , _fieldMap(0)
    , _currentTF(0)
    , _totalTF(0)
    , _df(0)
    , _lastDocId(0)
    , _lastDocPayload(0)
    , _lastFieldMap(0)
    , _compressMode(compressMode)
    , _tfBitmapWriter(nullptr)
    , _docSkipListWriter(nullptr)
    , _docListFormat(docListFormat)
    , _byteSlicePool(byteSlicePool)
{
    assert(byteSlicePool);
    assert(bufferPool);
    if (docListFormatOption.HasTfBitmap()) {
        _tfBitmapWriter = IE_POOL_COMPATIBLE_NEW_CLASS(byteSlicePool, PositionBitmapWriter, simplePool);
    }
    // TODO, docListFormat can not be nullptr, and should get Init return value
    if (!docListFormat) {
        _docListFormat = new DocListFormat(docListFormatOption, formatVersion);
        _ownDocListFormat = true;
    }
    _docListBuffer.Init(_docListFormat);
}

DocListEncoderImproved::~DocListEncoderImproved()
{
    if (_docSkipListWriter) {
        _docSkipListWriter->~BufferedSkipListWriter();
        _docSkipListWriter = nullptr;
    }
    if (_ownDocListFormat) {
        DELETE_AND_SET_NULL(_docListFormat);
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool, _tfBitmapWriter);
    _tfBitmapWriter = nullptr;
}

void DocListEncoderImproved::AddPosition(int32_t fieldIdxInPack)
{
    if (_docListFormatOption.HasFieldMap()) {
        assert(fieldIdxInPack < 8);
        assert(fieldIdxInPack >= 0);
        _fieldMap |= (1 << fieldIdxInPack);
    }
    _currentTF++;
    _totalTF++;
}

void DocListEncoderImproved::EndDocument(docid32_t docId, docpayload_t docPayload)
{
    if (_isDocIdContinuous && _df > 0) {
        _isDocIdContinuous = ((_lastDocId + 1) == docId);
    }
    AddDocument(docId, docPayload, _currentTF, _fieldMap);
    _df = _df + 1;

    if (_tfBitmapWriter) {
        // set to remember the first occ in this doc
        _tfBitmapWriter->Set(_totalTF - _currentTF);
        _tfBitmapWriter->EndDocument(_df, _totalTF);
    }

    _currentTF = 0;
    _fieldMap = 0;
}

void DocListEncoderImproved::Dump(const std::shared_ptr<file_system::FileWriter>& file)
{
    Flush();
    uint32_t docSkipListSize = 0;
    if (_docSkipListWriter) {
        docSkipListSize = _docSkipListWriter->EstimateDumpSize();
    }

    uint32_t docListSize = _docListBuffer.EstimateDumpSize();
    AUTIL_LOG(TRACE1, "doc skip list length: [%u], doc list length: [%u", docSkipListSize, docListSize);

    file->WriteVUInt32(docSkipListSize).GetOrThrow();
    file->WriteVUInt32(docListSize).GetOrThrow();

    if (_docSkipListWriter) {
        _docSkipListWriter->Dump(file);
    }

    _docListBuffer.Dump(file);
    if (_tfBitmapWriter) {
        _tfBitmapWriter->Dump(file, _totalTF);
    }
}

void DocListEncoderImproved::Flush()
{
    uint8_t compressMode = ShortListOptimizeUtil::IsShortDocList(_df) ? SHORT_LIST_COMPRESS_MODE : _compressMode;
    FlushDocListBuffer(compressMode);
    if (_docSkipListWriter) {
        _docSkipListWriter->FinishFlush();
    }
    if (_tfBitmapWriter) {
        _tfBitmapWriter->Resize(_totalTF);
    }
}

uint32_t DocListEncoderImproved::GetDumpLength() const
{
    uint32_t docSkipListSize = 0;
    if (_docSkipListWriter) {
        docSkipListSize = _docSkipListWriter->EstimateDumpSize();
    }

    uint32_t docListSize = _docListBuffer.EstimateDumpSize();
    uint32_t tfBitmapLength = 0;
    if (_tfBitmapWriter) {
        tfBitmapLength = _tfBitmapWriter->GetDumpLength(_totalTF);
    }
    return VByteCompressor::GetVInt32Length(docSkipListSize) + VByteCompressor::GetVInt32Length(docListSize) +
           docSkipListSize + docListSize + tfBitmapLength;
}

void DocListEncoderImproved::AddDocument(docid32_t docId, docpayload_t docPayload, tf_t tf, fieldmap_t fieldMap)
{
    if (!PostingFormatOption::IsReferenceCompress(_compressMode)) {
        _docListBuffer.PushBack(0, docId - _lastDocId);
    } else {
        _docListBuffer.PushBack(0, docId);
    }

    int n = 1;
    if (_docListFormatOption.HasTfList()) {
        _docListBuffer.PushBack(n++, tf);
    }
    if (_docListFormatOption.HasDocPayload()) {
        _docListBuffer.PushBack(n++, docPayload);
    }
    if (_docListFormatOption.HasFieldMap()) {
        _docListBuffer.PushBack(n++, fieldMap);
    }

    _docListBuffer.EndPushBack();

    _lastDocId = docId;
    _lastDocPayload = docPayload;
    _lastFieldMap = fieldMap;

    if (_docListBuffer.NeedFlush()) {
        FlushDocListBuffer(_compressMode);
    }
}

void DocListEncoderImproved::FlushDocListBuffer(uint8_t compressMode)
{
    uint32_t flushSize = _docListBuffer.Flush(compressMode);
    if (flushSize > 0) {
        if (compressMode != SHORT_LIST_COMPRESS_MODE) {
            if (_docSkipListWriter == nullptr) {
                CreateDocSkipListWriter();
            }
            AddSkipListItem(flushSize);
        }
    }
}

void DocListEncoderImproved::CreateDocSkipListWriter()
{
    void* buffer = _byteSlicePool->allocate(sizeof(BufferedSkipListWriter));
    autil::mem_pool::RecyclePool* bufferPool =
        dynamic_cast<autil::mem_pool::RecyclePool*>(_docListBuffer.GetBufferPool());
    assert(bufferPool);

    BufferedSkipListWriter* docSkipListWriter =
        new (buffer) BufferedSkipListWriter(_byteSlicePool, bufferPool, _compressMode);
    docSkipListWriter->Init(_docListFormat->GetDocListSkipListFormat());
    // skip list writer should be atomic created;
    _docSkipListWriter = docSkipListWriter;
}

void DocListEncoderImproved::AddSkipListItem(uint32_t itemSize)
{
    const DocListSkipListFormat* skipListFormat = _docListFormat->GetDocListSkipListFormat();
    assert(skipListFormat);

    if (skipListFormat->HasTfList()) {
        _docSkipListWriter->AddItem(_lastDocId, _totalTF, itemSize);
    } else {
        _docSkipListWriter->AddItem(_lastDocId, itemSize);
    }
}

InMemDocListDecoder* DocListEncoderImproved::GetInMemDocListDecoder(autil::mem_pool::Pool* sessionPool) const
{
    // copy sequence
    // df -> skiplist -> doclist -> (TBD: poslist)
    df_t df = _df;

    // TODO: delete buffer in pool
    SkipListReader* skipListReader = nullptr;
    if (_docSkipListWriter) {
        const DocListSkipListFormat* skipListFormat = _docListFormat->GetDocListSkipListFormat();
        assert(skipListFormat);

        if (skipListFormat->HasTfList()) {
            InMemTriValueSkipListReader* inMemSkipListReader =
                IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, InMemTriValueSkipListReader, sessionPool);
            inMemSkipListReader->Load(_docSkipListWriter);
            skipListReader = inMemSkipListReader;
        } else {
            InMemPairValueSkipListReader* inMemSkipListReader = IE_POOL_COMPATIBLE_NEW_CLASS(
                sessionPool, InMemPairValueSkipListReader, sessionPool, _compressMode == REFERENCE_COMPRESS_MODE);
            inMemSkipListReader->Load(_docSkipListWriter);
            skipListReader = inMemSkipListReader;
        }
    }
    BufferedByteSlice* docListBuffer =
        IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, BufferedByteSlice, sessionPool, sessionPool);
    _docListBuffer.SnapShot(docListBuffer);

    InMemDocListDecoder* decoder = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, InMemDocListDecoder, sessionPool,
                                                                _compressMode == REFERENCE_COMPRESS_MODE);
    decoder->Init(df, skipListReader, docListBuffer);

    return decoder;
}
} // namespace indexlib::index
