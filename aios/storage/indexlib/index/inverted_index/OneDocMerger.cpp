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
#include "indexlib/index/inverted_index/OneDocMerger.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, OneDocMerger);

OneDocMerger::OneDocMerger(const PostingFormatOption& formatOption, PostingDecoderImpl* decoder,
                           SingleTermIndexSegmentPatchIterator* patchIter)
    : _docCountInBuf(0)
    , _docBufCursor(0)
    , _lastDocBufCursor(0)
    , _docId(0)
    , _posCountInBuf(0)
    , _posBufCursor(0)
    , _posIndex(0)
    , _decoder(decoder)
    , _patchIterator(patchIter)
    , _lastReadFrom(BOTH)
    , _formatOption(formatOption)
{
}

OneDocMerger::~OneDocMerger() {}

docid32_t OneDocMerger::CurrentDoc() const
{
    if (_lastReadFrom == PATCH) {
        return _patchDoc.DocId();
    } else if (_lastReadFrom == BUFFERED_POSTING) {
        return _docId;
    } else {
        return _patchDoc.DocId();
    }
}

bool OneDocMerger::PatchEnd() const { return _patchIterator == nullptr; }
OneDocMerger::NextResult OneDocMerger::PatchCurrent() const
{
    if (PatchEnd()) {
        return NextResult(/*docId=*/0, /*isEof=*/true);
    }
    return NextResult(/*docId=*/_patchDoc.DocId(), /*isEof=*/false);
}
OneDocMerger::NextResult OneDocMerger::PatchNext()
{
    if (!_patchIterator) {
        return NextResult(/*docId=*/0, /*isEof=*/true);
    }
    bool patchHasNext = _patchIterator->Next(&_patchDoc);
    if (!patchHasNext) {
        _patchIterator = nullptr;
        return NextResult(/*docId=*/0, /*isEof=*/true);
    }
    return NextResult(/*docId=*/_patchDoc.DocId(), /*isEof=*/false);
}

bool OneDocMerger::BufferedPostingEnd() const { return _decoder == nullptr; }
OneDocMerger::NextResult OneDocMerger::BufferedPostingCurrent() const
{
    if (BufferedPostingEnd()) {
        return NextResult(/*docId=*/0, /*isEof=*/true);
    }
    return NextResult(_docId, /*isEof=*/false);
}
OneDocMerger::NextResult OneDocMerger::BufferedPostingNext()
{
    if (!_decoder) {
        return NextResult(/*docId=*/0, /*isEof=*/true);
    }
    if (_docBufCursor == _docCountInBuf) {
        _docBufCursor = 0;
        _docCountInBuf =
            _decoder->DecodeDocList(_docIdBuf, _tfListBuf, _docPayloadBuf, _fieldMapBuffer, MAX_DOC_PER_RECORD);
        if (_docCountInBuf != 0) {
            _bufReader.Reset((char*)_docIdBuf);
        } else {
            _decoder = nullptr;
            return NextResult(/*docId=*/0, /*isEof=*/true);
        }
    }
    if (_formatOption.IsReferenceCompress()) {
        _docId = _bufReader[_docBufCursor];
    } else {
        _docId += _docIdBuf[_docBufCursor];
    }
    _lastDocBufCursor = _docBufCursor;
    if (!BufferedPostingEnd()) {
        ++_docBufCursor;
    }
    return NextResult(/*docId=*/_docId, /*isEof=*/false);
}

bool OneDocMerger::BookkeepReadFrom(const NextResult& postingResult, const NextResult& patchResult,
                                    LastReadFrom* lastReadFrom)
{
    *lastReadFrom = INVALID;
    if (patchResult.IsEof() and postingResult.IsEof()) {
        return false;
    }
    if (patchResult < postingResult) {
        *lastReadFrom = PATCH;
    } else if (patchResult > postingResult) {
        *lastReadFrom = BUFFERED_POSTING;
    } else {
        *lastReadFrom = BOTH;
    }
    return true;
}

bool OneDocMerger::Next()
{
    // int64_t patchDocId = DOCID_EOF;
    // int64_t postingDocId = DOCID_EOF;
    NextResult postingResult;
    NextResult patchResult;

    while (!PatchEnd() or !BufferedPostingEnd()) {
        if (_lastReadFrom == PATCH) {
            patchResult = PatchNext();
            postingResult = BufferedPostingCurrent();
        } else if (_lastReadFrom == BUFFERED_POSTING) {
            postingResult = BufferedPostingNext();
            patchResult = PatchCurrent();
        } else if (_lastReadFrom == BOTH) {
            postingResult = BufferedPostingNext();
            patchResult = PatchNext();
        }

        if (!BookkeepReadFrom(postingResult, patchResult, &_lastReadFrom)) {
            return false;
        }
        if (!(patchResult > postingResult) and _patchDoc.IsDelete()) {
            continue;
        }
        return true;
    }

    // while (!PatchEnd() or !BufferedPostingEnd()) {
    //     if (_lastReadFrom == PATCH) {
    //         patchDocId = PatchNext();
    //     } else if (_lastReadFrom == BUFFERED_POSTING) {
    //         postingDocId = BufferedPostingNext();
    //     } else if (_lastReadFrom==BOTH){
    //         postingDocId = BufferedPostingNext();
    //         patchDocId = PatchNext();
    //     }else{
    //     }

    //     if (patchDocId < postingDocId) {
    //         _lastReadFrom = PATCH;
    //     } else if (patchDocId > postingDocId) {
    //         _lastReadFrom = BUFFERED_POSTING;
    //     } else if (patchDocId == DOCID_EOF) {
    //         return false;
    //     } else {
    //         _lastReadFrom = BOTH;
    //     }

    //     if (patchDocId <= postingDocId and _patchDoc.IsDelete()) {
    //         continue;
    //     }
    //     return true;
    // }
    return false;
}

void OneDocMerger::Merge(docid32_t newDocId,
                         PostingWriterImpl* postingWriter) // writer to new seg
{
    tf_t tf = MergePosition(newDocId, postingWriter);
    if (newDocId != INVALID_DOCID) {
        if (_formatOption.HasFieldMap()) {
            assert(!_patchIterator);
            postingWriter->SetFieldMap(_fieldMapBuffer[_lastDocBufCursor]);
        }

        if (_formatOption.HasTermFrequency() && !_formatOption.HasPositionList()) {
            assert(!_patchIterator);
            postingWriter->SetCurrentTF(tf);
        }

        if (_formatOption.HasTermPayload()) {
            assert(!_patchIterator);
            assert(_decoder);
            postingWriter->SetTermPayload(_decoder->GetTermMeta()->GetPayload());
        }
        docpayload_t docPayload = 0;
        if (_lastReadFrom == BUFFERED_POSTING or _lastReadFrom == BOTH) {
            docPayload = _docPayloadBuf[_lastDocBufCursor];
        }
        postingWriter->EndDocument(newDocId, docPayload);
    }
}

tf_t OneDocMerger::MergePosition(docid32_t newDocId, PostingWriterImpl* postingWriter)
{
    if (!_formatOption.HasTermFrequency()) {
        return 0;
    }
    assert(!_patchIterator);

    tf_t tf = 0;
    pos_t pos = 0;
    while (true) {
        if (_formatOption.HasPositionList()) {
            if (_posBufCursor == _posCountInBuf) {
                _posBufCursor = 0;
                _posCountInBuf = _decoder->DecodePosList(_posBuf, _posPayloadBuf, MAX_POS_PER_RECORD);
                if (_posCountInBuf == 0) {
                    break;
                }
            }
            if (newDocId != INVALID_DOCID) {
                pos += _posBuf[_posBufCursor];
                postingWriter->AddPosition(pos, _posPayloadBuf[_posBufCursor], 0);
            }
            _posBufCursor++;
        }
        tf++;
        _posIndex++;

        if (_formatOption.HasTfList() && tf >= _tfListBuf[_lastDocBufCursor]) {
            break;
        }

        if (_formatOption.HasTfBitmap() && _decoder->IsDocEnd(_posIndex)) {
            break;
        }
    }
    return tf;
}
} // namespace indexlib::index
