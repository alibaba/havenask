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
#pragma once

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/BufferedByteSlice.h"
#include "indexlib/index/inverted_index/format/DocListFormat.h"
#include "indexlib/index/inverted_index/format/DocListFormatOption.h"
#include "indexlib/index/inverted_index/format/InMemDocListDecoder.h"
#include "indexlib/index/inverted_index/format/PositionBitmapWriter.h"
#include "indexlib/index/inverted_index/format/skiplist/BufferedSkipListWriter.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::index {

class DocListEncoder
{
public:
    DocListEncoder(const DocListFormatOption& docListFormatOption, util::SimplePool* simplePool,
                   autil::mem_pool::Pool* byteSlicePool, autil::mem_pool::RecyclePool* bufferPool,
                   DocListFormat* docListFormat = nullptr,
                   index::CompressMode compressMode = index::PFOR_DELTA_COMPRESS_MODE,
                   format_versionid_t formatVersion = indexlibv2::config::InvertedIndexConfig::BINARY_FORMAT_VERSION);

    ~DocListEncoder();

    void AddPosition(int32_t fieldIdxInPack);
    void EndDocument(docid_t docId, docpayload_t docPayload);
    void Dump(const file_system::FileWriterPtr& file);
    uint32_t GetDumpLength() const;
    void Flush();
    void FlushDocListBuffer(uint8_t compressMode);

    uint32_t GetCurrentTF() const { return _currentTF; }
    uint32_t GetTotalTF() const { return _totalTF; }
    uint32_t GetDF() const { return _df; }

    fieldmap_t GetFieldMap() const { return _fieldMap; }
    docid_t GetLastDocId() const { return _lastDocId; }
    docpayload_t GetLastDocPayload() const { return _lastDocPayload; }
    fieldmap_t GetLastFieldMap() const { return _lastFieldMap; }

    void SetFieldMap(fieldmap_t fieldMap) { _fieldMap = fieldMap; }
    void SetCurrentTF(tf_t tf)
    {
        _currentTF = tf;
        _totalTF += tf;
    }

    InMemDocListDecoder* GetInMemDocListDecoder(autil::mem_pool::Pool* sessionPool) const;

    index::PositionBitmapWriter* GetPositionBitmapWriter() const { return _tfBitmapWriter; }

    bool IsDocIdContinuous() const { return _isDocIdContinuous; }

private:
    void AddDocument(docid_t docId, docpayload_t docPayload, tf_t tf, fieldmap_t fieldMap);
    void CreateDocSkipListWriter();
    void AddSkipListItem(uint32_t itemSize);

private:
    const DocListFormat* TEST_GetDocListFormat() const { return _docListFormat; }
    BufferedByteSlice* TEST_GetDocListBuffer() { return &_docListBuffer; }
    void TEST_SetDocSkipListWriter(BufferedSkipListWriter* skipListBuffer) { _docSkipListWriter = skipListBuffer; }

    void TEST_SetPositionBitmapWriter(PositionBitmapWriter* tfBitmapWriter)
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_byteSlicePool, _tfBitmapWriter);
        _tfBitmapWriter = tfBitmapWriter;
    }

private:
    BufferedByteSlice _docListBuffer;
    bool _ownDocListFormat;                   // 1byte
    bool _isDocIdContinuous;                  // 1byte
    DocListFormatOption _docListFormatOption; // 1byte
    fieldmap_t _fieldMap;                     // 1byte
    tf_t _currentTF;                          // 4byte
    tf_t _totalTF;                            // 4byte
    df_t volatile _df;                        // 4byte

    docid_t _lastDocId;                // 4byte
    docpayload_t _lastDocPayload;      // 2byte
    fieldmap_t _lastFieldMap;          // 1byte
    index::CompressMode _compressMode; // 1byte
    // volatile for realtime
    PositionBitmapWriter* _tfBitmapWriter; // 8byte
    BufferedSkipListWriter* volatile _docSkipListWriter;
    DocListFormat* _docListFormat;
    autil::mem_pool::Pool* _byteSlicePool;

    friend class DocListEncoderTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
