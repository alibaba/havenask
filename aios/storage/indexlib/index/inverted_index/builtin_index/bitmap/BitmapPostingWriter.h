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

#include "autil/mem_pool/PoolBase.h"
#include "fslib/fs/File.h"
#include "indexlib/file_system/ByteSliceWriter.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/util/ExpandableBitmap.h"

namespace indexlib::index {

class BitmapPostingWriter : public index::PostingWriter
{
public:
    BitmapPostingWriter(autil::mem_pool::PoolBase* pool = NULL);
    ~BitmapPostingWriter();

public:
    void AddPosition(pos_t pos, pospayload_t posPayload, fieldid_t fieldId) override
    {
        _currentTF++;
        _totalTF++;
    }

    void Update(docid_t docId, bool isDelete);

    void EndDocument(docid_t docId, docpayload_t docPayload) override;
    void EndDocument(docid_t docId, docpayload_t docPayload, fieldmap_t fieldMap) override
    {
        return EndDocument(docId, docPayload);
    }

    void EndSegment() override {}

    void Dump(const std::shared_ptr<file_system::FileWriter>& file) override;

    uint32_t GetDumpLength() const override;

    uint32_t GetTotalTF() const override { return _totalTF; }

    uint32_t GetDF() const override { return _df; }

    bool NotExistInCurrentDoc() const override { return _currentTF == 0; }

    void SetTermPayload(termpayload_t payload) override { _termPayload = payload; }

    termpayload_t GetTermPayload() const override { return _termPayload; }

    docid_t GetLastDocId() const override { return _lastDocId; }

    void SetCurrentTF(tf_t tf) override { _currentTF = tf; }

    bool CreateReorderPostingWriter(autil::mem_pool::Pool* pool, const std::vector<docid_t>* newOrder,
                                    PostingWriter* output) const override;

    size_t GetEstimateDumpTempMemSize() const override { return _estimateDumpTempMemSize; }

public:
    void AddPosition(pos_t pos, pospayload_t posPayload) { AddPosition(pos, posPayload, 0); }

    const util::ExpandableBitmap& GetBitmap() const { return _bitmap; }

    const util::ExpandableBitmap* GetBitmapData() const { return &_bitmap; }

private:
    uint32_t _df;
    uint32_t _totalTF;
    termpayload_t _termPayload;
    util::ExpandableBitmap _bitmap;
    docid_t _lastDocId;
    tf_t _currentTF;
    size_t _estimateDumpTempMemSize = 0;

    static const uint32_t INIT_BITMAP_ITEM_NUM = 128 * 1024;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
