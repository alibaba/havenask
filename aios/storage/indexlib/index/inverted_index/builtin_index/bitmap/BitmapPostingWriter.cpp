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
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"

#include "autil/memory.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/util/NumericUtil.h"

namespace indexlib::index {
namespace {
using indexlib::util::Bitmap;
}

AUTIL_LOG_SETUP(indexlib.index, BitmapPostingWriter);

BitmapPostingWriter::BitmapPostingWriter(autil::mem_pool::PoolBase* pool)
    : _df(0)
    , _totalTF(0)
    , _termPayload(0)
    , _bitmap(INIT_BITMAP_ITEM_NUM, false, pool)
    , _lastDocId(INVALID_DOCID)
    , _currentTF(0)
{
}

BitmapPostingWriter::~BitmapPostingWriter() {}

void BitmapPostingWriter::EndDocument(docid_t docId, docpayload_t docPayload)
{
    _df++;
    _currentTF = 0;
    _bitmap.Set(docId);
    _lastDocId = docId;
    _estimateDumpTempMemSize = _bitmap.Size();
}

void BitmapPostingWriter::Update(docid_t docId, bool isDelete)
{
    if (isDelete) {
        if (_bitmap.Reset(docId)) {
            --_df;
        }
    } else {
        if (_bitmap.Set(docId)) {
            ++_df;
        }
    }
    _currentTF = 0;
    _lastDocId = std::max(_lastDocId, docId);
}

void BitmapPostingWriter::Dump(const std::shared_ptr<file_system::FileWriter>& file)
{
    TermMeta termMeta(GetDF(), GetTotalTF(), GetTermPayload());
    TermMetaDumper tmDumper;
    tmDumper.Dump(file, termMeta);

    uint32_t size = util::NumericUtil::UpperPack(_lastDocId + 1, Bitmap::SLOT_SIZE);
    size = size / Bitmap::BYTE_SLOT_NUM;
    file->Write((void*)&size, sizeof(uint32_t)).GetOrThrow();
    file->Write((void*)_bitmap.GetData(), size).GetOrThrow();
}

uint32_t BitmapPostingWriter::GetDumpLength() const
{
    TermMeta termMeta(GetDF(), GetTotalTF(), GetTermPayload());
    TermMetaDumper tmDumper;

    uint32_t size = util::NumericUtil::UpperPack(_lastDocId + 1, Bitmap::SLOT_SIZE);
    size = size / Bitmap::BYTE_SLOT_NUM;
    return size + sizeof(uint32_t) + tmDumper.CalculateStoreSize(termMeta);
}

bool BitmapPostingWriter::CreateReorderPostingWriter(autil::mem_pool::Pool* pool, const std::vector<docid_t>* newOrder,
                                                     PostingWriter* output) const
{
    auto dumpWriter = dynamic_cast<BitmapPostingWriter*>(output);
    assert(typeid(pool) == typeid(autil::mem_pool::Pool*));
    assert(newOrder);
    assert(dumpWriter);

    dumpWriter->_totalTF = _totalTF;
    dumpWriter->_termPayload = _termPayload;
    for (uint32_t docId = _bitmap.Begin(); docId != Bitmap::INVALID_INDEX; docId = _bitmap.Next(docId)) {
        dumpWriter->Update(newOrder->at(docId), false);
    }
    return true;
}

} // namespace indexlib::index
