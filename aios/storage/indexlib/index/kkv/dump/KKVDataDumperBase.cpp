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
#include "indexlib/index/kkv/dump/KKVDataDumperBase.h"

#include "indexlib/index/kkv/common/KKVIndexFormat.h"
#include "indexlib/index/kkv/dump/KKVDocSorterFactory.h"

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.config, KKVDataDumperBase);

Status KKVDataDumperBase::Init(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                               const indexlib::file_system::WriterOption& skeyOption,
                               const indexlib::file_system::WriterOption& valueOption, size_t totalPkeyCount)
{
    assert(_kkvIndexConfig);
    _dumpDirectory = directory;
    const auto& indexPreference = _kkvIndexConfig->GetIndexPreference();
    _pkeyDumper.reset(new PKeyDumper());
    RETURN_STATUS_DIRECTLY_IF_ERROR(_pkeyDumper->Init(_dumpDirectory, indexPreference, totalPkeyCount));

    auto [status, kkvDocSorter] = KKVDocSorterFactory::CreateIfNeccessary(_kkvIndexConfig, _phrase);
    RETURN_IF_STATUS_ERROR(status, "create kkv doc sorter failed");
    _kkvDocSorter.reset(kkvDocSorter.release());

    return InitSkeyAndValueDumper(_dumpDirectory, skeyOption, valueOption);
}

Status KKVDataDumperBase::Dump(uint64_t pkey, bool isDeletedPKey, bool isLastSkey, const KKVDoc& kkvDoc)
{
    if (_kkvDocSorter == nullptr) {
        return DoDump(pkey, isDeletedPKey, isLastSkey, kkvDoc);
    } else {
        return SortDump(pkey, isDeletedPKey, isLastSkey, kkvDoc);
    }
}

Status KKVDataDumperBase::SortDump(uint64_t pkey, bool isDeletedPKey, bool isLastSkey, const KKVDoc& kkvDoc)
{
    assert(_kkvDocSorter);
    assert(_kkvDocSorter->Size() == 0 || pkey == _curPkey);

    _kkvDocSorter->AddDoc(isDeletedPKey, kkvDoc);
    _curPkey = pkey;
    if (!isLastSkey) {
        return Status::OK();
    }

    auto results = _kkvDocSorter->Sort();
    size_t size = results.size();
    for (uint32_t idx = 0; idx < size; ++idx) {
        bool isLastSkey = ((idx + 1) == size);
        auto& [isPKeyDeleted, doc] = results[idx];
        RETURN_STATUS_DIRECTLY_IF_ERROR(DoDump(pkey, isPKeyDeleted, isLastSkey, doc));
    }

    _kkvDocSorter->Reset();
    return Status::OK();
}

Status KKVDataDumperBase::Close()
{
    assert(_pkeyDumper);
    RETURN_STATUS_DIRECTLY_IF_ERROR(_pkeyDumper->Close());
    RETURN_STATUS_DIRECTLY_IF_ERROR(CloseSkeyAndValueDumper());
    return DumpMeta();
}

Status KKVDataDumperBase::DumpMeta()
{
    auto keepSkeySorted = _kkvDocSorter != nullptr && _kkvDocSorter->KeepSkeySorted();
    KKVIndexFormat format(_storeTs, keepSkeySorted, IsValueInline());
    return format.Store(_dumpDirectory);
}

} // namespace indexlibv2::index
