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
#include "indexlib/index/kkv/dump/KKVDataDumperBase.h"
#include "indexlib/index/kkv/dump/KKVValueDumper.h"
#include "indexlib/index/kkv/dump/NormalSKeyDumper.h"
namespace indexlibv2::index {

template <typename SKeyType>
class NormalKKVDataDumper final : public KKVDataDumperBase
{
public:
    NormalKKVDataDumper(const std::shared_ptr<config::KKVIndexConfig>& kkvIndexConfig, bool storeTs,
                        KKVDumpPhrase phrase)
        : KKVDataDumperBase(kkvIndexConfig, storeTs, phrase)
    {
    }
    ~NormalKKVDataDumper() = default;

public:
    size_t GetMaxValueLen() const override
    {
        assert(_valueDumper);
        return _valueDumper->GetMaxValueLen();
    }

    size_t GetTotalSKeyCount() const override
    {
        assert(_skeyDumper);
        return _skeyDumper->GetTotalSKeyCount();
    }

    uint32_t GetMaxSKeyCount() const override
    {
        assert(_skeyDumper);
        return _skeyDumper->GetMaxSKeyCount();
    }

    bool IsValueInline() const override { return false; }

protected:
    Status InitSkeyAndValueDumper(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                  const indexlib::file_system::WriterOption& skeyOption,
                                  const indexlib::file_system::WriterOption& valueOption) override
    {
        assert(directory);
        _skeyDumper = std::make_unique<NormalSKeyDumper<SKeyType>>(_storeTs, _kkvIndexConfig->StoreExpireTime());
        RETURN_STATUS_DIRECTLY_IF_ERROR(_skeyDumper->Init(directory, skeyOption));

        auto valueConfig = _kkvIndexConfig->GetValueConfig();
        assert(valueConfig);
        _valueDumper = std::make_unique<KKVValueDumper>(valueConfig->GetFixedLength());
        return _valueDumper->Init(directory, valueOption);
    }

    Status CloseSkeyAndValueDumper() override
    {
        assert(_valueDumper);
        assert(_skeyDumper);

        RETURN_STATUS_DIRECTLY_IF_ERROR(_valueDumper->Close());
        return _skeyDumper->Close();
    }

    Status DoDump(uint64_t pkey, bool pkeyDeleted, bool isLastSkey, const KKVDoc& doc) override
    {
        assert(_valueDumper);
        assert(_skeyDumper);
        assert(_pkeyDumper);

        auto valueOffset = ChunkItemOffset::Invalid();
        if (!pkeyDeleted && !doc.skeyDeleted) {
            assert(doc.value.size() > 0);
            auto [status, offset] = _valueDumper->Dump(doc.value);
            RETURN_IF_STATUS_ERROR(status, "dump value failed");
            valueOffset = offset;
        }

        auto [status, skeyOffset] = _skeyDumper->Dump(pkeyDeleted, doc.skeyDeleted, isLastSkey, doc.skey, doc.timestamp,
                                                      doc.expireTime, valueOffset);
        RETURN_IF_STATUS_ERROR(status, "dump skey failed");

        RETURN_IF_STATUS_ERROR(_pkeyDumper->Dump(pkey, skeyOffset), "dump pkey failed");
        return Status::OK();
    }

private:
    std::unique_ptr<NormalSKeyDumper<SKeyType>> _skeyDumper;
    std::unique_ptr<KKVValueDumper> _valueDumper;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, NormalKKVDataDumper, SKeyType);

} // namespace indexlibv2::index