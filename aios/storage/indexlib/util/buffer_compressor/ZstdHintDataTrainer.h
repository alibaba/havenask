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
#include <zstd.h>

#include "indexlib/util/buffer_compressor/CompressHintDataTrainer.h"

namespace indexlib { namespace util {

class ZstdHintDataTrainer : public CompressHintDataTrainer
{
public:
    ZstdHintDataTrainer();
    ~ZstdHintDataTrainer();

    ZstdHintDataTrainer(const ZstdHintDataTrainer&) = delete;
    ZstdHintDataTrainer& operator=(const ZstdHintDataTrainer&) = delete;
    ZstdHintDataTrainer(ZstdHintDataTrainer&&) = delete;
    ZstdHintDataTrainer& operator=(ZstdHintDataTrainer&&) = delete;

public:
    bool TrainHintData() override;

protected:
    bool PrepareHintDataBuffer() noexcept override;

private:
    std::vector<char> _sampleDataBuf;
    std::vector<size_t> _sampleDataLenVec;
    ZSTD_CCtx* _cctx;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ZstdHintDataTrainer> ZstdHintDataTrainerPtr;

}} // namespace indexlib::util
