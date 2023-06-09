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
#include "indexlib/index/common/numeric_compress/EncoderProvider.h"

#include "indexlib/index/common/numeric_compress/GroupVint32Encoder.h"
#include "indexlib/index/common/numeric_compress/NewPfordeltaIntEncoder.h"
#include "indexlib/index/common/numeric_compress/NoCompressIntEncoder.h"
#include "indexlib/index/common/numeric_compress/ReferenceCompressIntEncoder.h"
#include "indexlib/index/common/numeric_compress/VbyteInt32Encoder.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, EncoderProvider);

EncoderProvider::EncoderProvider() : _disableSseOptimize(false) { Init(); }

EncoderProvider::~EncoderProvider() {}

void EncoderProvider::Init()
{
    _int32PForDeltaEncoder.reset(new NewPForDeltaInt32Encoder());
    _noSseInt32PForDeltaEncoder.reset(new NoSseNewPForDeltaInt32Encoder());
    _int16PForDeltaEncoder.reset(new NewPForDeltaInt16Encoder());
    _int8PForDeltaEncoder.reset(new NewPForDeltaInt8Encoder());

    _int32PForDeltaEncoderBlockOpt.reset(new NewPForDeltaInt32Encoder(true));
    _noSseInt32PForDeltaEncoderBlockOpt.reset(new NoSseNewPForDeltaInt32Encoder(true));
    _int16PForDeltaEncoderBlockOpt.reset(new NewPForDeltaInt16Encoder(true));
    _int8PForDeltaEncoderBlockOpt.reset(new NewPForDeltaInt8Encoder(true));

    _int32NoCompressEncoder.reset(new NoCompressInt32Encoder());
    _int16NoCompressEncoder.reset(new NoCompressInt16Encoder());
    _int8NoCompressEncoder.reset(new NoCompressInt8Encoder());

    _int32NoCompressNoLengthEncoder.reset(new NoCompressInt32Encoder(false));
    _int32VByteEncoder.reset(new VbyteInt32Encoder());
    _int32ReferenceCompressEncoder.reset(new ReferenceCompressInt32Encoder());
}
} // namespace indexlib::index
