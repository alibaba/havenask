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

#include "indexlib/index/common/numeric_compress/IntEncoder.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/util/Singleton.h"

namespace indexlib::index {

class EncoderProvider : public util::Singleton<EncoderProvider>
{
public:
    struct EncoderParam {
    public:
        explicit EncoderParam(uint8_t _mode = PFOR_DELTA_COMPRESS_MODE, bool _shortListVbyteCompress = false,
                              bool _enableP4DeltaBlockOpt = false)
            : mode(_mode)
            , shortListVbyteCompress(_shortListVbyteCompress)
            , enableP4DeltaBlockOpt(_enableP4DeltaBlockOpt)
        {
        }
        uint8_t mode = PFOR_DELTA_COMPRESS_MODE;
        bool shortListVbyteCompress = false;
        bool enableP4DeltaBlockOpt = false;
    };

public:
    EncoderProvider();
    ~EncoderProvider();

public:
    /**
     * Get encoder for doc-list
     */
    const Int32Encoder* GetDocListEncoder(const EncoderParam& param = EncoderParam()) const;

    /**
     * Get encoder for tf-list
     */
    const Int32Encoder* GetTfListEncoder(const EncoderParam& param = EncoderParam()) const;

    /**
     * Get encoder for pos-list
     */
    const Int32Encoder* GetPosListEncoder(const EncoderParam& param = EncoderParam()) const;

    /**
     * Get encoder for field map (single byte)
     */
    const Int8Encoder* GetFieldMapEncoder(const EncoderParam& param = EncoderParam()) const;

    /**
     * Get encoder for doc payload
     */
    const Int16Encoder* GetDocPayloadEncoder(const EncoderParam& param = EncoderParam()) const;

    /**
     * Get encoder for pos payload
     */
    const Int8Encoder* GetPosPayloadEncoder(const EncoderParam& param = EncoderParam()) const;

    /**
     * Get encoder for offset-list
     */
    const Int32Encoder* GetSkipListEncoder(const EncoderParam& param = EncoderParam()) const;

    void DisableSseOptimize() { _disableSseOptimize = true; }

protected:
    void Init();

    const Int32Encoder* GetInt32PForDeltaEncoder(bool enableBlockOpt) const
    {
        if (enableBlockOpt) {
            return _disableSseOptimize ? _noSseInt32PForDeltaEncoderBlockOpt.get()
                                       : _int32PForDeltaEncoderBlockOpt.get();
        }
        return _disableSseOptimize ? _noSseInt32PForDeltaEncoder.get() : _int32PForDeltaEncoder.get();
    }

protected:
    std::shared_ptr<Int32Encoder> _int32PForDeltaEncoder;
    std::shared_ptr<Int32Encoder> _noSseInt32PForDeltaEncoder;
    std::shared_ptr<Int16Encoder> _int16PForDeltaEncoder;
    std::shared_ptr<Int8Encoder> _int8PForDeltaEncoder;

    std::shared_ptr<Int32Encoder> _int32PForDeltaEncoderBlockOpt;
    std::shared_ptr<Int32Encoder> _noSseInt32PForDeltaEncoderBlockOpt;
    std::shared_ptr<Int16Encoder> _int16PForDeltaEncoderBlockOpt;
    std::shared_ptr<Int8Encoder> _int8PForDeltaEncoderBlockOpt;

    std::shared_ptr<Int32Encoder> _int32NoCompressEncoder;
    std::shared_ptr<Int16Encoder> _int16NoCompressEncoder;
    std::shared_ptr<Int8Encoder> _int8NoCompressEncoder;
    std::shared_ptr<Int32Encoder> _int32NoCompressNoLengthEncoder;
    std::shared_ptr<Int32Encoder> _int32ReferenceCompressEncoder;

    std::shared_ptr<Int32Encoder> _int32VByteEncoder;
    bool _disableSseOptimize;

private:
    friend class PostingFormatTest;

private:
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////////
//

inline const Int32Encoder* EncoderProvider::GetDocListEncoder(const EncoderParam& param) const
{
    if (param.mode == PFOR_DELTA_COMPRESS_MODE) {
        return GetInt32PForDeltaEncoder(param.enableP4DeltaBlockOpt);
    } else if (param.mode == SHORT_LIST_COMPRESS_MODE) {
        return param.shortListVbyteCompress ? _int32VByteEncoder.get() : _int32NoCompressEncoder.get();
    } else if (param.mode == REFERENCE_COMPRESS_MODE) {
        return _int32ReferenceCompressEncoder.get();
    }
    return NULL;
}

inline const Int32Encoder* EncoderProvider::GetTfListEncoder(const EncoderParam& param) const
{
    if (param.mode == PFOR_DELTA_COMPRESS_MODE || param.mode == REFERENCE_COMPRESS_MODE) {
        return GetInt32PForDeltaEncoder(param.enableP4DeltaBlockOpt);
    } else if (param.mode == SHORT_LIST_COMPRESS_MODE) {
        return _int32VByteEncoder.get();
    }
    return NULL;
}

inline const Int32Encoder* EncoderProvider::GetPosListEncoder(const EncoderParam& param) const
{
    if (param.mode == PFOR_DELTA_COMPRESS_MODE || param.mode == REFERENCE_COMPRESS_MODE) {
        return GetInt32PForDeltaEncoder(param.enableP4DeltaBlockOpt);
    } else if (param.mode == SHORT_LIST_COMPRESS_MODE) {
        return _int32VByteEncoder.get();
    }
    return NULL;
}

inline const Int8Encoder* EncoderProvider::GetFieldMapEncoder(const EncoderParam& param) const
{
    if (param.mode == PFOR_DELTA_COMPRESS_MODE || param.mode == REFERENCE_COMPRESS_MODE) {
        return param.enableP4DeltaBlockOpt ? _int8PForDeltaEncoderBlockOpt.get() : _int8PForDeltaEncoder.get();
    } else if (param.mode == SHORT_LIST_COMPRESS_MODE) {
        return _int8NoCompressEncoder.get();
    }
    return NULL;
}

inline const Int16Encoder* EncoderProvider::GetDocPayloadEncoder(const EncoderParam& param) const
{
    if (param.mode == PFOR_DELTA_COMPRESS_MODE || param.mode == REFERENCE_COMPRESS_MODE) {
        return param.enableP4DeltaBlockOpt ? _int16PForDeltaEncoderBlockOpt.get() : _int16PForDeltaEncoder.get();
    } else if (param.mode == SHORT_LIST_COMPRESS_MODE) {
        return _int16NoCompressEncoder.get();
    }
    return NULL;
}

inline const Int8Encoder* EncoderProvider::GetPosPayloadEncoder(const EncoderParam& param) const
{
    if (param.mode == PFOR_DELTA_COMPRESS_MODE || param.mode == REFERENCE_COMPRESS_MODE) {
        return param.enableP4DeltaBlockOpt ? _int8PForDeltaEncoderBlockOpt.get() : _int8PForDeltaEncoder.get();
    } else if (param.mode == SHORT_LIST_COMPRESS_MODE) {
        return _int8NoCompressEncoder.get();
    }
    return NULL;
}

inline const Int32Encoder* EncoderProvider::GetSkipListEncoder(const EncoderParam& param) const
{
    if (param.mode == PFOR_DELTA_COMPRESS_MODE) {
        return GetInt32PForDeltaEncoder(param.enableP4DeltaBlockOpt);
    } else if (param.mode == SHORT_LIST_COMPRESS_MODE) {
        return _int32NoCompressNoLengthEncoder.get();
    } else if (param.mode == REFERENCE_COMPRESS_MODE) {
        return _int32NoCompressEncoder.get();
    }
    return NULL;
}

} // namespace indexlib::index
