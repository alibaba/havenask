#ifndef __INDEXLIB_ENCODER_PROVIDER_H
#define __INDEXLIB_ENCODER_PROVIDER_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/singleton.h"
#include "indexlib/common/numeric_compress/int_encoder.h"
#include "indexlib/index_define.h"

IE_NAMESPACE_BEGIN(common);

class EncoderProvider : public misc::Singleton<EncoderProvider>
{
public:
    EncoderProvider();
    ~EncoderProvider();

public:
    
    /**
     * Get encoder for doc-list
     */
    const Int32Encoder* GetDocListEncoder(uint8_t mode = index::PFOR_DELTA_COMPRESS_MODE) const;

    /**
     * Get encoder for tf-list
     */
    const Int32Encoder* GetTfListEncoder(uint8_t mode = index::PFOR_DELTA_COMPRESS_MODE) const;

    /**
     * Get encoder for pos-list
     */
    const Int32Encoder* GetPosListEncoder(uint8_t mode = index::PFOR_DELTA_COMPRESS_MODE) const;

    /**
     * Get encoder for field map (single byte)
     */
    const Int8Encoder* GetFieldMapEncoder(uint8_t mode = index::PFOR_DELTA_COMPRESS_MODE) const;

    /**
     * Get encoder for doc payload
     */
    const Int16Encoder* GetDocPayloadEncoder(uint8_t mode = index::PFOR_DELTA_COMPRESS_MODE) const;

    /**
     * Get encoder for pos payload
     */
    const Int8Encoder* GetPosPayloadEncoder(uint8_t mode = index::PFOR_DELTA_COMPRESS_MODE) const;

    /**
     * Get encoder for offset-list
     */
    const Int32Encoder* GetSkipListEncoder(uint8_t mode = index::PFOR_DELTA_COMPRESS_MODE) const;
    
    void DisableSseOptimize()
    { mDisableSseOptimize = true; }

protected:
    void Init();
    const Int32Encoder * GetInt32PForDeltaEncoder() const
    { return mDisableSseOptimize ? mNoSseInt32PForDeltaEncoder.get() : mInt32PForDeltaEncoder.get(); }

protected:
    Int32EncoderPtr mInt32PForDeltaEncoder;
    Int32EncoderPtr mNoSseInt32PForDeltaEncoder;
    Int16EncoderPtr mInt16PForDeltaEncoder;
    Int8EncoderPtr mInt8PForDeltaEncoder;

    Int32EncoderPtr mInt32NoCompressEncoder;
    Int16EncoderPtr mInt16NoCompressEncoder;
    Int8EncoderPtr mInt8NoCompressEncoder;
    Int32EncoderPtr mInt32NoCompressNoLengthEncoder;
    Int32EncoderPtr mInt32ReferenceCompressEncoder;

    Int32EncoderPtr mInt32VByteEncoder;
    bool mDisableSseOptimize;
    
private:
    IE_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////////
//

inline const Int32Encoder* EncoderProvider::GetDocListEncoder(uint8_t mode) const
{
    if (mode == index::PFOR_DELTA_COMPRESS_MODE)
    {
        return GetInt32PForDeltaEncoder();
    }
    else if (mode == index::SHORT_LIST_COMPRESS_MODE)
    {
        return mInt32NoCompressEncoder.get();
    }
    else if (mode == index::REFERENCE_COMPRESS_MODE)
    {
        return mInt32ReferenceCompressEncoder.get();
    }
    return NULL;
}

inline const Int32Encoder* EncoderProvider::GetTfListEncoder(uint8_t mode) const
{
    if (mode == index::PFOR_DELTA_COMPRESS_MODE || mode == index::REFERENCE_COMPRESS_MODE)
    {
        return GetInt32PForDeltaEncoder();
    }
    else if (mode == index::SHORT_LIST_COMPRESS_MODE)
    {
        return mInt32VByteEncoder.get();
    }
    return NULL;
}

inline const Int32Encoder* EncoderProvider::GetPosListEncoder(uint8_t mode) const
{
    if (mode == index::PFOR_DELTA_COMPRESS_MODE || mode == index::REFERENCE_COMPRESS_MODE)
    {
        return GetInt32PForDeltaEncoder();
    }    
    else if (mode == index::SHORT_LIST_COMPRESS_MODE)
    {
        return mInt32VByteEncoder.get();
    }
    return NULL;
}

inline const Int8Encoder* EncoderProvider::GetFieldMapEncoder(uint8_t mode) const
{
    if (mode == index::PFOR_DELTA_COMPRESS_MODE || mode == index::REFERENCE_COMPRESS_MODE)
    {
        return mInt8PForDeltaEncoder.get();
    }
    else if (mode == index::SHORT_LIST_COMPRESS_MODE)
    {
        return mInt8NoCompressEncoder.get();
    }
    return NULL;
}

inline const Int16Encoder* EncoderProvider::GetDocPayloadEncoder(uint8_t mode) const
{
    if (mode == index::PFOR_DELTA_COMPRESS_MODE || mode == index::REFERENCE_COMPRESS_MODE)
    {
        return mInt16PForDeltaEncoder.get();
    }    
    else if (mode == index::SHORT_LIST_COMPRESS_MODE)
    {
        return mInt16NoCompressEncoder.get();
    }
    return NULL;
}

inline const Int8Encoder* EncoderProvider::GetPosPayloadEncoder(uint8_t mode) const
{
    if (mode == index::PFOR_DELTA_COMPRESS_MODE || mode == index::REFERENCE_COMPRESS_MODE)
    {
        return mInt8PForDeltaEncoder.get();
    }
    else if (mode == index::SHORT_LIST_COMPRESS_MODE)
    {
        return mInt8NoCompressEncoder.get();
    }
    return NULL;
}

inline const Int32Encoder* EncoderProvider::GetSkipListEncoder(uint8_t mode) const
{
    if (mode == index::PFOR_DELTA_COMPRESS_MODE)
    {
        return GetInt32PForDeltaEncoder();
    }
    else if (mode == index::SHORT_LIST_COMPRESS_MODE)
    {
        return mInt32NoCompressNoLengthEncoder.get();
    }
    else if (mode == index::REFERENCE_COMPRESS_MODE)
    {
        return mInt32NoCompressEncoder.get();
    }
    return NULL;
}

typedef std::tr1::shared_ptr<EncoderProvider> EncoderProviderPtr;

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ENCODER_PROVIDER_H
