#ifndef __INDEXLIB_COMPRESS_FILE_META_H
#define __INDEXLIB_COMPRESS_FILE_META_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(file_system);

class CompressFileMeta
{
public:
    // first: uncompress endPos, second: compress endPos
    struct CompressBlockMeta
    {
        CompressBlockMeta(size_t _uncompressEndPos, size_t _compressEndPos)
            : uncompressEndPos(_uncompressEndPos)
            , compressEndPos(_compressEndPos)
        {}

        size_t uncompressEndPos;
        size_t compressEndPos;
    };

public:
    CompressFileMeta() {}
    ~CompressFileMeta() {}

public:
    bool Init(size_t blockCount, const char* buffer)
    {
        CompressBlockMeta* blockAddr = (CompressBlockMeta*)buffer;
        for (size_t i = 0; i < blockCount; i++)
        {
            if (!AddOneBlock(blockAddr[i]))
            {
                return false;
            }
        }
        return true;
    }

public:
    bool AddOneBlock(const CompressBlockMeta& blockMeta)
    {
        return AddOneBlock(blockMeta.uncompressEndPos, blockMeta.compressEndPos);
    }

    bool AddOneBlock(size_t uncompressEndPos, size_t compressEndPos)
    {
        if (mBlockMetas.empty())
        {
            mBlockMetas.push_back(CompressBlockMeta(uncompressEndPos, compressEndPos));
            return true;
        }

        if (mBlockMetas.back().uncompressEndPos >= uncompressEndPos ||
            mBlockMetas.back().compressEndPos >= compressEndPos)
        {
            return false;
        }
        mBlockMetas.push_back(CompressBlockMeta(uncompressEndPos, compressEndPos));
        return true;
    }

    size_t GetBlockCount() const
    {
        return mBlockMetas.size();
    }

    char* Data() const
    {
        return (char*)mBlockMetas.data();
    }

    size_t Size() const
    {
        return mBlockMetas.size() * sizeof(CompressBlockMeta);
    }

    const std::vector<CompressBlockMeta>& GetCompressBlockMetas() const
    {
        return mBlockMetas;
    }

    size_t GetMaxCompressBlockSize() const 
    {
        size_t maxBlockSize = 0;
        size_t preEndPos = 0;
        for (size_t i = 0; i < mBlockMetas.size(); ++i)
        {
            size_t curBlockSize = mBlockMetas[i].compressEndPos - preEndPos;
            maxBlockSize = std::max(maxBlockSize, curBlockSize);
            preEndPos = mBlockMetas[i].compressEndPos;
        }
        return maxBlockSize;
    }

    size_t GetUnCompressFileLength() const
    {
        return mBlockMetas.empty() ? 0 : mBlockMetas.back().uncompressEndPos;
    }

public:
    static size_t GetMetaLength(size_t blockCount)
    {
        return blockCount * sizeof(CompressBlockMeta);
    }

private:
    std::vector<CompressBlockMeta> mBlockMetas;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressFileMeta);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_COMPRESS_FILE_META_H
