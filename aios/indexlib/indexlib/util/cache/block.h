#ifndef __INDEXLIB_BLOCK_H
#define __INDEXLIB_BLOCK_H

IE_NAMESPACE_BEGIN(util);

struct BlockId
{
    BlockId() : fileId(0), inFileIdx(0) {}
    BlockId(uint64_t fileId_, uint64_t inFileIdx_)
        : fileId(fileId_)
        , inFileIdx(inFileIdx_)
    {}

    bool operator==(const BlockId& other) const
    {
        return fileId == other.fileId &&
            inFileIdx == other.inFileIdx;
    }
    
    uint64_t fileId;
    uint64_t inFileIdx;
};
typedef BlockId blockid_t;

struct Block
{
    Block(const blockid_t& id_, uint8_t* data_) : id(id_), data(data_) {}
    Block() : data(NULL) {}

    blockid_t id;
    uint8_t* data;
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BLOCK_H
