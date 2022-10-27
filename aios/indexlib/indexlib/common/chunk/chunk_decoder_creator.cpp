#include "indexlib/common/chunk/chunk_decoder_creator.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ChunkDecoderCreator);

SimpleCachedChunkDecoder ChunkDecoderCreator::mSimpleCacheDecoder;

ChunkDecoderCreator::ChunkDecoderCreator() 
{
}

ChunkDecoderCreator::~ChunkDecoderCreator() 
{
}

IE_NAMESPACE_END(common);

