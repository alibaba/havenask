#include "indexlib/index/normal/adaptive_bitmap/multi_adaptive_bitmap_index_writer.h"
#include "indexlib/config/high_frequency_vocabulary.h"
#include "indexlib/config/high_freq_vocabulary_creator.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiAdaptiveBitmapIndexWriter);


MultiAdaptiveBitmapIndexWriter::~MultiAdaptiveBitmapIndexWriter() 
{
}

void MultiAdaptiveBitmapIndexWriter::EndPosting()
{
    HighFrequencyVocabularyPtr vocabulary = 
        HighFreqVocabularyCreator::CreateAdaptiveVocabulary(mIndexConfig->GetIndexName(),
                mIndexConfig->GetIndexType(), mIndexConfig->GetDictConfig());
    assert(vocabulary);
    for (auto writer : mAdaptiveBitmapIndexWriters)
    {
        auto& dictKeys = writer->GetAdaptiveDictKeys();
        for (auto key : dictKeys)
        {
            vocabulary->AddKey(key);            
        }
    }

    vocabulary->DumpTerms(mAdaptiveDictFolder);
}

IE_NAMESPACE_END(index);

