#include <string>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/config/index_config.h"
#include "indexlib/config/impl/index_config_impl.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/dictionary_schema.h"
#include "indexlib/config/adaptive_dictionary_config.h"
#include "indexlib/config/truncate_term_vocabulary.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, IndexConfigImpl);

void IndexConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (mVirtual)
    {
        return;
    }
    map<string, Any> jsonMap = json.GetMap();
    //TODO split to PackIndexConfig and SingleIndexConfig
    if (json.GetMode() == TO_JSON)
    {
        json.Jsonize(INDEX_NAME, mIndexName);
        if (!mAnalyzer.empty())
        {
            json.Jsonize(INDEX_ANALYZER, mAnalyzer);
        }
        string typeStr = IndexConfig::IndexTypeToStr(mIndexType);
        json.Jsonize(INDEX_TYPE, typeStr);

        int enableValue = 1;
        int disableValue = 0;
	
        if (mHasTruncate)
        {
            json.Jsonize(HAS_TRUNCATE, mHasTruncate);
            if (!mUseTruncateProfiles.empty())
            {
                json.Jsonize(USE_TRUNCATE_PROFILES, mUseTruncateProfiles);
            }
        }

        if (mDictConfig.get())
        {
            string dictName = mDictConfig->GetDictName();
            json.Jsonize(HIGH_FEQUENCY_DICTIONARY, dictName);
        }
        
        if (mAdaptiveDictConfig.get())
        {
            string ruleName = mAdaptiveDictConfig->GetRuleName();
            json.Jsonize(HIGH_FEQUENCY_ADAPTIVE_DICTIONARY, ruleName);
        }

        if (mDictConfig || mAdaptiveDictConfig)
        {
            string postingType;
            if (mHighFreqencyTermPostingType == hp_bitmap)
            {
                postingType = HIGH_FREQ_TERM_BITMAP_POSTING;
            }
            else
            {
                assert(mHighFreqencyTermPostingType == hp_both);
                postingType = HIGH_FREQ_TERM_BOTH_POSTING;
            }
            json.Jsonize(HIGH_FEQUENCY_TERM_POSTING_TYPE, postingType);
        }
        
        if (mShardingType == IndexConfig::IST_NEED_SHARDING)
        {
            bool needSharding = true;
            int32_t shardingCount = (int32_t)mShardingIndexConfigs.size();
            json.Jsonize(NEED_SHARDING, needSharding);
            json.Jsonize(SHARDING_COUNT, shardingCount);
        }

        if (mIndexType == it_pack || mIndexType == it_expack 
            || mIndexType == it_text)
        {    
            if (mOptionFlag & of_term_payload)
            {
                json.Jsonize(TERM_PAYLOAD_FLAG, enableValue);
            }
            else
            {
                json.Jsonize(TERM_PAYLOAD_FLAG, disableValue);
            }

            if (mOptionFlag & of_doc_payload)
            {
                json.Jsonize(DOC_PAYLOAD_FLAG, enableValue);
            }
            else
            {
                json.Jsonize(DOC_PAYLOAD_FLAG, disableValue);
            }

            if (mOptionFlag & of_position_payload)
            {
                json.Jsonize(POSITION_PAYLOAD_FLAG, enableValue);
            }
            else
            {
                json.Jsonize(POSITION_PAYLOAD_FLAG, disableValue);
            }

            if (mOptionFlag & of_position_list)
            {
                json.Jsonize(POSITION_LIST_FLAG, enableValue);
            }
            else
            {
                json.Jsonize(POSITION_LIST_FLAG, disableValue);
            }
            
            if (mOptionFlag & of_tf_bitmap)
            {
                json.Jsonize(TERM_FREQUENCY_BITMAP, enableValue);
            }
            else
            {
                json.Jsonize(TERM_FREQUENCY_BITMAP, disableValue);
            }
        } 
        else if (mIndexType == it_string
                 || mIndexType == it_number
                 || mIndexType == it_number_int8
                 || mIndexType == it_number_uint8
                 || mIndexType == it_number_int16
                 || mIndexType == it_number_uint16
                 || mIndexType == it_number_int32
                 || mIndexType == it_number_uint32
                 || mIndexType == it_number_int64
                 || mIndexType == it_number_uint64)
        {
            if (mOptionFlag & of_term_payload)
            {
                json.Jsonize(TERM_PAYLOAD_FLAG, enableValue);
            }
            else
            {
                json.Jsonize(TERM_PAYLOAD_FLAG, disableValue);
            }

            if (mOptionFlag & of_doc_payload)
            {
                json.Jsonize(DOC_PAYLOAD_FLAG, enableValue);
            }
            else
            {
                json.Jsonize(DOC_PAYLOAD_FLAG, disableValue);
            }
        }
        if ((mIndexType != it_primarykey64) && 
            (mIndexType != it_primarykey128) &&
            (mIndexType != it_kv) &&
            (mIndexType != it_kkv) && 
            (mIndexType != it_trie) &&
            (mIndexType != it_spatial) &&
            (mIndexType != it_date) &&
            (mIndexType != it_range) &&
            !(mOptionFlag & of_term_frequency))
        {
            json.Jsonize(TERM_FREQUENCY_FLAG, disableValue);
        }
        // todo: jsonize DICT_INLINE_COMPRESS to be compatible with legacy binary,
        // remove in the next release
        JsonizeDictInlineFlagForLegacyBinary(json);
        
        if (mIsReferenceCompress)
        {
            string indexCompressMode = INDEX_COMPRESS_MODE_REFERENCE;
            json.Jsonize(INDEX_COMPRESS_MODE, indexCompressMode);
        }

        if (mIsHashTypedDictionary)
        {
            json.Jsonize(USE_HASH_DICTIONARY, mIsHashTypedDictionary);
        }
        
        if (mCustomizedConfigs.size() > 1)
        {
            vector<Any> anyVec;
            for (size_t i = 0; i < mCustomizedConfigs.size(); i++)
            {
                anyVec.push_back(ToJson(*mCustomizedConfigs[i]));
            }
            json.Jsonize(CUSTOMIZED_CONFIG, anyVec);
        }
    }
    else
    {
        string postingType = HIGH_FREQ_TERM_BITMAP_POSTING;
        json.Jsonize(HIGH_FEQUENCY_TERM_POSTING_TYPE, postingType, postingType);        
        if (postingType == HIGH_FREQ_TERM_BOTH_POSTING)
        {
            mHighFreqencyTermPostingType = hp_both;
        }
        else if (postingType == HIGH_FREQ_TERM_BITMAP_POSTING)
        {
            mHighFreqencyTermPostingType = hp_bitmap;
        }
        else
        {
            INDEXLIB_FATAL_ERROR(UnSupported, "High frequency term posting list" 
                    " type(%s) doesn't support.", postingType.c_str());
        }
        json.Jsonize(HAS_TRUNCATE, mHasTruncate, mHasTruncate);
        if (mHasTruncate)
        {
            json.Jsonize(USE_TRUNCATE_PROFILES, mUseTruncateProfiles, mUseTruncateProfiles);
        }

        string compressMode;
        json.Jsonize(INDEX_COMPRESS_MODE, compressMode, INDEX_COMPRESS_MODE_PFOR_DELTA);
        mIsReferenceCompress = (compressMode == INDEX_COMPRESS_MODE_REFERENCE) ? true : false;
        json.Jsonize(USE_HASH_DICTIONARY, mIsHashTypedDictionary, mIsHashTypedDictionary);

        auto iter = jsonMap.find(CUSTOMIZED_CONFIG);
        if (iter != jsonMap.end())
        {

            JsonArray customizedConfigVec = AnyCast<JsonArray>(iter->second);
            for (JsonArray::iterator configIter = customizedConfigVec.begin(); 
                 configIter != customizedConfigVec.end(); ++configIter)
            {
                JsonMap customizedConfigMap = AnyCast<JsonMap>(*configIter);
                Jsonizable::JsonWrapper jsonWrapper(customizedConfigMap);
                CustomizedConfigPtr customizedConfig(new CustomizedConfig());
                customizedConfig->Jsonize(jsonWrapper);
                mCustomizedConfigs.push_back(customizedConfig);
            }
        }

    }
}

void IndexConfigImpl::JsonizeDictInlineFlagForLegacyBinary(
        legacy::Jsonizable::JsonWrapper& json)
{
    if (mIndexType == it_primarykey64 || mIndexType == it_primarykey128 ||
        mIndexType == it_kv || mIndexType == it_kkv ||
        mIndexType == it_trie)
    {
        return;
    }
    if ((mOptionFlag & of_position_list)
         || ((mOptionFlag & of_term_frequency)
             && (mOptionFlag & of_tf_bitmap)))
    {
        return;
    }
    bool trueVar = true;
    json.Jsonize(HAS_DICT_INLINE_COMPRESS, trueVar);
}


void IndexConfigImpl::Check() const
{
    if ((mOptionFlag & of_position_payload)
        && !(mOptionFlag & of_position_list))
    {
        INDEXLIB_FATAL_ERROR(Schema, "position payload flag is 1 but no position list.");
    }

    if (!mDictConfig && !mAdaptiveDictConfig 
        && mHighFreqencyTermPostingType == hp_both)
    {
        INDEXLIB_FATAL_ERROR(Schema, "high_frequency_term_posting_type is set to both"
               " without high_frequency_dictionary/high_frequency_adaptive_dictionary");
    }

    if (mIsReferenceCompress && 
        ((mOptionFlag & of_term_frequency) && !(mOptionFlag & of_tf_bitmap)))
    {
        INDEXLIB_FATAL_ERROR(Schema,
                             "reference_compress does not support tf(not tf_bitmap)");
    }
}

bool IndexConfigImpl::HasTruncateProfile(const std::string& truncateProfileName) const
{
    assert(mHasTruncate);

    if (mUseTruncateProfiles.empty())
    {
        return true;
    }
    StringTokenizer st(mUseTruncateProfiles, USE_TRUNCATE_PROFILES_SEPRATOR,
                       StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it)
    {
        if (truncateProfileName == *it)
        {
            return true;
        }
    }
    return false;
}

vector<string> IndexConfigImpl::GetUseTruncateProfiles() const
{
    vector<string> profiles;
    StringTokenizer st(mUseTruncateProfiles, USE_TRUNCATE_PROFILES_SEPRATOR,
                       StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
    return st.getTokenVector();
}

void IndexConfigImpl::AssertEqual(const IndexConfigImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mIndexId, other.mIndexId, "mIndexId not equal");
    IE_CONFIG_ASSERT_EQUAL(mIndexName, other.mIndexName, "mIndexName not equal");
    IE_CONFIG_ASSERT_EQUAL(mIndexType, other.mIndexType, "mIndexType not equal");
    IE_CONFIG_ASSERT_EQUAL(mShardingType, other.mShardingType, "mShardingType not equal");
    IE_CONFIG_ASSERT_EQUAL(mOptionFlag, other.mOptionFlag, "mOptionFlag not equal");
    IE_CONFIG_ASSERT_EQUAL(mShardingIndexConfigs.size(), other.mShardingIndexConfigs.size(),
                           "mShardingIndexConfigs size not equal");
    for (size_t i = 0; i < mShardingIndexConfigs.size(); i++)
    {
        mShardingIndexConfigs[i]->AssertEqual(*other.mShardingIndexConfigs[i]);
    }
    for (size_t i = 0; i < mCustomizedConfigs.size(); i++)
    {
        mCustomizedConfigs[i]->AssertEqual(*other.mCustomizedConfigs[i]);
    }
}

void IndexConfigImpl::LoadTruncateTermVocabulary(
        const storage::ArchiveFolderPtr& metaFolder,
        const vector<string>& truncIndexNames)
{
    TruncateTermVocabularyPtr truncTermVocabulary(
            new TruncateTermVocabulary(metaFolder));

    truncTermVocabulary->Init(truncIndexNames);
    if (truncTermVocabulary->GetTermCount() > 0)
    {
        mTruncateTermVocabulary = truncTermVocabulary;
    }
}

void IndexConfigImpl::LoadTruncateTermVocabulary(
        const file_system::DirectoryPtr& truncMetaDir, 
        const vector<string>& truncIndexNames)
{
    TruncateTermVocabularyPtr truncTermVocabulary(
            new TruncateTermVocabulary(storage::ArchiveFolderPtr()));

    truncTermVocabulary->Init(truncMetaDir, truncIndexNames);
    if (truncTermVocabulary->GetTermCount() > 0)
    {
        mTruncateTermVocabulary = truncTermVocabulary;
    }
}

bool IndexConfigImpl::IsTruncateTerm(dictkey_t key) const
{
    if (!mTruncateTermVocabulary)
    {
        return false;
    }
    return mTruncateTermVocabulary->Lookup(key);
}

bool IndexConfigImpl::GetTruncatePostingCount(dictkey_t key, 
        int32_t& count) const
{
    count = 0;
    if (!mTruncateTermVocabulary)
    {
        return false;
    }
    return mTruncateTermVocabulary->LookupTF(key, count);
}

bool IndexConfigImpl::IsBitmapOnlyTerm(dictkey_t key) const
{
    if (!mHightFreqVocabulary)
    {
        return false;
    }

    if (mHighFreqencyTermPostingType != hp_bitmap)
    {
        return false;
    }

    return mHightFreqVocabulary->Lookup(key);
}

void IndexConfigImpl::Disable()
{
    mStatus = (mStatus == is_normal) ? is_disable : mStatus;
    if (mShardingType == IndexConfig::IST_NEED_SHARDING)
    {
        for (size_t i = 0; i < mShardingIndexConfigs.size(); ++i)
        {
            mShardingIndexConfigs[i]->Disable();
        }
    }
}

void IndexConfigImpl::Delete()
{
    if (mIndexType == it_primarykey128 ||
        mIndexType == it_primarykey64 ||
        mIndexType == it_trie ||
        mIndexType == it_kv ||
        mIndexType == it_kkv)
    {
        INDEXLIB_FATAL_ERROR(Schema, "delete index [%s] fail, index is pk!",
                             mIndexName.c_str());
    }
    mStatus = is_deleted;     
}

IE_NAMESPACE_END(config);
