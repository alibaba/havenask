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
#ifndef __INDEXLIB_JOIN_DOCID_READER_H
#define __INDEXLIB_JOIN_DOCID_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/join_cache/join_docid_reader_base.h"

namespace indexlib { namespace partition {

template <typename JoinAttrType, typename PKIndexType>
class JoinDocidReader : public JoinDocidReaderBase
{
public:
    typedef index::AttributeIteratorTyped<JoinAttrType> JoinAttributeIterator;
    typedef index::AttributeIteratorTyped<docid_t> JoinCacheIterator;
    typedef indexlib::index::LegacyPrimaryKeyReader<PKIndexType> PrimaryKeyIndexReader;
    typedef std::shared_ptr<index::PrimaryKeyIndexReader> PrimaryKeyIndexReaderPtr;

public:
    JoinDocidReader(std::string joinAttrName, JoinCacheIterator* joinCacheIter,
                    JoinAttributeIterator* mainAttributeIter, index::PrimaryKeyIndexReaderPtr auxPkReader,
                    index::DeletionMapReaderPtr auxDelMapReader);
    ~JoinDocidReader();

public:
    docid_t GetAuxDocid(docid_t mainDocid);

private:
    std::string mJoinAttrName;
    JoinCacheIterator* mJoinCacheIter;
    JoinAttributeIterator* mMainAttributeIter;
    index::PrimaryKeyIndexReaderPtr mAuxPkReader;
    index::DeletionMapReaderPtr mAuxDelMapReader;
    IE_LOG_DECLARE();
};

template <typename JoinAttrType, typename PKIndexType>
JoinDocidReader<JoinAttrType, PKIndexType>::JoinDocidReader(std::string joinAttrName, JoinCacheIterator* joinCacheIter,
                                                            JoinAttributeIterator* mainAttributeIter,
                                                            index::PrimaryKeyIndexReaderPtr auxPkReader,
                                                            index::DeletionMapReaderPtr auxDelMapReader)
    : mJoinAttrName(joinAttrName)
    , mJoinCacheIter(joinCacheIter)
    , mMainAttributeIter(mainAttributeIter)
    , mAuxPkReader(auxPkReader)
    , mAuxDelMapReader(auxDelMapReader)
{
}

template <typename JoinAttrType, typename PKIndexType>
JoinDocidReader<JoinAttrType, PKIndexType>::~JoinDocidReader()
{
    if (mJoinCacheIter) {
        POOL_DELETE_CLASS(mJoinCacheIter);
    }
    if (mMainAttributeIter) {
        POOL_DELETE_CLASS(mMainAttributeIter);
    }
}

template <typename JoinAttrType, typename PKIndexType>
inline docid_t JoinDocidReader<JoinAttrType, PKIndexType>::GetAuxDocid(docid_t mainDocid)
{
    docid_t joinDocId = INVALID_DOCID;
    if (mJoinCacheIter) {
        if (mJoinCacheIter->Seek(mainDocid, joinDocId) && joinDocId != INVALID_DOCID &&
            !mAuxDelMapReader->IsDeleted(joinDocId)) {
            return joinDocId;
        }
    }

    JoinAttrType joinAttrValue;
    if (mMainAttributeIter->Seek(mainDocid, joinAttrValue)) {
        const std::string& strValue = autil::StringUtil::toString(joinAttrValue);
        joinDocId = mAuxPkReader->Lookup(strValue);
        if (mJoinCacheIter) {
            mJoinCacheIter->UpdateValue(mainDocid, joinDocId);
        }
        return joinDocId;
    }
    return INVALID_DOCID;
}
}} // namespace indexlib::partition

#endif //__INDEXLIB_JOIN_DOCID_READER_H
