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
#ifndef __INDEXLIB_ATTRIBUTE_MERGER_FACTORY_H
#define __INDEXLIB_ATTRIBUTE_MERGER_FACTORY_H

#include <map>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/index_base/index_meta/parallel_merge_item.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Singleton.h"

DECLARE_REFERENCE_CLASS(index, PackAttributeMerger);
DECLARE_REFERENCE_CLASS(index, AttributeMerger);
DECLARE_REFERENCE_CLASS(index, AttributeMergerCreator);

namespace indexlib { namespace index {

class AttributeMergerFactory : public util::Singleton<AttributeMergerFactory>
{
public:
    typedef std::map<FieldType, AttributeMergerCreatorPtr> CreatorMap;

protected:
    AttributeMergerFactory();
    friend class util::LazyInstantiation;

public:
    ~AttributeMergerFactory();

public:
    AttributeMerger* CreateAttributeMerger(
        const config::AttributeConfigPtr& attrConfig, bool needMergePatch,
        const index_base::MergeItemHint& hint = index_base::MergeItemHint(),
        const index_base::MergeTaskResourceVector& taskResources = index_base::MergeTaskResourceVector());

    AttributeMerger* CreatePackAttributeMerger(
        const config::PackAttributeConfigPtr& packAttrConfig, bool needMergePatch, size_t patchBufferSize,
        const index_base::MergeItemHint& hint = index_base::MergeItemHint(),
        const index_base::MergeTaskResourceVector& taskResources = index_base::MergeTaskResourceVector());

    void RegisterCreator(const AttributeMergerCreatorPtr& creator);
    void RegisterMultiValueCreator(const AttributeMergerCreatorPtr& creator);

protected:
#ifdef ATTRIBUTE_MERGER_FACTORY_UNITTEST
    friend class AttributeMergerFactoryTest;
    CreatorMap& GetMap() { return mMergerCreators; }
#endif

private:
    void Init();
    bool IsDocIdJoinAttribute(const config::AttributeConfigPtr& attrConfig);

private:
    autil::RecursiveThreadMutex mLock;
    CreatorMap mMergerCreators;
    CreatorMap mMultiValueMergerCreators;
};

DEFINE_SHARED_PTR(AttributeMergerFactory);
}} // namespace indexlib::index

#endif //__INDEXLIB_ATTRIBUTE_MERGER_FACTORY_H
