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
/*********************************************************************
 * $Author: liang.xial $
 *
 * $LastChangedBy: liang.xial $
 *
 * $Revision: 2577 $
 *
 * $LastChangedDate: 2011-03-09 09:50:55 +0800 (Wed, 09 Mar 2011) $
 *
 * $Id: cmcluster_xml_build.h 2577 2011-03-09 01:50:55Z liang.xial $
 *
 * $Brief: ini file parser $
 ********************************************************************/

#ifndef CONFIG_CMCLUSTER_XML_BUILD_H_
#define CONFIG_CMCLUSTER_XML_BUILD_H_

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_common.pb.h"

namespace cm_basic {

class CMClusterXMLBuild
{
public:
    // free the result manually
    // extra: whether to write extra properties
    static char* buildXmlCMCluster(cm_basic::CMCluster* pCMCluster, bool extra = false);
};

} // namespace cm_basic

#endif // CONFIG_CMCLUSTER_XML_BUILD_H_
