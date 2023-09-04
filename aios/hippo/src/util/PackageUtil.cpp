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
#include "util/PackageUtil.h"
#include "util/PathUtil.h"
#include "fslib/util/FileUtil.h"
#include "autil/StringUtil.h"
#include "autil/ExtendHashFunction.h"

using namespace std;
using namespace autil;

using namespace fslib;
USE_HIPPO_NAMESPACE(util);
BEGIN_HIPPO_NAMESPACE(util);
HIPPO_LOG_SETUP(util, PackageUtil);

PackageUtil::PackageUtil() {
}

PackageUtil::~PackageUtil() {
}

string PackageUtil::getPackageChecksum(
        const vector<proto::PackageInfo> &packageInfos)
{
    set<string> checksums;
    for (size_t i = 0; i < packageInfos.size(); i++) {
        string checksum = PackageUtil::getPackageChecksum(
                packageInfos[i]);
        if (checksum.empty()) {
            return "";
        }
        checksums.insert(checksum);
    }
    string checksum;
    for (set<string>::iterator it = checksums.begin();
         it != checksums.end(); it++)
    {
        checksum += *it;
    }
    return StringUtil::strToHexStr(ExtendHashFunction::hashString(checksum));
}

string PackageUtil::getPackageChecksum(
        const proto::PackageInfo &packageInfo)
{
    string packageUri = packageInfo.packageuri();
    StringUtil::trim(packageUri);
    if (packageUri.empty()) {
        return "";
    }
    if (!packageInfo.has_type()) {
        return "";
    }
    if (packageInfo.type() == proto::PackageInfo::RPM) {
        return packageUri + "|RPM";
    } else if (packageInfo.type() == proto::PackageInfo::ARCHIVE) {
        return packageUri + "|ACV";
    } else if (packageInfo.type() == proto::PackageInfo::IMAGE) {
        return packageUri + "|IMG";
    } else {
        return "";
    }
}

string PackageUtil::getPackageInfoStr(
        const proto::PackageInfo &packageInfo)
{
    string infoStr;
    if (packageInfo.has_packageuri()) {
        infoStr += "packageURI: " + packageInfo.packageuri() + ", ";
    } else {
        infoStr += "packageURI: empty, ";
    }
    if (packageInfo.has_type()) {
        infoStr = infoStr + "packageType: " +
                  proto::PackageInfo::PackageType_Name(packageInfo.type());
    } else {
        infoStr = infoStr + "packageType: empty";
    }
    return infoStr;
}

string PackageUtil::getPackageInfosStr(
        const vector<proto::PackageInfo> &packageInfos)
{
    string infoStr;
    vector<proto::PackageInfo>::const_iterator it = packageInfos.begin();
    for (; it != packageInfos.end(); it++) {
        infoStr += getPackageInfoStr(*it) + "\n";
    }
    return infoStr;
}

string PackageUtil::readLatestLink(const string &cacheBase, const string &installerId) {
    string linksPath = PathUtil::joinPath(cacheBase, PKG_LINKS_DIRNAME);
    string latestLink = PathUtil::joinPath(linksPath, installerId + LATEST_LINK_SYMBOL);
    if (fslib::util::FileUtil::isExist(latestLink) && PathUtil::isLink(latestLink)) {
        return PathUtil::readLink(latestLink);
    }
    return string();
}

END_HIPPO_NAMESPACE(util);

