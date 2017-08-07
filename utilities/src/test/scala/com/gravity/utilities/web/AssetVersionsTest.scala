package com.gravity.utilities.web

import com.gravity.utilities.BaseScalaTest

class AssetVersionsTest extends BaseScalaTest {

  test("AssetVersionsFromOpsManifest") {
    val lines =
      "/foo/bar/baz.css|/foo/bar/baz.01234567deadbeef.css" ::
      "/foo/bar/3/index.js|/foo/bar/3/index.89abcdef0123456.js" ::
      "malformed" ::
      "" ::
      Nil

    val assetVersions = new AssetVersionsFromOpsManifest {
      override def manifestLines: List[String] = lines
    }
    assetVersions.latestAssetIfBundled("/foo/bar/baz.css") should equal (Some("/foo/bar/baz.01234567deadbeef.css"))
    assetVersions.latestAssetIfBundled("/foo/bar/3/index.js") should equal (Some("/foo/bar/3/index.89abcdef0123456.js"))
    assetVersions.latestAssetIfBundled("malformed") should equal (None)
    assetVersions.latestAssetIfBundled("") should equal (None)
  }

}