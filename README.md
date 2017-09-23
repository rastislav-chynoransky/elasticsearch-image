## DEPRECATED

### Please note, this project is deprecated and no longer being maintained. Because of my limited time for make it work on upward version, and that is no very detail official plug-in guideline in E.S, not to mentioned E.S code is change so fast that usually the code is not backward compatible. 

However, instead of implements it as plug-in, that is another way to do so [link](https://www.linkedin.com/pulse/hacking-elasticsearch-image-retrieval-ashwin-saval), the only down side is you need to write more low level code instead.

----------


Image Plugin for Elasticsearch
==============================

The Image Plugin is an Content Based Image Retrieval Plugin for Elasticsearch using [LIRE (Lucene Image Retrieval)](https://github.com/dermotte/lire). It allows users to index images and search for similar images.

It adds an `image` field type and an `image` query.

To create the plugin, open cmd or shell at this directory, enter `gradlew plugin`.

Once the build done, you can find the distribution in build/distribution directory, unzip this file to `%elasticsearch%/plugins/elasticsearch-image-x.x.x` where the x is the version number.



|     Image Plugin          |  elasticsearch    | Release date |
|---------------------------|-------------------|:------------:|
| 2.4.1                     | 2.4.1             | 2016-10-28   |
| 2.4.0                     | 2.4.0             | 2016-09-28   |
| 2.3.4                     | 2.3.4             | 2016-07-18   |
| 2.3.3                     | 2.3.3             | 2016-06-13   |
| 2.3.2                     | 2.3.2             | 2016-05-16   |
| 2.2.0                     | 2.2.0             | 2016-03-01   |
| 2.1.1                     | 2.1.1             | 2016-01-09   |
| 1.3.0-SNAPSHOT (master)   | 1.1.0             |              |
| 1.2.0                     | 1.0.1             | 2014-03-20   |
| 1.1.0                     | 1.0.1             | 2014-03-13   |
| 1.0.0                     | 1.0.1             | 2014-03-05   |


## Developers:
Kevin Wang <kzwang>

Angelo Leto <angleto>

zengde <zengde>

kiwionly <kiwionly>

youqian <youqian>


## Example
#### Create Settings

```sh
curl -XPUT 'localhost:9200/my_index' -d '{
  "settings": {
    "number_of_shards": 5,
    "number_of_replicas": 1,
    "index.version.created": 1070499
  }
}'
```

Since elasticsearch 2.2, that is a version checked, index version must set before version 2.0.0 beta 1.

#### Create Mapping

```sh
curl -XPUT 'localhost:9200/my_index/my_image_item/_mapping' -d '{
    "my_image_item": {
        "properties": {
            "my_img": {
                "type": "image",
                "feature": {
                    "CEDD": {
                        "hash": ["BIT_SAMPLING"]
                    },
                    "JCD": {
                        "hash": ["BIT_SAMPLING", "LSH"]
                    }
                },
                "metadata": {
                    "jpeg.image_width": {
                        "type": "string",
                        "store": "yes"
                    },
                    "jpeg.image_height": {
                        "type": "string",
                        "store": "yes"
                    }
                }
            }
        }
    }
}'
```
`type` should be `image`. This is the type register by this plugin. **Mandatory**

`feature` is a map of features for index. You can only search what you specific, e.g. base on example above, specific `JCD` with `LSH` in mapping allow search for it, but you cannot search `CEDD` with `LSH` 
because the index mapping for `LSH` is not specific and created. If you not specific hash for a `feature`, it won't work. **Mandatory, at least one is required** 

`hash` can be set if you want to search on hash. **Mandatory**

`metadata` is a map of metadata for index, only those metadata will be indexed. See [Metadata](#metadata). **Optional**


#### Index Image
```sh
curl -XPOST 'localhost:9200/test/test' -d '{
    "my_img": "... base64 encoded image ..."
}'
```

#### Search Image
```sh
curl -XPOST 'localhost:9200/test/test/_search' -d '{
	"from": 0,
    "size": 3,
    "query": {
        "image": {
            "my_img": {
                "feature": "CEDD",
                "image": "... base64 encoded image to search ...",
                "hash": "BIT_SAMPLING",
                "boost": 2.1,
                "limit": 100
            }
        }
    }
}'
```
`feature` should be one of the features in the mapping. See above.  **Mandatory**

`image` base64 of image to search.  **Optional if search using existing image**

`hash` should be same to the hash set in mapping. See Above.  **Optional**

`boost` score boost  **Optional**


#### Search Image using existing image in index
```sh
curl -XPOST 'localhost:9200/test/test/_search' -d '{ 	
    "query": {
        "image": {
            "my_img": {
                "feature": "CEDD",
                "index": "test",
                "type": "test",
                "id": "image1",
                "hash": "BIT_SAMPLING"
            }
        }
    }
}'
```
`index` the index to fetch image from. Default to current index.  **Optional**

`type` the type to fetch image from.  **Mandatory**

`id` the id of the document to fetch image from.  **Mandatory**

`field` the field specified as path to fetch image from. Example above is "my_img **Optional**

`routing` a custom routing value to be used when retrieving the external image doc.  **Optional**

### image query Builder
```sh
SearchRequestBuilder queryBuilder = searchClient.prepareSearch(INDEX)
		.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		.setTypes("Image")
		.setFrom(from)
		.setSize(size);
	
	ImageQueryBuilder query = new ImageQueryBuilder("img");  //image field
	query.feature(feature);
	query.hash(hash);
	query.lookupIndex(INDEX);
	query.lookupType("Image");
	query.lookupId(itemId);	
```


### Metadata
Metadata are extracted using [metadata-extractor](https://code.google.com/p/metadata-extractor/). See [SampleOutput](https://code.google.com/p/metadata-extractor/wiki/SampleOutput) for some examples of metadata.

The field name in index will be `directory.tag_name`, all lower case and space becomes underscore(`_`). e.g. if the *Directory* is `JPEG` and *Tag Name* is `Image Height`, the field name will be `jpeg.image_height`



### Supported Image Formats
Images are processed by Java ImageIO, supported formats can be found [here](http://docs.oracle.com/javase/7/docs/api/javax/imageio/package-summary.html)

Additional formats can be supported by ImageIO plugins, for example [TwelveMonkeys](https://github.com/haraldk/TwelveMonkeys)


### Supported Features
[`AUTO_COLOR_CORRELOGRAM`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/AutoColorCorrelogram.java),  [`BINARY_PATTERNS_PYRAMID`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/BinaryPatternsPyramid.java), [`CEDD`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/CEDD.java), [`SIMPLE_COLOR_HISTOGRAM`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/SimpleColorHistogram.java), [`COLOR_LAYOUT`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/ColorLayout.java), [`EDGE_HISTOGRAM`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/EdgeHistogram.java), [`FCTH`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/FCTH.java), [`GABOR`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/Gabor.java), [`JCD`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/JCD.java), [`JOINT_HISTOGRAM`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/joint/JointHistogram.java), [`JPEG_COEFFICIENT_HISTOGRAM`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/JpegCoefficientHistogram.java), [`LOCAL_BINARY_PATTERNS`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/LocalBinaryPatterns.java), [`LUMINANCE_LAYOUT`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/LuminanceLayout.java), [`OPPONENT_HISTOGRAM`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/OpponentHistogram.java), [`PHOG`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/PHOG.java), [`ROTATION_INVARIANT_LOCAL_BINARY_PATTERNS`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/RotationInvariantLocalBinaryPatterns.java), [`SCALABLE_COLOR`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/ScalableColor.java), [`TAMURA`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/imageanalysis/Tamura.java)


### Supported Hash Mode
[`BIT_SAMPLING`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/indexing/hashing/BitSampling.java), [`LSH`](https://code.google.com/p/lire/source/browse/trunk/src/main/java/net/semanticmetadata/lire/indexing/hashing/LocalitySensitiveHashing.java)

Hash will increase search speed with large data sets

See [Large image data sets with LIRE ?some new numbers](http://www.semanticmetadata.net/2013/03/20/large-image-data-sets-with-lire-some-new-numbers/) 


### Settings
|     Setting          |  Description    | Default |
|----------------------|-----------------|:-------:|
| index.image.use_thread_pool | use multiple thread when multiple features are required | True |
| index.image.ignore_metadata_error| ignore errors happened during extract metadata from image | True |

## ChangeLog

#### 2.4.0 (2016-10-28)
- upgrade to 2.4.1
- remove support from maven, because is hard to remember update the dependency, if you using Maven for eclipse, please install buildship gradle plugin, it should work as expected.


#### 2.4.0 (2016-09-28)
- upgrade to 2.4.0

#### 2.3.4 (2016-07-18)
- upgrade to 2.3.4

#### 2.3.3 (2016-06-13)
- upgrade to 2.3.3


#### 2.3.2 (2016-05-16)
- fix a JCD feature bug, see [here](https://github.com/visuual/elasticsearch-image/commit/be80790ed23253faf677a8f336da6228e8e3fd82)
- thanks youqian for 2.3.2 patch.

#### 2.2.0 (2016-03-01)
- upgrade to lire 1.0b2.
- all LIRE features supported.
- index.image.use_thread_pool is optional.
- index.version.created is mandatory in settings.
- add gradle support. (maven no longer use)
- simplify index and search by remove some parameters.
- limit no longer use, use pagination `from` and `size` from elastic search instead.
- remove ImageHashLimitQuery and ImageQuery, this 2 classes possible no longer work 
   (I cound not make it work, also that is possibility not valid anymore for new elastic search version).

*reindex is needed if using difference version of LIRE.

#### 2.1.1 (2016-01-06)

#### 1.2.0 (2014-03-20)

- Use multi-thread when multiple features are required to improve index speed
- Allow index metadata
- Allow query by existing image in index

#### 1.1.0 (2014-03-13)

- Added `limit` in `image` query
- Added plugin version in es-plugin.properties

#### 1.0.0 (2014-03-05)

- initial release