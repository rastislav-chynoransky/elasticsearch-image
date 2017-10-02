package org.elasticsearch.plugin.image;

import org.elasticsearch.index.mapper.descriptor.DescriptorMapper;
import org.elasticsearch.index.mapper.image.ImageMapper;
import org.elasticsearch.index.query.descriptor.DescriptorQueryParser;
import org.elasticsearch.index.query.image.ImageQueryParser;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;


public class ImagePlugin extends Plugin {

    @Override
    public String name() {
        return "image";
    }

    @Override
    public String description() {
        return "Elasticsearch Image Plugin";
    }

    public void onModule(IndicesModule indicesModule) {
        indicesModule.registerMapper(ImageMapper.CONTENT_TYPE, new ImageMapper.TypeParser(new ThreadPool("elasticsearch-image")));
        indicesModule.registerQueryParser(ImageQueryParser.class);

        indicesModule.registerMapper(DescriptorMapper.CONTENT_TYPE, new DescriptorMapper.TypeParser());
        indicesModule.registerQueryParser(DescriptorQueryParser.class);
    }
}
