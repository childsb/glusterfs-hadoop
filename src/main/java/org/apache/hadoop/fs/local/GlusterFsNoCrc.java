package org.apache.hadoop.fs.local;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FilterFs;

public class GlusterFsNoCrc extends FilterFs{

    GlusterFsNoCrc(Configuration conf) throws IOException, URISyntaxException{
        super(new GlusterVol(conf));
    }

    GlusterFsNoCrc(final URI theUri, final Configuration conf) throws IOException, URISyntaxException{
        this(conf);
    }


}
