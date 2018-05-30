package lk.lab.maxcompute.oss.zip;

import com.aliyun.odps.udf.Extractor;
import com.aliyun.odps.udf.OdpsStorageHandler;
import com.aliyun.odps.udf.Outputer;


public class ZipStorageHandler extends OdpsStorageHandler {

    @Override
    public Class<? extends Extractor> getExtractorClass() {
        return ZipExtractor.class;
    }

    @Override
    public Class<? extends Outputer> getOutputerClass() {
        return ZipOutputer.class;
    }
}
