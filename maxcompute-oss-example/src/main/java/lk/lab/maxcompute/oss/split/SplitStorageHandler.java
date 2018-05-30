package lk.lab.maxcompute.oss.split;

import com.aliyun.odps.udf.Extractor;
import com.aliyun.odps.udf.OdpsStorageHandler;
import com.aliyun.odps.udf.Outputer;


public class SplitStorageHandler extends OdpsStorageHandler {

    @Override
    public Class<? extends Extractor> getExtractorClass() {
        return SplitExtractor.class;
    }

    @Override
    public Class<? extends Outputer> getOutputerClass() {
        return SplitOutputer.class;
    }
}
