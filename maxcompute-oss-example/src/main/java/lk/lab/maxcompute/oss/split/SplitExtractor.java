package lk.lab.maxcompute.oss.split;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.InputStreamSet;
import com.aliyun.odps.io.SourceInputStream;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.Extractor;


public class SplitExtractor extends Extractor {
    
    private String                  delimiter       = ",";  // 参数传入每行文本的列分隔符
    
    private InputStreamSet          inputs;
    private DataAttributes          attributes;
    private Reader                  currentReader;
    private boolean                 firstRead       = true;
    private boolean                 complexText     = false;
    private char                    linebreakChar   = '\n';

    @Override
    public void setup(ExecutionContext ctx, InputStreamSet inputs, DataAttributes attributes) {
        this.inputs = inputs;
        this.attributes = attributes;
        initParams();
    }

    @Override
    public Record extract() throws IOException {
        String line = readNextLine();
        if (line == null) {
            return null;// 返回null标志已经读取完成
        }
        while ("".equals(line.trim()) || line.length() == 0 || line.charAt(0) == '\r' // 遇到空行则继续处理
                || line.charAt(0) == '\n') {
            line = readNextLine();
            if (line == null)
                return null;
        }
        return textLineToRecord(line);
    }

    @Override
    public void close() {
       // no-op
    }
    
    private void initParams() {
        String paramDelimiter = this.attributes.getValueByKey("delimiter");
        if (paramDelimiter != null && !"".equals(paramDelimiter)) {
            this.delimiter = paramDelimiter;
        }
    }
    
    private String readNextLine() throws IOException {
        if (firstRead) {
            firstRead = false;
            currentReader = moveToNextStream();
            if (currentReader == null) {
                return null;
            }
        }
        // 读取行级数据
        while (currentReader != null) {
            String line = complexText ? parseLine(currentReader)
                    : ((BufferedReader) currentReader).readLine();
            if (line != null) {
                return line;
            }
            currentReader = moveToNextStream();
        }
        return null;
    }
    
    private String parseLine(Reader reader) throws IOException {
        int ch = reader.read();
        boolean emptyLine = true;
        while (ch == '\r') {
            reader.read();
        }
        if (ch < 0) {
            return null;
        }
        StringBuffer line = new StringBuffer();
        while (ch >= 0) {
            if (ch == this.linebreakChar) {
                if (emptyLine) {
                    return new String();
                } else {
                    break;
                }
            } else if (ch == '\r') {
                // do nothing, only to note the case of \r
            } else {
                line.append((char) ch);
                emptyLine = false;
            }
            ch = reader.read();
        }
        return line.toString();
    }
    
    private Reader moveToNextStream() throws IOException {
        SourceInputStream stream = inputs.next();
        if (stream == null) {
            return null;
        }
        
        System.out.println("========inputs.next():" + stream.getFileName() + "========");
        return createReaderBySplitCondition(stream);
    }
    
    private Reader createReaderBySplitCondition(SourceInputStream stream) throws IOException {
        long splitSize = stream.getSplitSize();
        long fileSize = stream.getFileSize();
        System.out.println("-- fileSize:" + fileSize + ";splitSize:" + splitSize);
        if (fileSize != splitSize) {
            this.complexText = true;
            long currentPos = stream.getCurrentPos();
            long splitStart = stream.getSplitStart();
            System.out.println("-- currentPos:" + currentPos + ";splitStart:" + splitStart);
            return new SplitReaderWithBoundaryCheck(stream, Charset.forName("GBK"), linebreakChar);
        }
        this.complexText = false;
        // if it does not specify the split then treat it as normal processing.
        return new BufferedReader(new InputStreamReader(stream, "GBK"));
    }
    
    private Record textLineToRecord(String line) {
        Column[] outputColumns = this.attributes.getRecordColumns();
        ArrayRecord record = new ArrayRecord(outputColumns);
        if (this.attributes.getRecordColumns().length == 0) {
            return record;
        }
        
        String[] parts = line.split(delimiter);
        int[] outputIndexes = this.attributes.getNeededIndexes();
        if (outputIndexes == null) {
            throw new IllegalArgumentException("No outputIndexes supplied.");
        }
        if (outputIndexes.length != outputColumns.length) {
            throw new IllegalArgumentException("Mismatched output schema: Expecting "
                    + outputColumns.length + " columns but get " + parts.length);
        }
        int index = 0;
        for (int i = 0; i < parts.length; i++) {
            // only parse data in columns indexed by output indexes
            if (index < outputIndexes.length && i == outputIndexes[index]) {
                switch (outputColumns[index].getType()) {
                case STRING:
                    record.setString(index, parts[i]);
                    break;
                case BIGINT:
                    record.setBigint(index, Long.parseLong(parts[i]));
                    break;
                case BOOLEAN:
                    record.setBoolean(index, Boolean.parseBoolean(parts[i]));
                    break;
                case DOUBLE:
                    record.setDouble(index, Double.parseDouble(parts[i]));
                    break;
                case DATETIME:
                case DECIMAL:
                case ARRAY:
                case MAP:
                default:
                    throw new IllegalArgumentException("Type " + outputColumns[index].getType()
                            + " not supported for now.");
                }
                index++;
            }
        }
        
        return record;
    }
}
