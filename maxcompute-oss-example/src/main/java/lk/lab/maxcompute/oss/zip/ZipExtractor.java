package lk.lab.maxcompute.oss.zip;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Pattern;
import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.InputStreamSet;
import com.aliyun.odps.io.SourceInputStream;
import com.aliyun.odps.udf.DataAttributes;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.Extractor;


public class ZipExtractor extends Extractor {
    
    private String                  delimiter       = ",";  // 参数传入每行文本的列分隔符
    private Pattern                 patternModel;  // 参数传入需要解析的文件名的正则表达式
    private int                     dataLineStart   = 1;  // 参数传入文本文件中数据行的起始行数
    
    private InputStreamSet          inputs;
    private DataAttributes          attributes;
    private BufferedReader          currentReader;
    private boolean                 firstRead       = true;
    private int                     currentLine     = 1;

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
        
        String paramPattern = this.attributes.getValueByKey("pattern");
        if (paramPattern !=null && !"".equals(paramPattern)) {
            this.patternModel = Pattern.compile(paramPattern);
        } else {
            this.patternModel = Pattern.compile("*");
        }
        
        String paramDataLineStart = this.attributes.getValueByKey("datalinestart");
        if (paramDataLineStart !=null && !"".equals(paramDataLineStart)) {
            this.dataLineStart = Integer.valueOf(paramDataLineStart);
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
            String line = currentReader.readLine();
            if (line != null) {
                if (currentLine < dataLineStart) { // 若当前行小于数据起始行，则继续读取下一条记录
                    currentLine++;
                    continue;
                }
                if (!"EOF".equals(line)) { // 若未到达文件尾则将该行内容返回，若到达文件尾则直接跳到下个文件
                    return line;
                }
            }
            currentReader = moveToNextStream();
            currentLine = 1;
        }
        return null;
    }
    
    private BufferedReader moveToNextStream() throws IOException {
        SourceInputStream stream = null;
        while ((stream = inputs.next()) != null) {
            String fileName = stream.getFileName();
            System.out.println("========inputs.next():" + fileName + "========");
            if (patternModel.matcher(fileName).matches()) {
                System.out.println(String
                        .format("- match fileName:[%s], pattern:[%s]", fileName, patternModel
                                .pattern()));
                ZipCycleInputStream zipCycleInputStream = new ZipCycleInputStream(stream);
                return new BufferedReader(new InputStreamReader(zipCycleInputStream, "UTF-8"), 8192);
            } else {
                 System.out.println(String.format(
                         "-- discard fileName:[%s], pattern:[%s]", fileName, patternModel.pattern()));
                continue;
            }
        }
        return null;
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
